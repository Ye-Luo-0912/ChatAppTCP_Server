using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Integration.Push;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// <see cref="IPushDispatcher"/> 默认实现：Gateway 内嵌推送编排器。
/// <para>
/// 流程：
/// <list type="number">
/// <item>从 <see cref="IPushTokenStore"/> 拉取目标用户全部活跃令牌。</item>
/// <item>用户无令牌时立即返回 <see cref="PushDeliveryResult.NoTokensRegistered"/>（跳过指标）。</item>
/// <item>按平台分组，并行调用对应 <see cref="IPushProvider"/>（每令牌一个 Task）。</item>
/// <item>主线一4/6：对 rate_limited / provider_unavailable 的 token 按指数退避内部重试。</item>
/// <item>门禁1：Token 级幂等——投递前检查 <see cref="IPushIdempotencyStore.IsSentAsync"/>，
///   已成功投递的 token 跳过，避免 JetStream NAK 重投整条命令时重复推送。</item>
/// <item>门禁5：永久失败（invalid_token / payload_too_large）记录到 <see cref="IPushDlqStore"/>。</item>
/// <item>门禁4：对 invalid_token 令牌入队 <see cref="PushInvalidTokenCleanupQueue"/>，
///   由后台 worker 可靠注销（不阻塞请求生命周期，非 fire-and-forget）。</item>
/// </list>
/// </para>
/// <para>
/// 主线一7：每 Provider 并发由 <see cref="PushOptions.MaxConcurrentSendsPerProvider"/> 限制，
/// 避免突发流量打爆 Provider API。
/// </para>
/// </summary>
internal sealed partial class PushDispatcher : IPushDispatcher
{
    private readonly IPushTokenStore _tokenStore;
    private readonly Dictionary<PushPlatform, IPushProvider> _providersByPlatform;
    private readonly PushOptions _options;
    private readonly IPushIdempotencyStore _idempotencyStore;
    private readonly IPushDlqStore _dlqStore;
    private readonly PushInvalidTokenCleanupQueue _cleanupQueue;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<PushDispatcher> _logger;
    // 主线一7：per-Provider 并发限制器。按 Platform 索引，0 表示不限制。
    private readonly Dictionary<PushPlatform, SemaphoreSlim> _concurrencyGates;

    public PushDispatcher(
        IPushTokenStore tokenStore,
        IEnumerable<IPushProvider> providers,
        IOptions<PushOptions> options,
        IPushIdempotencyStore idempotencyStore,
        IPushDlqStore dlqStore,
        PushInvalidTokenCleanupQueue cleanupQueue,
        TimeProvider timeProvider,
        ILogger<PushDispatcher> logger)
    {
        _tokenStore = tokenStore;
        _providersByPlatform = providers.ToDictionary(p => p.Platform);
        _options = options.Value;
        _idempotencyStore = idempotencyStore;
        _dlqStore = dlqStore;
        _cleanupQueue = cleanupQueue;
        _timeProvider = timeProvider;
        _logger = logger;

        // 主线一7：为每个 Provider 创建并发限制器。
        var maxConcurrent = Math.Max(0, _options.MaxConcurrentSendsPerProvider);
        _concurrencyGates = new Dictionary<PushPlatform, SemaphoreSlim>();
        foreach (var platform in _providersByPlatform.Keys)
        {
            _concurrencyGates[platform] = maxConcurrent > 0
                ? new SemaphoreSlim(maxConcurrent, maxConcurrent)
                : new SemaphoreSlim(int.MaxValue, int.MaxValue);
        }
    }

    public async Task<PushDeliveryResult> DispatchAsync(
        PushDeliveryCommand command,
        CancellationToken cancellationToken = default)
    {
        var tokens = await _tokenStore.ListAsync(command.TargetUserId, cancellationToken).ConfigureAwait(false);
        if (tokens.Count == 0)
        {
            return PushDeliveryResult.Skipped(command.TargetUserId);
        }

        // 门禁1：稳定 deliveryId（JetStream 重投整条命令时保持相同，用于幂等去重）。
        var deliveryId = BuildDeliveryId(command);

        var tasks = new Task<PushDeliveryOutcome>[tokens.Count];
        for (var i = 0; i < tokens.Count; i++)
        {
            var record = tokens[i];
            tasks[i] = DispatchWithRetryAsync(command, record, deliveryId, cancellationToken);
        }

        var completed = await Task.WhenAll(tasks).ConfigureAwait(false);
        var outcomes = new List<PushDeliveryOutcome>(completed);
        var result = PushDeliveryResult.FromOutcomes(command.TargetUserId, outcomes);

        // 门禁4：无效令牌入队可靠清理（后台 worker 消费注销，非请求生命周期 fire-and-forget）。
        if (result.InvalidTokenFingerprints.Count > 0)
        {
            EnqueueInvalidTokens(command.TargetUserId, tokens, result.InvalidTokenFingerprints);
        }

        // 门禁5：永久失败（invalid_token / payload_too_large）记录 DLQ，供人工排查与重放。
        await RecordDlqAsync(command, deliveryId, outcomes, cancellationToken).ConfigureAwait(false);

        if (result.SucceededCount == 0 && result.AttemptedCount > 0)
        {
            LogAllFailed(_logger, command.TargetUserId, result.AttemptedCount, result.RetryableFailureCount, result.InvalidTokenFingerprints.Count);
        }

        return result;
    }

    /// <summary>
    /// 主线一4/6/7 + 门禁1：带并发限制、Token 粒度重试与幂等去重的投递。
    /// </summary>
    private async Task<PushDeliveryOutcome> DispatchWithRetryAsync(
        PushDeliveryCommand command,
        PushTokenRecord record,
        string deliveryId,
        CancellationToken cancellationToken)
    {
        var fingerprint = FingerprintToken(record.Token);

        // 门禁1：已成功投递的 token 直接跳过（幂等重放），避免重复推送。
        if (await IsAlreadySentAsync(deliveryId, fingerprint, cancellationToken).ConfigureAwait(false))
        {
            return new PushDeliveryOutcome
            {
                TokenFingerprint = fingerprint,
                Platform = (byte)record.Platform,
                Succeeded = true
            };
        }

        var outcome = await DispatchToOneAsync(command, record, cancellationToken).ConfigureAwait(false);
        if (outcome.Succeeded)
            await MarkSentAsync(deliveryId, fingerprint, cancellationToken).ConfigureAwait(false);

        // 主线一4/6：对可重试失败按指数退避重试。
        var retryCount = Math.Max(0, _options.TokenRetryCount);
        for (var attempt = 0; attempt < retryCount; attempt++)
        {
            if (outcome.Succeeded || !IsRetryable(outcome.ErrorCode))
                break;

            // 指数退避：max(RetryAfter, base * 2^attempt)。
            var delay = _options.TokenRetryBaseDelay * (1 << attempt);
            if (outcome.RetryAfter is { } retryAfter && retryAfter > delay)
                delay = retryAfter;

            try
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }

            outcome = await DispatchToOneAsync(command, record, cancellationToken).ConfigureAwait(false);
            if (outcome.Succeeded)
                await MarkSentAsync(deliveryId, fingerprint, cancellationToken).ConfigureAwait(false);
        }

        return outcome;
    }

    /// <summary>
    /// 门禁1：幂等检查（fail-open）。若存储异常则允许发送（重复推送比丢失安全）。
    /// </summary>
    private async ValueTask<bool> IsAlreadySentAsync(
        string deliveryId, string fingerprint, CancellationToken ct)
    {
        try
        {
            return await _idempotencyStore.IsSentAsync(deliveryId, fingerprint, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogIdempotencyCheckFailed(_logger, ex, deliveryId);
            return false;
        }
    }

    /// <summary>
    /// 门禁1：记录成功投递（best-effort，失败不阻断返回）。
    /// </summary>
    private async ValueTask MarkSentAsync(
        string deliveryId, string fingerprint, CancellationToken ct)
    {
        try
        {
            await _idempotencyStore.MarkSentAsync(deliveryId, fingerprint, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogIdempotencyMarkFailed(_logger, ex, deliveryId);
        }
    }

    /// <summary>
    /// 主线一4：判断错误码是否可重试。
    /// </summary>
    private static bool IsRetryable(string? errorCode) =>
        errorCode is "rate_limited" or "provider_unavailable" or "unknown";

    private async Task<PushDeliveryOutcome> DispatchToOneAsync(
        PushDeliveryCommand command,
        PushTokenRecord record,
        CancellationToken cancellationToken)
    {
        if (!_providersByPlatform.TryGetValue(record.Platform, out var provider))
        {
            return new PushDeliveryOutcome
            {
                TokenFingerprint = FingerprintToken(record.Token),
                Platform = (byte)record.Platform,
                Succeeded = false,
                ErrorCode = "provider_unavailable"
            };
        }

        // 主线一7：并发限制（SemaphoreSlim 限制每 Provider 并发投递数）。
        var gate = _concurrencyGates[record.Platform];
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            PushProviderResult providerResult;
            try
            {
                providerResult = await provider.SendAsync(
                    record.Token,
                    command.Title,
                    command.Body,
                    command.ConversationId,
                    command.CustomData,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                LogProviderException(_logger, ex, record.Platform, command.TargetUserId);
                // P0-4：未分类异常（timeout/DNS/HTTP client exception）默认归为 provider_unavailable（可重试），
                // 而非 unknown（被归为永久失败 → ACK → 消息丢失）。
                providerResult = PushProviderResult.Fail("provider_unavailable");
            }

            return new PushDeliveryOutcome
            {
                TokenFingerprint = FingerprintToken(record.Token),
                Platform = (byte)record.Platform,
                Succeeded = providerResult.Succeeded,
                ErrorCode = providerResult.ErrorCode,
                RetryAfter = providerResult.RetryAfter
            };
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// 门禁4：将无效令牌入队，交给后台 <see cref="PushInvalidTokenCleanupWorker"/> 可靠注销。
    /// </summary>
    private void EnqueueInvalidTokens(
        long userId,
        IReadOnlyList<PushTokenRecord> allTokens,
        IReadOnlyList<string> invalidFingerprints)
    {
        var fingerprintSet = new HashSet<string>(invalidFingerprints, StringComparer.Ordinal);
        foreach (var token in allTokens)
        {
            if (fingerprintSet.Contains(FingerprintToken(token.Token)))
                _cleanupQueue.Enqueue(userId, token.Token);
        }
    }

    /// <summary>
    /// 门禁5：将永久失败（invalid_token / payload_too_large）的 token 记录到 DLQ。
    /// <para>
    /// 携带 DeliveryId、失败 Token 子集（Provider/ErrorCode/RetryAfter）、原始 Trace Context 与记录时间。
    /// </para>
    /// </summary>
    private async Task RecordDlqAsync(
        PushDeliveryCommand command,
        string deliveryId,
        IReadOnlyList<PushDeliveryOutcome> outcomes,
        CancellationToken cancellationToken)
    {
        var permanentFailures = new List<PushDlqFailedToken>();
        foreach (var o in outcomes)
        {
            if (!o.Succeeded && o.ErrorCode is "invalid_token" or "payload_too_large")
            {
                permanentFailures.Add(new PushDlqFailedToken
                {
                    TokenFingerprint = o.TokenFingerprint,
                    Platform = o.Platform,
                    ErrorCode = o.ErrorCode!,
                    RetryAfter = o.RetryAfter
                });
            }
        }

        if (permanentFailures.Count == 0)
            return;

        var activity = Activity.Current;
        var entry = new PushDlqEntry
        {
            DeliveryId = deliveryId,
            TargetUserId = command.TargetUserId,
            FailedTokens = permanentFailures,
            TraceParent = activity is not null ? BuildTraceParent(activity) : null,
            TraceState = activity?.TraceStateString,
            RecordedAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            await _dlqStore.RecordAsync(entry, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogDlqRecordFailed(_logger, ex, deliveryId, permanentFailures.Count);
        }
    }

    /// <summary>
    /// 门禁1：稳定 deliveryId。优先使用 MessageId；缺失时退化为命令内容哈希，
    /// 保证 JetStream 重投同一条命令时 deliveryId 稳定。
    /// </summary>
    private static string BuildDeliveryId(PushDeliveryCommand command)
    {
        if (!string.IsNullOrWhiteSpace(command.MessageId))
            return $"{command.TargetUserId}:{command.MessageId}";

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(
            $"{command.TargetUserId}|{command.Title}|{command.Body}|{command.ConversationId}|{command.OccurredAtMs}"));
        return $"{command.TargetUserId}:{Convert.ToHexString(hash, 0, 8).ToLowerInvariant()}";
    }

    private static string BuildTraceParent(Activity activity) =>
        $"00-{activity.TraceId}-{activity.SpanId}-01";

    /// <summary>
    /// 令牌指纹：SHA256 前 8 字节 hex（16 字符）。用于日志/指标/无效令牌匹配，不泄露完整令牌。
    /// </summary>
    internal static string FingerprintToken(string token)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(token));
        return Convert.ToHexString(hash, 0, 8).ToLowerInvariant();
    }

    [LoggerMessage(
        LogLevel.Warning,
        "Push delivery failed for all tokens: userId={UserId} attempted={Attempted} retryable={Retryable} invalid={Invalid}")]
    static partial void LogAllFailed(
        ILogger logger, long userId, int attempted, int retryable, int invalid);

    [LoggerMessage(
        LogLevel.Error,
        "Push provider {Platform} threw exception: userId={UserId}")]
    static partial void LogProviderException(
        ILogger logger, Exception ex, PushPlatform platform, long userId);

    [LoggerMessage(
        LogLevel.Warning,
        "Push idempotency check failed for deliveryId={DeliveryId}; proceeding to send (fail-open).")]
    static partial void LogIdempotencyCheckFailed(ILogger logger, Exception ex, string deliveryId);

    [LoggerMessage(
        LogLevel.Warning,
        "Push idempotency mark failed for deliveryId={DeliveryId} (best-effort).")]
    static partial void LogIdempotencyMarkFailed(ILogger logger, Exception ex, string deliveryId);

    [LoggerMessage(
        LogLevel.Error,
        "Failed to record push DLQ entry for deliveryId={DeliveryId} failedTokens={FailedTokens}.")]
    static partial void LogDlqRecordFailed(ILogger logger, Exception ex, string deliveryId, int failedTokens);
}