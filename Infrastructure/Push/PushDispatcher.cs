using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Push;
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
/// <item>收集结果：成功计数、可重试失败计数、无效令牌指纹列表。</item>
/// <item>主线一8：对 invalid_token 令牌调用 <see cref="IPushTokenStore.UnregisterByTokenAsync"/>
///   可靠注销（带重试，await 完成不 fire-and-forget）。</item>
/// </list>
/// </para>
/// <para>
/// 主线一7：每 Provider 并发由 <see cref="PushOptions.MaxConcurrentSendsPerProvider"/> 限制，
/// 避免突发流量打爆 Provider API。
/// </para>
/// <para>
/// 不含幂等去重（本轮聚焦投递；去重由调用方或后续 Redis 去重层实现）。
/// 令牌指纹：令牌字符串的 SHA256 前 8 字节 hex（用于日志/指标/无效注销匹配，不泄露完整令牌）。
/// </para>
/// </summary>
internal sealed partial class PushDispatcher : IPushDispatcher
{
    private readonly IPushTokenStore _tokenStore;
    private readonly Dictionary<Core.Messaging.Push.PushPlatform, IPushProvider> _providersByPlatform;
    private readonly PushOptions _options;
    private readonly ILogger<PushDispatcher> _logger;
    // 主线一7：per-Provider 并发限制器。按 Platform 索引，0 表示不限制。
    private readonly Dictionary<Core.Messaging.Push.PushPlatform, SemaphoreSlim> _concurrencyGates;

    public PushDispatcher(
        IPushTokenStore tokenStore,
        IEnumerable<IPushProvider> providers,
        IOptions<PushOptions> options,
        ILogger<PushDispatcher> logger)
    {
        _tokenStore = tokenStore;
        _providersByPlatform = providers.ToDictionary(p => p.Platform);
        _options = options.Value;
        _logger = logger;

        // 主线一7：为每个 Provider 创建并发限制器。
        var maxConcurrent = Math.Max(0, _options.MaxConcurrentSendsPerProvider);
        _concurrencyGates = new Dictionary<Core.Messaging.Push.PushPlatform, SemaphoreSlim>();
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

        var tasks = new Task<PushDeliveryOutcome>[tokens.Count];
        for (var i = 0; i < tokens.Count; i++)
        {
            var record = tokens[i];
            tasks[i] = DispatchWithRetryAsync(command, record, cancellationToken);
        }

        var completed = await Task.WhenAll(tasks).ConfigureAwait(false);
        var outcomes = new List<PushDeliveryOutcome>(completed);
        var result = PushDeliveryResult.FromOutcomes(command.TargetUserId, outcomes);

        // 主线一8：可靠注销无效令牌（await + 重试，非 fire-and-forget）。
        if (result.InvalidTokenFingerprints.Count > 0)
        {
            await UnregisterInvalidTokensReliableAsync(
                command.TargetUserId,
                tokens,
                result.InvalidTokenFingerprints,
                cancellationToken).ConfigureAwait(false);
        }

        if (result.SucceededCount == 0 && result.AttemptedCount > 0)
        {
            LogAllFailed(_logger, command.TargetUserId, result.AttemptedCount, result.RetryableFailureCount, result.InvalidTokenFingerprints.Count);
        }

        return result;
    }

    /// <summary>
    /// 主线一4/6/7：带并发限制和 Token 粒度重试的投递。
    /// </summary>
    private async Task<PushDeliveryOutcome> DispatchWithRetryAsync(
        PushDeliveryCommand command,
        PushTokenRecord record,
        CancellationToken cancellationToken)
    {
        var outcome = await DispatchToOneAsync(command, record, cancellationToken).ConfigureAwait(false);

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
        }

        return outcome;
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
    /// 主线一8：可靠注销无效令牌（await + 重试，非 fire-and-forget）。
    /// <para>
    /// 注销失败不阻塞投递返回，但会在后台重试 <see cref="PushOptions.InvalidTokenUnregisterRetryCount"/> 次，
    /// 避免无效 Token 残留导致后续推送继续尝试已失效的令牌。
    /// </para>
    /// </summary>
    private async Task UnregisterInvalidTokensReliableAsync(
        long userId,
        IReadOnlyList<PushTokenRecord> allTokens,
        IReadOnlyList<string> invalidFingerprints,
        CancellationToken cancellationToken)
    {
        var fingerprintSet = new HashSet<string>(invalidFingerprints, StringComparer.Ordinal);
        var maxRetry = Math.Max(0, _options.InvalidTokenUnregisterRetryCount);

        foreach (var token in allTokens)
        {
            if (!fingerprintSet.Contains(FingerprintToken(token.Token)))
                continue;

            for (var attempt = 0; attempt <= maxRetry; attempt++)
            {
                try
                {
                    await _tokenStore.UnregisterByTokenAsync(userId, token.Token, cancellationToken)
                        .ConfigureAwait(false);
                    break; // 注销成功，跳出重试。
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    LogUnregisterFailed(_logger, ex, userId, attempt, maxRetry);
                    if (attempt >= maxRetry)
                        break;

                    // 指数退避：100ms * 2^attempt。
                    var delay = TimeSpan.FromMilliseconds(100 * (1 << attempt));
                    try
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                }
            }
        }
    }

    /// <summary>
    /// 令牌指纹：SHA256 前 8 字节 hex（16 字符）。用于日志/指标/无效令牌匹配，不泄露完整令牌。
    /// </summary>
    internal static string FingerprintToken(string token)
    {
        var hash = System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(token));
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
        ILogger logger, Exception ex, Core.Messaging.Push.PushPlatform platform, long userId);

    [LoggerMessage(
        LogLevel.Error,
        "Failed to unregister invalid push token: userId={UserId} attempt={Attempt}/{MaxRetry}")]
    static partial void LogUnregisterFailed(
        ILogger logger, Exception ex, long userId, int attempt, int maxRetry);
}
