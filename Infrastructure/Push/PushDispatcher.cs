using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// <see cref="IPushDispatcher"/> 默认实现：Gateway 内嵌推送编排器。
/// <para>
/// 流程：
/// <list type="number">
/// <item>从 <see cref="IPushTokenStore"/> 拉取目标用户全部活跃令牌。</item>
/// <item>用户无令牌时立即返回 <see cref="PushDeliveryResult.NoTokensRegistered"/>（跳过指标）。</item>
/// <item>按平台分组，并行调用对应 <see cref="IPushProvider"/>（每令牌一个 Task）。</item>
/// <item>收集结果：成功计数、可重试失败计数、无效令牌指纹列表。</item>
/// <item>对 invalid_token 令牌调用 <see cref="IPushTokenStore.UnregisterByTokenAsync"/> 异步注销。</item>
/// </list>
/// </para>
/// <para>
/// 不含 Retry/DLQ（本轮聚焦 Contract + 基础编排；Retry/DLQ 由后续独立 Worker 实现）。
/// 不含幂等去重（本轮聚焦投递；去重由调用方或后续 Redis 去重层实现）。
/// </para>
/// <para>
/// 令牌指纹：令牌字符串的 SHA256 前 8 字节 hex（用于日志/指标/无效注销匹配，不泄露完整令牌）。
/// </para>
/// </summary>
internal sealed partial class PushDispatcher : IPushDispatcher
{
    private readonly IPushTokenStore _tokenStore;
    private readonly Dictionary<Core.Messaging.Push.PushPlatform, IPushProvider> _providersByPlatform;
    private readonly ILogger<PushDispatcher> _logger;

    public PushDispatcher(
        IPushTokenStore tokenStore,
        IEnumerable<IPushProvider> providers,
        ILogger<PushDispatcher> logger)
    {
        _tokenStore = tokenStore;
        _providersByPlatform = providers.ToDictionary(p => p.Platform);
        _logger = logger;
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
            tasks[i] = DispatchToOneAsync(command, record, cancellationToken);
        }

        var completed = await Task.WhenAll(tasks).ConfigureAwait(false);
        var outcomes = new List<PushDeliveryOutcome>(completed);
        var result = PushDeliveryResult.FromOutcomes(command.TargetUserId, outcomes);

        // 异步注销无效令牌（不阻塞返回）。
        if (result.InvalidTokenFingerprints.Count > 0)
        {
            _ = UnregisterInvalidTokensAsync(command.TargetUserId, tokens, result.InvalidTokenFingerprints, cancellationToken);
        }

        if (result.SucceededCount == 0 && result.AttemptedCount > 0)
        {
            LogAllFailed(_logger, command.TargetUserId, result.AttemptedCount, result.RetryableFailureCount, result.InvalidTokenFingerprints.Count);
        }

        return result;
    }

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
            providerResult = PushProviderResult.Fail("unknown");
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

    private async Task UnregisterInvalidTokensAsync(
        long userId,
        IReadOnlyList<PushTokenRecord> allTokens,
        IReadOnlyList<string> invalidFingerprints,
        CancellationToken cancellationToken)
    {
        var fingerprintSet = new HashSet<string>(invalidFingerprints, StringComparer.Ordinal);
        foreach (var token in allTokens)
        {
            if (fingerprintSet.Contains(FingerprintToken(token.Token)))
            {
                try
                {
                    await _tokenStore.UnregisterByTokenAsync(userId, token.Token, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogUnregisterFailed(_logger, ex, userId);
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
        "Failed to unregister invalid push token: userId={UserId}")]
    static partial void LogUnregisterFailed(
        ILogger logger, Exception ex, long userId);
}
