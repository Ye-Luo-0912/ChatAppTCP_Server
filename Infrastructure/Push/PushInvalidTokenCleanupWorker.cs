using ChatApp.Realtime.Abstractions.Push;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 无效 Token 可靠清理 worker（门禁4）。
/// <para>
/// 消费 <see cref="PushInvalidTokenCleanupQueue"/> 中的清理任务，对每个无效 Token 调用
/// <see cref="IPushTokenStore.UnregisterByTokenAsync"/>，失败时按 <see cref="PushOptions.InvalidTokenUnregisterRetryCount"/>
/// 指数退避重试。清理与请求生命周期解耦，避免 fire-and-forget Task 在投递路径上丢失。
/// </para>
/// </summary>
internal sealed partial class PushInvalidTokenCleanupWorker : BackgroundService
{
    private readonly PushInvalidTokenCleanupQueue _queue;
    private readonly IPushTokenStore _tokenStore;
    private readonly PushOptions _options;
    private readonly ILogger<PushInvalidTokenCleanupWorker> _logger;

    public PushInvalidTokenCleanupWorker(
        PushInvalidTokenCleanupQueue queue,
        IPushTokenStore tokenStore,
        IOptions<PushOptions> options,
        ILogger<PushInvalidTokenCleanupWorker> logger)
    {
        _queue = queue;
        _tokenStore = tokenStore;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var item in _queue.Reader
                               .ReadAllAsync(stoppingToken)
                               .ConfigureAwait(false))
            {
                await UnregisterWithRetryAsync(item, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // 正常停机：停止消费。
        }
    }

    /// <summary>
    /// 门禁4：带重试的可靠注销。失败不阻塞主流程，但会在后台重试
    /// <see cref="PushOptions.InvalidTokenUnregisterRetryCount"/> 次，避免无效 Token 残留。
    /// </summary>
    internal async Task UnregisterWithRetryAsync(
        PushInvalidTokenCleanupItem item,
        CancellationToken ct)
    {
        var maxRetry = Math.Max(0, _options.InvalidTokenUnregisterRetryCount);
        for (var attempt = 0; attempt <= maxRetry; attempt++)
        {
            try
            {
                await _tokenStore.UnregisterByTokenAsync(item.UserId, item.Token, ct)
                    .ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                LogUnregisterFailed(_logger, ex, item.UserId, attempt, maxRetry);
                if (attempt >= maxRetry)
                    return;

                // 指数退避：100ms * 2^attempt。
                var delay = TimeSpan.FromMilliseconds(100 * (1 << attempt));
                try
                {
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
            }
        }
    }

    [LoggerMessage(
        LogLevel.Error,
        "Failed to unregister invalid push token: userId={UserId} attempt={Attempt}/{MaxRetry}")]
    static partial void LogUnregisterFailed(
        ILogger logger, Exception ex, long userId, int attempt, int maxRetry);
}