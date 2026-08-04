using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 门禁3：Push Token 渐进式重加密 worker。
/// <para>
/// 密钥轮换后，旧 Key 加密的历史令牌需逐步用当前 Key 重加密。本 worker 按
/// <see cref="PushOptions.TokenReencryptionInterval"/> 周期扫描 Redis 中的令牌并重加密，
/// 避免一次性迁移（也避免长期依赖旧 Key 解密）。
/// </para>
/// </summary>
internal sealed partial class PushTokenReencryptionWorker : BackgroundService
{
    private readonly RedisPushTokenStore _tokenStore;
    private readonly PushOptions _options;
    private readonly ILogger<PushTokenReencryptionWorker> _logger;

    public PushTokenReencryptionWorker(
        RedisPushTokenStore tokenStore,
        IOptions<PushOptions> options,
        ILogger<PushTokenReencryptionWorker> logger)
    {
        _tokenStore = tokenStore;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // 启动时先跑一次，随后按间隔调度。
            await RunOnceAsync(stoppingToken).ConfigureAwait(false);

            using var timer = new PeriodicTimer(_options.TokenReencryptionInterval);
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await RunOnceAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // 正常停机。
        }
    }

    private async Task RunOnceAsync(CancellationToken ct)
    {
        try
        {
            var count = await _tokenStore.ReencryptOldTokensAsync(ct).ConfigureAwait(false);
            if (count > 0)
                LogReencrypted(_logger, count);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            LogReencryptFailed(_logger, ex);
        }
    }

    [LoggerMessage(
        LogLevel.Information,
        "Push token re-encryption sweep completed: reencrypted={Count}.")]
    static partial void LogReencrypted(ILogger logger, int count);

    [LoggerMessage(
        LogLevel.Error,
        "Push token re-encryption sweep failed.")]
    static partial void LogReencryptFailed(ILogger logger, Exception ex);
}