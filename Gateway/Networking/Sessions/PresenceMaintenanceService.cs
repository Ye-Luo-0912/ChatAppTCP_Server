using ChatApp.TcpGateway.Gateway.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 后台服务：周期性调用 <see cref="IGlobalPresenceStore.RunMaintenanceAsync"/>
/// 清理 Presence ZSET 中的过期成员。
/// <para>
/// 热路径（SetOnline/SetOffline/Refresh/IsOnline/GetOnlineMany）不做 ZREMRANGEBYSCORE，
/// 仅用 ZCOUNT key (now +inf) 统计未过期成员；过期成员的内存回收由本服务完成。
/// 周期由 <see cref="TcpGatewayOptions.PresenceMaintenanceInterval"/> 配置（默认 5 分钟）。
/// </para>
/// </summary>
internal sealed partial class PresenceMaintenanceService(
    IGlobalPresenceStore presenceStore,
    IOptions<TcpGatewayOptions> options,
    ILogger<PresenceMaintenanceService> logger) : BackgroundService
{
    private readonly TimeSpan _interval = options.Value.PresenceMaintenanceInterval;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_interval <= TimeSpan.Zero)
            return;

        try
        {
            using var timer = new PeriodicTimer(_interval);
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await presenceStore.RunMaintenanceAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // 正常关闭。
        }
        catch (Exception ex)
        {
            LogMaintenanceTerminated(logger, ex);
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Error,
        Message = "Presence maintenance background service terminated unexpectedly.")]
    private static partial void LogMaintenanceTerminated(ILogger logger, Exception exception);
}
