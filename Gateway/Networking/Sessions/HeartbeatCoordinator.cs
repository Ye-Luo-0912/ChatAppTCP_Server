using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 心跳扫描协调器：周期性扫描连接，执行超时关闭、设备租约 TTL 刷新
/// 与 Redis 全局在线状态刷新。
/// <para>
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取以消除 God Service 中散落的后台扫描循环。
/// 单例，由宿主在 ExecuteAsync 中驱动 <see cref="RunAsync"/>，停机时取消 token 退出。
/// </para>
/// <para>
/// 分桶扫描设计（消除周期性任务风暴）：
/// <list type="bullet">
/// <item>tick 间隔 = <see cref="TcpGatewayOptions.HeartbeatScanInterval"/> /
///   <see cref="TcpGatewayOptions.HeartbeatBucketCount"/>（默认 30s/30 = 1s）；</item>
/// <item>每 tick 扫描全部会话执行超时关闭（廉价，无 I/O）；</item>
/// <item>每 tick 仅对"当前桶"内的会话执行 Redis 刷新（lease + presence），
///   桶索引 = connectionId % bucketCount，将原本每 30s 一次的 10k 任务脉冲
///   打散为每秒约 333 任务的平滑流量；</item>
/// <item>刷新前追加确定性 jitter（tick 间隔 × jitterRatio），避免同桶任务同步触发 Redis；</item>
/// <item>刷新并发上限 = <see cref="TcpGatewayOptions.HeartbeatRefreshConcurrency"/>（取代原硬编码 32）。</item>
/// </list>
/// 实际 Redis 往返与异常吞噬委托 <see cref="SessionLifecycleCoordinator"/> 完成。
/// </para>
/// <para>
/// 当 <see cref="TcpGatewayOptions.HeartbeatBucketCount"/> = 1 时退化为全量扫描（兼容旧行为）。
/// </para>
/// </summary>
internal sealed class HeartbeatCoordinator
{
    private readonly TcpGatewayOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly TcpListenerHost _listenerHost;
    private readonly Func<IEnumerable<TcpClientSession>> _getSessions;
    private readonly SessionLifecycleCoordinator _lifecycleCoordinator;
    private readonly UserSessionRegistry _userSessions;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;
    private readonly Random _jitterRandom = new();

    public HeartbeatCoordinator(
        TcpGatewayOptions options,
        TimeProvider timeProvider,
        TcpListenerHost listenerHost,
        Func<IEnumerable<TcpClientSession>> getSessions,
        SessionLifecycleCoordinator lifecycleCoordinator,
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        ILogger logger)
    {
        _options = options;
        _timeProvider = timeProvider;
        _listenerHost = listenerHost;
        _getSessions = getSessions;
        _lifecycleCoordinator = lifecycleCoordinator;
        _userSessions = userSessions;
        _metrics = metrics;
        _logger = logger;
    }

    /// <summary>
    /// 驱动心跳扫描循环，直到 cancellationToken 取消。
    /// 单次 tick 内并发刷新 Redis（SemaphoreSlim 限 <see cref="TcpGatewayOptions.HeartbeatRefreshConcurrency"/>），
    /// 单点失败不中断循环。
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var bucketCount = Math.Max(1, _options.HeartbeatBucketCount);
        // tick 间隔 = 扫描周期 / 桶数。默认 30s/30 = 1s，每秒扫描一个桶。
        var tickInterval = bucketCount > 1
            ? _options.HeartbeatScanInterval / bucketCount
            : _options.HeartbeatScanInterval;

        // jitter 窗口：tick 间隔 × jitterRatio。每个刷新任务在 [0, jitterWindow) 内随机延迟。
        var jitterWindowMs = tickInterval.TotalMilliseconds * _options.HeartbeatRefreshJitterRatio;

        using var timer = new PeriodicTimer(tickInterval, _timeProvider);
        using var refreshGate = new SemaphoreSlim(
            _options.HeartbeatRefreshConcurrency,
            _options.HeartbeatRefreshConcurrency);

        var tickCounter = 0;

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                var currentBucket = tickCounter % bucketCount;
                tickCounter++;

                var tickStart = _timeProvider.GetTimestamp();
                _listenerHost.SweepAdmission();

                var sessions = _getSessions().ToArray();
                _metrics.HeartbeatSessionsScanned(sessions.Length);

                var presenceRefreshUsers = new HashSet<long>();
                var refreshTasks = new List<Task>();

                foreach (var session in sessions)
                {
                    // 超时关闭：每 tick 检查全部会话（廉价，无 I/O）。
                    // 保留全量扫描以保证认证/空闲超时检测延迟 ≤ tickInterval。
                    if (!session.IsAuthenticated &&
                        session.ConnectionAge > _options.AuthenticationTimeout)
                    {
                        session.Close(SessionCloseReason.AuthenticationTimedOut);
                    }
                    else if (session.LastInboundAge > _options.IdleTimeout)
                    {
                        session.Close(SessionCloseReason.IdleTimedOut);
                    }
                    else if (session is { IsAuthenticated: true, UserId: > 0 })
                    {
                        // 刷新只在当前桶内执行：connectionId % bucketCount == currentBucket。
                        // 桶数=1 时退化为全量刷新（currentBucket 恒为 0）。
                        if (bucketCount > 1 &&
                            session.ConnectionId % (uint)bucketCount != (uint)currentBucket)
                        {
                            continue;
                        }

                        var userId = session.UserId;

                        // 设备租约刷新：独立条件。
                        // 仅当启用同设备替换且会话携带 DeviceIdHash 时才续期租约。
                        // 缺少 DeviceIdHash 的已认证连接不持有设备租约，不应续期。
                        if (_options.ReplaceSameDeviceSession
                            && session.DeviceIdHash is { } deviceHash)
                        {
                            var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);
                            var leaseId = session.ConnectionLeaseId;
                            refreshTasks.Add(RefreshWithJitterAsync(
                                () => _lifecycleCoordinator.RefreshLeaseAsync(
                                    refreshGate,
                                    userId,
                                    deviceHash,
                                    leaseId,
                                    leaseTtl,
                                    cancellationToken),
                                jitterWindowMs,
                                "lease",
                                cancellationToken));
                        }

                        // Presence 刷新：独立条件，按用户去重。
                        // 不应依赖 ReplaceSameDeviceSession 或 DeviceIdHash：
                        // 关闭同设备替换或无 DeviceIdHash 的已认证连接仍需续期 Redis 全局在线状态，
                        // 否则 TTL（5 分钟）过期后用户会被误判离线。
                        // 同一用户多设备会话只刷新一次（presenceRefreshUsers 去重）。
                        if (_options.EnableEphemeralPresenceAndTyping
                            && presenceRefreshUsers.Add(userId)
                            && _userSessions.GetSnapshot(userId).Length > 0)
                        {
                            refreshTasks.Add(RefreshWithJitterAsync(
                                () => _lifecycleCoordinator.RefreshPresenceAsync(
                                    refreshGate,
                                    userId,
                                    cancellationToken),
                                jitterWindowMs,
                                "presence",
                                cancellationToken));
                        }
                    }
                }

                if (refreshTasks.Count > 0)
                {
                    try
                    {
                        await Task.WhenAll(refreshTasks).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch
                    {
                        // 单个刷新失败已在 RefreshWithJitterAsync 内部记录；不中断心跳循环。
                    }
                }

                var tickDuration = _timeProvider.GetElapsedTime(tickStart);
                _metrics.HeartbeatScanCompleted(tickDuration);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    /// <summary>
    /// 在执行刷新操作前追加随机 jitter 延迟，避免同桶任务同步触发 Redis 造成抖动。
    /// 刷新成功/失败分别记录 metric；异常吞噬以避免 Task.WhenAll 短路。
    /// </summary>
    private async Task RefreshWithJitterAsync(
        Func<Task> refreshOperation,
        double jitterWindowMs,
        string kind,
        CancellationToken cancellationToken)
    {
        _metrics.HeartbeatRefreshAttempted(kind);

        if (jitterWindowMs > 0)
        {
            var jitterMs = _jitterRandom.NextDouble() * jitterWindowMs;
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(jitterMs), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
        }

        var opStart = _timeProvider.GetTimestamp();
        try
        {
            await refreshOperation().ConfigureAwait(false);
            var opDuration = _timeProvider.GetElapsedTime(opStart);
            _metrics.HeartbeatRefreshCompleted(opDuration, kind);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _metrics.HeartbeatRefreshFailed(kind);
            // 吞噬异常：Task.WhenAll 不应因单个刷新失败短路。
        }
    }
}
