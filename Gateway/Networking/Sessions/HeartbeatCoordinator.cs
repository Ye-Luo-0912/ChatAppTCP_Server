using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 心跳扫描协调器：周期性执行设备租约 TTL 刷新与 Redis 全局在线状态刷新。
/// <para>
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取以消除 God Service 中散落的后台扫描循环。
/// 单例，由宿主在 ExecuteAsync 中驱动 <see cref="RunAsync"/>，停机时取消 token 退出。
/// </para>
/// <para>
/// V2 重构：认证超时与空闲超时已迁移到全局 <see cref="Executor.DeadlineWheel"/>（per-connection
/// check-on-fire deadline），本协调器不再执行全量超时扫描。仅保留 Redis 分桶刷新：
/// <list type="bullet">
/// <item>tick 间隔 = <see cref="TcpGatewayOptions.HeartbeatScanInterval"/> /
///   <see cref="TcpGatewayOptions.HeartbeatBucketCount"/>（默认 30s/30 = 1s）；</item>
/// <item>每 tick 仅枚举 <see cref="HeartbeatBucketRegistry"/> 的一个连接桶 + 一个用户桶，
///   不再 <c>_sessions.ToArray()</c> 全量复制。10k 连接下每 tick 仅遍历 ~333 连接 + ~333 用户；</item>
/// <item>连接桶按 connectionId 分桶用于设备租约刷新（每连接独立租约）；</item>
/// <item>用户桶按 userId 分桶用于 presence 刷新（同用户多连接只在本 user 桶 tick 内刷新一次，
///   不再因 connectionId 桶不同导致一周期内重复刷新同一用户）；</item>
/// <item>刷新前追加确定性 jitter（tick 间隔 × jitterRatio），避免同桶任务同步触发 Redis；</item>
/// <item>刷新并发上限 = <see cref="TcpGatewayOptions.HeartbeatRefreshConcurrency"/>（取代原硬编码 32）；</item>
/// <item>刷新结果（成功/失败）由 <see cref="SessionLifecycleCoordinator.RefreshLeaseAsync"/> /
///   <see cref="SessionLifecycleCoordinator.RefreshPresenceAsync"/> 显式返回 bool，失败不再被记为成功。</item>
/// </list>
/// 实际 Redis 往返与异常吞噬委托 <see cref="SessionLifecycleCoordinator"/> 完成。
/// </para>
/// <para>
/// 当 <see cref="TcpGatewayOptions.HeartbeatBucketCount"/> = 1 时退化为全量刷新（兼容旧行为）。
/// </para>
/// </para>
/// </summary>
internal sealed class HeartbeatCoordinator
{
    private readonly TcpGatewayOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly TcpListenerHost _listenerHost;
    private readonly HeartbeatBucketRegistry _bucketRegistry;
    private readonly SessionLifecycleCoordinator _lifecycleCoordinator;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;
    private readonly Random _jitterRandom = new();

    public HeartbeatCoordinator(
        TcpGatewayOptions options,
        TimeProvider timeProvider,
        TcpListenerHost listenerHost,
        HeartbeatBucketRegistry bucketRegistry,
        SessionLifecycleCoordinator lifecycleCoordinator,
        GatewayMetrics metrics,
        ILogger logger)
    {
        _options = options;
        _timeProvider = timeProvider;
        _listenerHost = listenerHost;
        _bucketRegistry = bucketRegistry;
        _lifecycleCoordinator = lifecycleCoordinator;
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

        // 复用刷新任务列表，避免每 tick 分配新 List<Task>。
        var refreshTasks = new List<Task>(capacity: 256);
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

                // 仅枚举当前桶，不再 ToArray 全量会话。
                var sessionsInBucket = _bucketRegistry.GetConnectionBucket(currentBucket);
                ICollection<long> usersInBucket = _options.EnableEphemeralPresenceAndTyping
                    ? _bucketRegistry.GetUserBucket(currentBucket)
                    : Array.Empty<long>();

                // 指标：当前 tick 扫描的连接数（仅当前桶，非全局总数）。
                _metrics.HeartbeatSessionsScanned(sessionsInBucket.Count);
                refreshTasks.Clear();

                // 设备租约刷新：每连接独立租约，按 connectionId 桶遍历。
                if (_options.ReplaceSameDeviceSession)
                {
                    foreach (var session in sessionsInBucket)
                    {
                        // 未认证会话不持有租约；DeviceIdHash 缺失也不续期。
                        if (session is not { IsAuthenticated: true, UserId: > 0, DeviceIdHash: { } deviceHash })
                            continue;

                        var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);
                        var leaseId = session.ConnectionLeaseId;
                        refreshTasks.Add(RefreshLeaseWithJitterAsync(
                            () => _lifecycleCoordinator.RefreshLeaseAsync(
                                refreshGate,
                                session.UserId,
                                deviceHash,
                                leaseId,
                                leaseTtl,
                                cancellationToken),
                            jitterWindowMs,
                            cancellationToken));
                    }
                }

                // Presence 刷新：按 userId 桶遍历，同用户多连接只刷新一次（引用计数已在注册表去重）。
                if (_options.EnableEphemeralPresenceAndTyping)
                {
                    foreach (var userId in usersInBucket)
                    {
                        refreshTasks.Add(RefreshPresenceWithJitterAsync(
                            () => _lifecycleCoordinator.RefreshPresenceAsync(
                                refreshGate,
                                userId,
                                cancellationToken),
                            jitterWindowMs,
                            cancellationToken));
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
                        // 单个刷新失败已在 Refresh*WithJitterAsync 内部记录；不中断心跳循环。
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
    /// 在执行设备租约刷新前追加随机 jitter 延迟。
    /// 刷新成功/失败由 <see cref="SessionLifecycleCoordinator.RefreshLeaseAsync"/> 显式返回 bool，
    /// 失败时记录 <see cref="GatewayMetrics.HeartbeatRefreshFailed"/> 不再被误记为成功。
    /// </summary>
    private async Task RefreshLeaseWithJitterAsync(
        Func<Task<bool>> refreshOperation,
        double jitterWindowMs,
        CancellationToken cancellationToken)
    {
        _metrics.HeartbeatRefreshAttempted("lease");

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
            var success = await refreshOperation().ConfigureAwait(false);
            var opDuration = _timeProvider.GetElapsedTime(opStart);
            if (success)
            {
                _metrics.HeartbeatRefreshCompleted(opDuration, "lease");
            }
            else
            {
                _metrics.HeartbeatRefreshFailed("lease");
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _metrics.HeartbeatRefreshFailed("lease");
            // 吞噬异常：Task.WhenAll 不应因单个刷新失败短路。
        }
    }

    /// <summary>
    /// 在执行 presence 刷新前追加随机 jitter 延迟。
    /// 刷新成功/失败由 <see cref="SessionLifecycleCoordinator.RefreshPresenceAsync"/> 显式返回 bool。
    /// </summary>
    private async Task RefreshPresenceWithJitterAsync(
        Func<Task<bool>> refreshOperation,
        double jitterWindowMs,
        CancellationToken cancellationToken)
    {
        _metrics.HeartbeatRefreshAttempted("presence");

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
            var success = await refreshOperation().ConfigureAwait(false);
            var opDuration = _timeProvider.GetElapsedTime(opStart);
            if (success)
            {
                _metrics.HeartbeatRefreshCompleted(opDuration, "presence");
            }
            else
            {
                _metrics.HeartbeatRefreshFailed("presence");
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _metrics.HeartbeatRefreshFailed("presence");
        }
    }
}
