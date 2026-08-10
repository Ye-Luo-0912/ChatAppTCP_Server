using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 心跳刷新种类：设备租 lease 或全局 presence。
/// </summary>
internal enum HeartbeatRefreshKind : byte
{
    Lease = 1,
    Presence = 2,
}

/// <summary>
/// 心跳刷新工作项：值类型，避免每刷新分配 lambda/Task/Timer。
/// <para>
/// 由 HeartbeatCoordinator 的 tick 循环产生，写入有界 Channel，
/// 由固定 Worker 池消费执行。负载分散由 Worker 池并发数 + Channel 背压自然实现，
/// 不为每个刷新创建独立 Timer 或编码 jitter DueTimestamp——Worker 池数量本身就是
/// 并发上限与负载分散机制。
/// </para>
/// <para>
/// P1-A2：<see cref="LeaseOwnerToken"/> 携带私有所有权凭证（非公开 TransportId），
/// 用于 Redis RefreshIfOwner CAS。
/// </para>
/// </summary>
internal readonly record struct HeartbeatRefreshWork(
    HeartbeatRefreshKind Kind,
    long UserId,
    ulong DeviceHash,
    string? LeaseOwnerToken,
    TimeSpan LeaseTtl,
    long EnqueuedAtTimestamp);

/// <summary>
/// 心跳扫描协调器：周期性执行设备租约 TTL 刷新与 Redis 全局在线状态刷新。
/// <para>
/// V3 重构：采用<b>固定 Work Queue + 固定 Redis Worker</b>模型，替代每 tick 的
/// per-refresh Lambda/Task/Task.Delay/Task.WhenAll 分配。
/// <para>
/// 架构：
/// <code>
/// HeartbeatBucket (每 tick 枚举一个桶)
///     ↓  产生 HeartbeatRefreshWork 值类型
/// Bounded Channel (有界工作队列)
///     ↓
/// 固定 Redis Workers (N = HeartbeatRefreshConcurrency)
///     ↓  调用 SessionLifecycleCoordinator.Refresh*
/// Redis
/// </code>
/// </para>
/// <para>
/// 消除的每刷新分配：
/// <list type="bullet">
/// <item><b>Lambda 闭包</b>：work 是值类型，无闭包捕获；</item>
/// <item><b>Task 状态机</b>：Worker 在 RunAsync 启动时一次性创建，不随刷新数增长；</item>
/// <item><b>Task.Delay Timer</b>：Worker 池数量即并发上限，不为每个刷新创建独立 Timer；</item>
/// <item><b>Task.WhenAll</b>：Worker 持续消费 Channel，无需每 tick 同步等待。</item>
/// </list>
/// </para>
/// <para>
/// 负载分散：bounded Worker 数量（HeartbeatRefreshConcurrency）天然限制 Redis 并发，
/// Worker 按 Redis 往返速度逐条消费，无需 jitter Timer 即可将 333 项/tick 的刷新
/// 分散到多个 Redis 往返周期中。
/// </para>
/// <para>
/// 背压：Channel 容量 = WorkerCount × 4。队列满时 tick 循环 await WriteAsync 阻塞，
/// 防止 Redis 持续慢速时工作项无限积压。Redis 恢复后自动恢复写入。
/// </para>
/// </para>
/// <para>
/// V2：认证超时与空闲超时已迁移到全局 <see cref="Executor.DeadlineWheel"/>（per-connection
/// check-on-fire deadline），本协调器不再执行全量超时扫描。仅保留 Redis 分桶刷新。
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

    // 队列深度观测：tick 入队自增、Worker 出队自减。简单 volatile 计数，无锁。
    // 用于观测 Redis 慢速时队列积压趋势；精确瞬时值无意义，应关注是否逼近
    // Channel 容量（WorkerCount × 4）。
    private volatile int _currentQueueDepth;

    // 八.4：最老待处理项入队时间戳（GetTimestamp() 单位）。0 = 队列空。
    // 由 tick 循环入队时 CAS 设置（仅当队列从空→非空），Worker 排空时清零。
    // 并发近似值——ObservableGauge 拉取时读取，瞬时不一致可接受。
    private long _oldestEnqueueTimestamp;

    // 八.4：tick 间隔——用于 Worker 判定排队超时阈值。
    private TimeSpan _tickInterval;

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
    /// 驱动心跳扫描循环 + 固定 Redis Worker 池，直到 cancellationToken 取消。
    /// <para>
    /// tick 循环：每 tick 枚举一个桶，产生 HeartbeatRefreshWork 值写入有界 Channel。
    /// Worker 池：N 个固定 Task 并行消费 Channel，执行 Redis 刷新。
    /// </para>
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var bucketCount = Math.Max(1, _options.HeartbeatBucketCount);
        // tick 间隔 = 扫描周期 / 桶数。默认 30s/30 = 1s，每秒扫描一个桶。
        var tickInterval = bucketCount > 1
            ? _options.HeartbeatScanInterval / bucketCount
            : _options.HeartbeatScanInterval;
        _tickInterval = tickInterval;
        var leaseRefreshEveryCycles = GetRefreshEveryCycles(
            _options.DeviceLeaseRefreshInterval,
            _options.HeartbeatScanInterval);
        var presenceRefreshEveryCycles = GetRefreshEveryCycles(
            _options.GlobalPresenceRefreshInterval,
            _options.HeartbeatScanInterval);

        var workerCount = Math.Max(1, _options.HeartbeatRefreshConcurrency);
        // Channel 容量 = Worker × 4：队列满时 tick 循环阻塞提供背压，
        // 防止 Redis 持续慢速时工作项无限积压。
        var channelCapacity = workerCount * 4;
        var channel = Channel.CreateBounded<HeartbeatRefreshWork>(
            new BoundedChannelOptions(channelCapacity)
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        // 启动固定 Worker 池：每个 Worker 持续消费 Channel 执行 Redis 刷新。
        // Worker 数量 = HeartbeatRefreshConcurrency，即 Redis 并发上限。
        // gate=null：并发由 Worker 数量保证，无需 SemaphoreSlim。
        var workers = new Task[workerCount];
        for (var i = 0; i < workerCount; i++)
        {
            workers[i] = WorkerLoopAsync(channel.Reader, cancellationToken);
        }

        using var timer = new PeriodicTimer(tickInterval, _timeProvider);
        var tickCounter = 0;
        // 八.4：schedule_lag 跟踪——记录上次 tick 的预期下次触发时间戳。
        var tickFrequency = _timeProvider.TimestampFrequency;
        var tickIntervalTicks = (long)(tickInterval.TotalSeconds * tickFrequency);
        long? expectedNextTickTs = null;
        // 八.4：full_cycle.duration 跟踪——每 bucketCount 个 tick 记录一次周期耗时。
        var cycleStartTs = _timeProvider.GetTimestamp();

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                // 八.4：schedule_lag——实际触发时间 vs 计划时间。
                var actualTickStart = _timeProvider.GetTimestamp();
                if (expectedNextTickTs is { } expected)
                {
                    var lag = _timeProvider.GetElapsedTime(expected);
                    if (lag > TimeSpan.Zero)
                        _metrics.HeartbeatScheduleLag(lag);
                }
                expectedNextTickTs = actualTickStart + tickIntervalTicks;

                var currentBucket = tickCounter % bucketCount;
                tickCounter++;
                var refreshLease = IsRefreshCycleDue(
                    tickCounter,
                    bucketCount,
                    leaseRefreshEveryCycles);
                var refreshPresence = IsRefreshCycleDue(
                    tickCounter,
                    bucketCount,
                    presenceRefreshEveryCycles);

                _listenerHost.SweepAdmission();

                // 仅枚举当前桶，不再 ToArray 全量会话。
                var sessionsInBucket = _bucketRegistry.GetConnectionBucket(currentBucket);
                // Global presence doubles as the sharded realtime routing directory,
                // so its lease must be refreshed independently of optional ephemeral
                // Presence/Typing notifications.
                ICollection<long> usersInBucket =
                    _bucketRegistry.GetUserBucket(currentBucket);

                // 指标：当前 tick 扫描的连接数（仅当前桶，非全局总数）。
                _metrics.HeartbeatSessionsScanned(sessionsInBucket.Count);

                var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);

                // 设备租约刷新：每连接独立租约，按 connectionId 桶遍历。
                // 产生 HeartbeatRefreshWork 值写入 Channel，无 lambda/Task 分配。
                if (_options.ReplaceSameDeviceSession && refreshLease)
                {
                    foreach (var session in sessionsInBucket)
                    {
                        // 未认证会话不持有租约；DeviceIdHash 缺失也不续期。
                        if (session is not { IsAuthenticated: true, UserId: > 0, DeviceIdHash: { } deviceHash })
                            continue;

                        _metrics.HeartbeatRefreshAttempted("lease");
                        var work = new HeartbeatRefreshWork(
                            HeartbeatRefreshKind.Lease,
                            session.UserId,
                            deviceHash,
                            session.LeaseOwnerToken,
                            leaseTtl,
                            actualTickStart);

                        // 队列满时 await 阻塞提供背压（Redis 慢速时 tick 自然降速）。
                        await channel.Writer.WriteAsync(work, cancellationToken)
                            .ConfigureAwait(false);
                        Interlocked.Increment(ref _currentQueueDepth);
                        // 八.4：队列从空→非空时记录最老项时间戳。
                        if (_currentQueueDepth == 1)
                            Interlocked.CompareExchange(ref _oldestEnqueueTimestamp, actualTickStart, 0);
                    }
                }

                // Presence 刷新：按 userId 桶遍历，同用户多连接只刷新一次。
                if (refreshPresence)
                {
                    foreach (var userId in usersInBucket)
                    {
                        _metrics.HeartbeatRefreshAttempted("presence");
                        var work = new HeartbeatRefreshWork(
                            HeartbeatRefreshKind.Presence,
                            userId,
                            0,
                            null,
                            TimeSpan.Zero,
                            actualTickStart);

                        await channel.Writer.WriteAsync(work, cancellationToken)
                            .ConfigureAwait(false);
                        Interlocked.Increment(ref _currentQueueDepth);
                        Interlocked.CompareExchange(ref _oldestEnqueueTimestamp, actualTickStart, 0);
                    }
                }

                var tickDuration = _timeProvider.GetElapsedTime(actualTickStart);
                _metrics.HeartbeatScanCompleted(tickDuration);

                // 八.4：每 bucketCount 个 tick 记录一次完整扫描周期耗时。
                if (tickCounter % bucketCount == 0)
                {
                    var cycleDuration = _timeProvider.GetElapsedTime(cycleStartTs);
                    _metrics.HeartbeatFullCycleCompleted(cycleDuration);
                    cycleStartTs = _timeProvider.GetTimestamp();
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
        finally
        {
            // 通知 Worker 池停止：完成写入端，Worker 的 ReadAllAsync 自然结束。
            channel.Writer.TryComplete();
            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch
            {
                // Worker 异常已在 WorkerLoopAsync 内吞噬，此处忽略。
            }
        }
    }

    /// <summary>
    /// 固定 Redis Worker 循环：持续消费 Channel 中的刷新工作项并执行。
    /// <para>
    /// gate=null：并发由 Worker 数量保证。单点失败仅记录指标，不中断 Worker。
    /// </para>
    /// </summary>
    private async Task WorkerLoopAsync(
        ChannelReader<HeartbeatRefreshWork> reader,
        CancellationToken cancellationToken)
    {
        const string leaseLabel = "lease";
        const string presenceLabel = "presence";

        try
        {
            await foreach (var work in reader.ReadAllAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                Interlocked.Decrement(ref _currentQueueDepth);
                // 八.4：队列排空时清除最老项时间戳。
                if (_currentQueueDepth == 0)
                    Interlocked.Exchange(ref _oldestEnqueueTimestamp, 0);

                var kindLabel = work.Kind == HeartbeatRefreshKind.Lease
                    ? leaseLabel
                    : presenceLabel;

                // 八.4：测量排队等待时长，超阈值计为 overdue。
                var queueAge = _timeProvider.GetElapsedTime(work.EnqueuedAtTimestamp);
                if (queueAge > _tickInterval)
                    _metrics.HeartbeatRefreshOverdue(kindLabel);

                var opStart = _timeProvider.GetTimestamp();
                try
                {
                    bool success;
                    if (work.Kind == HeartbeatRefreshKind.Lease)
                    {
                        success = await _lifecycleCoordinator.RefreshLeaseAsync(
                            gate: null,
                            work.UserId,
                            work.DeviceHash,
                            work.LeaseOwnerToken!,
                            work.LeaseTtl,
                            cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        success = await _lifecycleCoordinator.RefreshPresenceAsync(
                            gate: null,
                            work.UserId,
                            cancellationToken).ConfigureAwait(false);
                    }

                    var opDuration = _timeProvider.GetElapsedTime(opStart);
                    if (success)
                        _metrics.HeartbeatRefreshCompleted(opDuration, kindLabel);
                    else
                        _metrics.HeartbeatRefreshFailed(kindLabel);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch
                {
                    _metrics.HeartbeatRefreshFailed(kindLabel);
                    // 吞噬异常：Worker 不应因单个刷新失败退出。
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
    }

    /// <summary>
    /// 当前待刷新工作项队列深度的快照（已入队未消费的数量）。
    /// <para>
    /// 简单 volatile 计数：tick 循环 WriteAsync 成功后自增，Worker 取出后自减。
    /// 用于观测 Redis 慢速时队列积压情况；精确瞬时值无意义，应关注趋势与是否逼近
    /// Channel 容量（WorkerCount × 4）。
    /// </para>
    /// </summary>
    internal int CurrentQueueDepth => _currentQueueDepth;

    internal static int GetRefreshEveryCycles(
        TimeSpan refreshInterval,
        TimeSpan scanInterval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(
            refreshInterval,
            TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(
            scanInterval,
            TimeSpan.Zero);

        var cycles = (refreshInterval.Ticks + scanInterval.Ticks - 1)
            / scanInterval.Ticks;
        return (int)Math.Clamp(cycles, 1, int.MaxValue);
    }

    internal static bool IsRefreshCycleDue(
        int tickCounter,
        int bucketCount,
        int refreshEveryCycles)
    {
        if (tickCounter <= 0 || bucketCount <= 0 || refreshEveryCycles <= 0)
        {
            return false;
        }

        var cycleIndex = (tickCounter - 1) / bucketCount;
        return cycleIndex % refreshEveryCycles == 0;
    }

    /// <summary>
    /// 八.4：当前最老待处理项的排队年龄（ms）。队列空时返回 0。
    /// </summary>
    internal double CurrentOldestQueueAgeMs
    {
        get
        {
            var ts = Volatile.Read(ref _oldestEnqueueTimestamp);
            if (ts == 0 || _currentQueueDepth == 0)
                return 0;
            return _timeProvider.GetElapsedTime(ts).TotalMilliseconds;
        }
    }
}
