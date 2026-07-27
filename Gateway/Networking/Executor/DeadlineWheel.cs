using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Gateway.Networking.Executor;

/// <summary>
/// 全局单调时钟 deadline 时间轮：替代每连接 <see cref="ITimer"/>。
/// <para>
/// 单 <see cref="PeriodicTimer"/> 驱动的分桶时间轮，所有连接共享。
/// 用于认证超时与发送超时：注册一个 deadline，到期回调或被取消。
/// 单调时钟 + generation 防止墙钟回拨死锁与 cancel/re-register ABA。
/// </para>
/// <para>
/// 线程安全：Register/Cancel/RunAsync 共用一个 <see cref="Lock"/>。
/// 注册与取消是低频操作（每连接生命周期内数次），sweep 周期触发，锁竞争极低。
/// </para>
/// </summary>
internal sealed partial class DeadlineWheel : IAsyncDisposable
{
    public static readonly TimeSpan DefaultTickInterval = TimeSpan.FromMilliseconds(100);
    public const int DefaultBucketCount = 1024;

    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _tickInterval;
    private readonly int _bucketCount;
    private readonly ILogger _logger;

    private readonly Lock _gate = new();
    private readonly List<DeadlineEntry>[] _buckets;
    // 已取消但尚未被 sweep 清理的 id。sweep 时遇到匹配条目即移除。
    private readonly HashSet<long> _cancelled = new();
    // 已触发的 id。用于让 Cancel 在已触发注册上幂等忽略，避免重复递减计数。
    // 注意：此集合会随生命周期增长，长期运行需在 FullSweepLocked 中按最低活跃 id 压缩。
    private readonly HashSet<long> _fired = new();
    private long _nextId;
    private long _lastSweptTick;
    private Task? _runTask;
    private CancellationTokenSource? _cts;

    // 可观测计数器：当前活跃 deadline 数（已注册未触发也未取消）。
    // 用于评估 DeadlineWheel 实际负载（认证/空闲/发送超时注册量）。
    private long _activeDeadlines;

    public DeadlineWheel(
        TimeProvider? timeProvider = null,
        TimeSpan? tickInterval = null,
        int? bucketCount = null,
        ILogger? logger = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _tickInterval = tickInterval ?? DefaultTickInterval;
        _bucketCount = bucketCount ?? DefaultBucketCount;
        _logger = logger ?? NullLogger.Instance;

        if (_tickInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(tickInterval));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(_bucketCount, 0);

        _buckets = new List<DeadlineEntry>[_bucketCount];
        for (var i = 0; i < _bucketCount; i++)
            _buckets[i] = new List<DeadlineEntry>();

        // 初始化为当前 tick - 1，使首个完成的 tick 能在下一次 sweep 时被扫描。
        _lastSweptTick = CurrentTick - 1;
    }

    private long CurrentTick => _timeProvider.GetTimestamp() / _tickInterval.Ticks;

    /// <summary>
    /// 启动时间轮驱动循环。重复调用幂等返回已存在的运行 Task。
    /// 调用方应在宿主 stopping 时取消 <paramref name="cancellationToken"/>。
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_runTask is not null)
            return _runTask;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runTask = RunAsync(_cts.Token);
        return _runTask;
    }

    /// <summary>
    /// 注册一个 deadline。到期时在 sweep 线程调用 <paramref name="callback"/>。
    /// <para>
    /// <paramref name="delay"/> 必须为正。delay 超过一圈（bucketCount × tickInterval）时
    /// 仍可正常工作：条目会被反复挂回同一桶直到到期（leftover 路径）。
    /// </para>
    /// <para>
    /// 回调在时间轮 sweep 线程同步执行，必须快速返回且不抛异常。
    /// 回调内不应直接执行重逻辑，应转发到其他执行器或设置标志位。
    /// </para>
    /// </summary>
    public DeadlineRegistration Register(TimeSpan delay, Action callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(delay, TimeSpan.Zero);

        var deadlineTicks = _timeProvider.GetTimestamp() + delay.Ticks;

        lock (_gate)
        {
            var id = ++_nextId;
            var tick = deadlineTicks / _tickInterval.Ticks;
            var bucketIndex = (int)(tick % _bucketCount);
            _buckets[bucketIndex].Add(new DeadlineEntry(id, deadlineTicks, callback));
            Interlocked.Increment(ref _activeDeadlines);
            return new DeadlineRegistration(id);
        }
    }

    /// <summary>
    /// 取消一个未触发的 deadline。已触发或已取消的注册会被忽略。
    /// <para>
    /// 取消仅标记 id 为 cancelled，条目仍留在桶中，sweep 时跳过并清理。
    /// 这避免了从桶 List 中 O(n) 查找删除的开销。
    /// </para>
    /// </summary>
    public void Cancel(DeadlineRegistration registration)
    {
        if (registration.Id == 0)
            return;

        lock (_gate)
        {
            // 已触发：幂等忽略，不重复递减（sweep 已在触发时递减过）。
            if (_fired.Contains(registration.Id))
                return;
            // 已取消：幂等忽略。
            if (!_cancelled.Add(registration.Id))
                return;
            // 首次取消：递减活跃计数。
            Interlocked.Decrement(ref _activeDeadlines);
        }
    }

    /// <summary>当前活跃 deadline 数（已注册未触发也未取消）。</summary>
    public long ActiveDeadlineCount => Interlocked.Read(ref _activeDeadlines);

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_tickInterval, _timeProvider);

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                PumpExpired();
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
    }

    /// <summary>
    /// 推进过期扫描：到期且未取消的 deadline 触发回调。
    /// 生产环境由 <see cref="RunAsync"/> 周期调用；测试可直接调用推进逻辑时钟。
    /// </summary>
    public void PumpExpired()
    {
        var now = _timeProvider.GetTimestamp();
        var currentTick = now / _tickInterval.Ticks;

        List<DeadlineEntry> toFire = new();

        lock (_gate)
        {
            if (currentTick <= _lastSweptTick + 1)
                return;

            var gap = currentTick - _lastSweptTick;
            if (gap >= _bucketCount)
            {
                // 大停顿（超过一圈）：全量扫描所有桶，避免漏过期。
                FullSweepLocked(now, toFire);
            }
            else
            {
                // 扫描 (_lastSweptTick, currentTick) 区间内所有已完成 tick。
                // 不扫描 currentTick 本身：当前 tick 窗口内可能有 deadline 尚未到达。
                for (var tick = _lastSweptTick + 1; tick < currentTick; tick++)
                    SweepBucketLocked(tick, now, toFire);
            }

            _lastSweptTick = currentTick - 1;
        }

        // 回调在锁外触发，避免回调内再次调用 Register/Cancel 造成重入死锁。
        foreach (var entry in toFire)
        {
            try
            {
                entry.Callback();
            }
            catch (Exception ex)
            {
                LogCallbackFailed(_logger, ex);
            }
        }
    }

    private void SweepBucketLocked(long tick, long nowTicks, List<DeadlineEntry> toFire)
    {
        var bucket = _buckets[tick % _bucketCount];
        if (bucket.Count == 0)
            return;

        List<DeadlineEntry>? leftover = null;

        for (var i = 0; i < bucket.Count; i++)
        {
            var entry = bucket[i];

            // 已取消：跳过并从 _cancelled 中移除（id 单调递增不重复，安全清理）。
            if (_cancelled.Remove(entry.Id))
                continue;

            if (entry.DeadlineTicks > nowTicks)
            {
                // 未来到期（仅停顿导致桶被复用时出现）：重新挂到正确桶。
                (leftover ??= new List<DeadlineEntry>()).Add(entry);
                continue;
            }

            toFire.Add(entry);
            // 已触发：标记到 _fired，使后续 Cancel 幂等忽略；
            // 并递减活跃计数。已 Cancel 的条目已在 Cancel() 中递减，不重复。
            _fired.Add(entry.Id);
            Interlocked.Decrement(ref _activeDeadlines);
        }

        bucket.Clear();

        if (leftover is not null)
        {
            foreach (var entry in leftover)
            {
                var idx = (int)(entry.DeadlineTicks / _tickInterval.Ticks % _bucketCount);
                _buckets[idx].Add(entry);
            }
        }
    }

    private void FullSweepLocked(long nowTicks, List<DeadlineEntry> toFire)
    {
        for (var i = 0; i < _bucketCount; i++)
            SweepBucketLocked(_lastSweptTick + 1 + i, nowTicks, toFire);
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        if (_runTask is not null)
        {
            try
            {
                await _runTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        // 清理所有未触发/未取消的 deadline：清空桶与计数器，避免 Dispose 后残留计数。
        // 回调不被触发（已取消的 RunTask 不再 sweep），调用方应在 Dispose 前显式 Cancel
        // 需要副作用的 deadline；此处仅做资源回收。
        lock (_gate)
        {
            for (var i = 0; i < _bucketCount; i++)
                _buckets[i].Clear();
            _cancelled.Clear();
            _fired.Clear();
            Interlocked.Exchange(ref _activeDeadlines, 0);
        }

        _cts?.Dispose();
    }

    private readonly record struct DeadlineEntry(long Id, long DeadlineTicks, Action Callback);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "DeadlineWheel 回调抛出异常")]
    private static partial void LogCallbackFailed(ILogger logger, Exception exception);
}

/// <summary>
/// Register 的不透明句柄。Id=0 表示无效注册（default 值）。
/// </summary>
internal readonly record struct DeadlineRegistration(long Id);
