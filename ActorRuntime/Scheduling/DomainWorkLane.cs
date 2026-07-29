using System.Runtime.CompilerServices;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Primitives;

namespace ChatApp.ActorRuntime.Scheduling;

/// <summary>
/// 领域强类型有界 Work Lane：替代通用 <see cref="AsyncOperationExecutor"/> 处理特定 Actor Domain 的 I/O。
/// <para>
/// 与 <see cref="AsyncOperationExecutor"/> 相比：
/// <list type="bullet">
/// <item><b>不装箱</b>：<typeparamref name="TWork"/> 是 struct，直接存储在 <see cref="BoundedMpmcRing{T}"/> 数组中，
/// 不像 <c>Channel&lt;IAsyncOperation&gt;</c> 那样把 struct 装箱到接口引用；</item>
/// <item><b>无 Per-op CTS 分配</b>：每 Worker 持有一个可复用 CTS，CancelAfter 仅设置内部 Timer。
/// stop 信号通过 stopToken.Register 回调显式 Cancel（替代 linked CTS）；</item>
/// <item><b>异步唤醒</b>：队列空时 Worker 通过 SemaphoreSlim.WaitAsync 真正休眠，空闲时接近零 CPU；</item>
/// <item><b>批量 drain</b>：Worker 唤醒后连续 TryDequeue 直到队列空，减少信号开销；</item>
/// <item><b>共享 Worker 池</b>：固定数量 Worker 串行 await，跨 Actor 并行；</item>
/// <item><b>Stop 信号传递</b>：stopToken.Cancel → workerCts.Cancel → in-flight work 取消 + Worker 循环退出。</item>
/// </list>
/// </para>
/// <para>
/// 用法：每个 Actor Domain 创建一个 <see cref="DomainWorkLane{TWork}"/> 实例，
/// 在 Behavior 的 Receive 中通过 <see cref="ActorContext.TryReserveOutstandingOperation"/>
/// 预留槽位后直接调用 <see cref="TrySubmit"/> 提交。
/// </para>
/// </summary>
public sealed class DomainWorkLane<TWork> : IAsyncDisposable
    where TWork : struct, IAsyncOperation
{
    private readonly BoundedMpmcRing<TWork> _ring;
    private readonly CacheLinePaddedCounter _submittedCount = new();
    private readonly CacheLinePaddedCounter _completedCount = new();
    private readonly CacheLinePaddedCounter _rejectedCount = new();
    private readonly CacheLinePaddedCounter _failedCount = new();
    private readonly CacheLinePaddedCounter _timeoutCount = new();

    private readonly int _maxConcurrency;
    private readonly TimeSpan _operationTimeout;
    private readonly List<Task> _workers = new();
    private readonly CancellationTokenSource _stopCts = new();
    // 二值唤醒信号：替代计数信号量，匹配批量 Drain 语义。
    // _signalState: 0 = Worker 正在 Drain 或已唤醒，1 = 需要唤醒。
    // 生产者只在 0→1 时 Release()，Worker 唤醒后重置为 0 并 Drain 全部 Work。
    // 避免计数信号量下"入队 N 条 → N 个 permit → 1 次 Drain 消费 1 permit → N-1 次空唤醒"。
    private readonly SemaphoreSlim _signal = new(0, 1);
    private int _signalState;

    private int _queuedCount;
    private int _inflightCount;
    private int _outstandingCount;
    private volatile bool _stopping;

    public DomainWorkLane(
        int maxConcurrency,
        int queueCapacity,
        TimeSpan operationTimeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxConcurrency, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(queueCapacity, 1);
        // queueCapacity 向上取整到 2 的幂（BoundedMpscRing 要求）
        var ringCapacity = NextPowerOfTwo(queueCapacity);

        _maxConcurrency = maxConcurrency;
        _operationTimeout = operationTimeout;
        _ring = new BoundedMpmcRing<TWork>(ringCapacity);
    }

    public int PendingCount => Volatile.Read(ref _outstandingCount);
    public int QueuedCount => Volatile.Read(ref _queuedCount);
    public int InflightCount => Volatile.Read(ref _inflightCount);
    public long TotalSubmitted => _submittedCount.Read();
    public long TotalCompleted => _completedCount.Read();
    public long TotalRejected => _rejectedCount.Read();

    /// <summary>
    /// 提交一条领域 Work。队列满时返回 false（调用方应 Suspend 或丢弃）。
    /// 不装箱：TWork 直接存入 ring buffer。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TrySubmit(in TWork work)
    {
        if (_stopping)
            return false;

        Interlocked.Increment(ref _queuedCount);
        Interlocked.Increment(ref _outstandingCount);
        if (!_ring.TryEnqueue(work))
        {
            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Decrement(ref _outstandingCount);
            _rejectedCount.Increment();
            return false;
        }

        _submittedCount.Increment();
        // 二值唤醒：只在 0→1 时 Release()，避免计数信号量的 permit 累积。
        // 若 Worker 正在 Drain（_signalState=0），设为 1 并唤醒；
        // 若已有待处理唤醒（_signalState=1），不重复 Release。
        if (Interlocked.Exchange(ref _signalState, 1) == 0)
        {
            _signal.Release();
        }
        return true;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_workers.Count > 0)
            return Task.CompletedTask;

        for (var i = 0; i < _maxConcurrency; i++)
            _workers.Add(RunWorkerAsync(_stopCts.Token));

        return Task.CompletedTask;
    }

    private async Task RunWorkerAsync(CancellationToken stopToken)
    {
        // 使用 Holder 包装当前 CTS：stopRegistration 回调通过 Holder 间接访问，
        // 确保 workerCts 被 TryReset 替换后回调仍能 Cancel 最新的 CTS。
        var holder = new CtsHolder(new CancellationTokenSource());
        // stopToken 回调：Cancel holder 内当前的 CTS。捕获 holder 而非 CTS 本身，
        // 使 CTS 替换后回调仍指向新 CTS。ObjectDisposedException 防御旧 CTS 被回收后的竞态。
        var stopRegistration = stopToken.Register(
            static state =>
            {
                var h = (CtsHolder)state!;
                var cts = h.Current;
                try { cts.Cancel(); }
                catch (ObjectDisposedException) { }
            },
            holder);

        try
        {
            while (true)
            {
                // 重置二值信号为 0（表示"有 Worker 正在 Drain"），然后批量 drain。
                // 关键：必须在 Drain 之前重置，而非之后。否则 Drain 期间（可能耗时很长，
                // 如慢速授权 I/O）_signalState 保持 1，生产者 Exchange 返回 1 不 Release，
                // 其他 Worker 永远不会被唤醒——表现为单慢请求阻塞全部后续请求。
                Volatile.Write(ref _signalState, 0);

                // 尝试批量 drain：一次唤醒处理尽可能多的 work，减少信号开销。
                while (_ring.TryDequeue(out var work))
                {
                    Interlocked.Decrement(ref _queuedCount);
                    Interlocked.Increment(ref _inflightCount);

                    // 多 Worker 信号补传：取出一条 work 后若队列非空，立即唤醒其他 Worker。
                    // 背景：二值信号合并入队——多条 work 快速入队时仅一次 Release()。
                    // 若当前 Worker 取出慢 I/O work 并 await 阻塞，剩余 fast work 会被搁置
                    // 在队列中无人处理（其他 Worker 在 WaitAsync 上无 permit 可消费）。
                    // 此处补传信号后，其他 Worker 可并行取出剩余 work，避免慢请求阻塞快请求。
                    if (_ring.Count > 0)
                    {
                        if (Interlocked.Exchange(ref _signalState, 1) == 0)
                            _signal.Release();
                    }

                    var workerCts = holder.Current;
                    try
                    {
                        await ExecuteWorkAsync(work, workerCts, stopToken)
                            .ConfigureAwait(false);
                        _completedCount.Increment();
                    }
                    catch (OperationCanceledException)
                        when (!stopToken.IsCancellationRequested && workerCts.IsCancellationRequested)
                    {
                        // 超时（非 stop）：workerCts 被 CancelAfter 触发。
                        _timeoutCount.Increment();
                        NotifyFailure(work, exception: null, AsyncOperationFailureKind.TimedOut);
                    }
                    catch (OperationCanceledException)
                        when (stopToken.IsCancellationRequested)
                    {
                        NotifyFailure(work, exception: null, AsyncOperationFailureKind.RuntimeStopping);
                    }
                    catch (Exception ex)
                    {
                        _failedCount.Increment();
                        NotifyFailure(work, ex, AsyncOperationFailureKind.Faulted);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _inflightCount);
                        Interlocked.Decrement(ref _outstandingCount);
                        // 复用 CTS：TryReset 清除取消状态，供下一个 work 使用。
                        // TryReset 失败（CTS 已取消）时创建新 CTS 替换。
                        // 旧 CTS 不立即 Dispose——holder 回调可能仍通过 Volatile.Read 访问它。
                        // 旧 CTS 由 GC 终结器处理 Timer 释放。
                        if (!workerCts.TryReset())
                        {
                            holder.Replace(new CancellationTokenSource());
                        }
                    }
                }

                // Drain 完成：队列空。重检防止"重置 _signalState=0 → 新入队设 1 并 Release → WaitAsync"竞态。
                // 若在 Drain 最后一次 TryDequeue 与此处之间有新入队，重检发现非空则继续 Drain。
                if (_ring.Count > 0)
                {
                    // 有新 Work 入队：继续循环（循环顶部会重置 _signalState=0 再 Drain）。
                    continue;
                }

                try
                {
                    await _signal.WaitAsync(stopToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            // 先反注册 stopToken 回调，再 Dispose 当前 CTS。
            stopRegistration.Dispose();
            holder.Current.Dispose();
        }
    }

    /// <summary>
    /// CTS 持有者：允许 stopToken 回调通过 Volatile.Read 访问最新的 CTS，
    /// 即使 Worker 在 TryReset 失败后替换了 CTS。
    /// </summary>
    private sealed class CtsHolder
    {
        private CancellationTokenSource _current;
        public CancellationTokenSource Current => Volatile.Read(ref _current);
        public CtsHolder(CancellationTokenSource initial) => _current = initial;
        public void Replace(CancellationTokenSource next) => Volatile.Write(ref _current, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask ExecuteWorkAsync(
        TWork work,
        CancellationTokenSource workerCts,
        CancellationToken stopToken)
    {
        if (_operationTimeout <= TimeSpan.Zero)
        {
            // 无超时：直接用 stopToken
            await work.ExecuteAsync(stopToken).ConfigureAwait(false);
            return;
        }

        // 复用 Worker 级 CTS：CancelAfter 仅设置内部 Timer（无 CTS 分配），
        // stop 时通过注册回调显式 Cancel（替代 linked CTS 的联动）。
        workerCts.CancelAfter(_operationTimeout);
        await work.ExecuteAsync(workerCts.Token).ConfigureAwait(false);
    }

    private static void NotifyFailure(
        in TWork work,
        Exception? exception,
        AsyncOperationFailureKind kind)
    {
        try
        {
            // in 参数不能调用扩展方法，先复制到本地
            var w = work;
            w.OnFailure(exception, kind);
        }
        catch
        {
            // Failure callback 不能终止 worker
        }
    }

    public async Task StopAsync()
    {
        if (_stopping)
            return;

        _stopping = true;
        await _stopCts.CancelAsync().ConfigureAwait(false);

        try
        {
            await Task.WhenAll(_workers).ConfigureAwait(false);
        }
        catch
        {
        }

        // 排空残留 work 并通知失败
        while (_ring.TryDequeue(out var abandoned))
        {
            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Decrement(ref _outstandingCount);
            NotifyFailure(abandoned, exception: null, AsyncOperationFailureKind.RuntimeStopping);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _stopCts.Dispose();
        _signal.Dispose();
    }

    public DomainWorkLaneSnapshot GetSnapshot()
        => new()
        {
            PendingCount = PendingCount,
            QueuedCount = QueuedCount,
            InflightCount = InflightCount,
            TotalSubmitted = _submittedCount.Read(),
            TotalCompleted = _completedCount.Read(),
            TotalRejected = _rejectedCount.Read(),
            TotalFailed = _failedCount.Read(),
            TotalTimeout = _timeoutCount.Read()
        };

    private static int NextPowerOfTwo(int value)
    {
        if (value <= 1) return 2;
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
}

public readonly struct DomainWorkLaneSnapshot
{
    public int PendingCount { get; init; }
    public int QueuedCount { get; init; }
    public int InflightCount { get; init; }
    public long TotalSubmitted { get; init; }
    public long TotalCompleted { get; init; }
    public long TotalRejected { get; init; }
    public long TotalFailed { get; init; }
    public long TotalTimeout { get; init; }
}

/// <summary>
/// 领域 Work 提交接口：由 ActorShard 实现，路由 TWork 到对应的 DomainWorkLane。
/// </summary>
internal interface IActorWorkSink<TWork> where TWork : struct, IAsyncOperation
{
    bool TrySubmit(in TWork work);
}
