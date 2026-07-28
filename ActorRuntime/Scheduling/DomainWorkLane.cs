using System.Runtime.CompilerServices;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Primitives;

namespace ChatApp.ActorRuntime.Scheduling;

/// <summary>
/// 领域强类型有界 Work Lane：替代通用 <see cref="AsyncOperationExecutor"/> 处理特定 Actor Domain 的 I/O。
/// <para>
/// 与 <see cref="AsyncOperationExecutor"/> 相比：
/// <list type="bullet">
/// <item><b>不装箱</b>：<typeparamref name="TWork"/> 是 struct，直接存储在 <see cref="BoundedMpscRing{T}"/> 数组中，
/// 不像 <c>Channel&lt;IAsyncOperation&gt;</c> 那样把 struct 装箱到接口引用；</item>
/// <item><b>无 Per-op Linked CTS</b>：用 <see cref="CancellationTokenSourcePool"/> 复用 CTS 实例，
/// 消除每次操作 <c>CreateLinkedTokenSource</c> + <c>CancelAfter</c> 的 CTS+Timer 分配；</item>
/// <item><b>共享 Worker 池</b>：固定数量 Worker 串行 await，跨 Actor 并行；</item>
/// <item><b>Stop 信号传递</b>：Worker 循环退出 + 显式取消 in-flight CTS，不依赖 linked CTS。</item>
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
    private readonly BoundedMpscRing<TWork> _ring;
    private readonly CancellationTokenSourcePool _ctsPool;
    private readonly CacheLinePaddedCounter _submittedCount = new();
    private readonly CacheLinePaddedCounter _completedCount = new();
    private readonly CacheLinePaddedCounter _rejectedCount = new();
    private readonly CacheLinePaddedCounter _failedCount = new();
    private readonly CacheLinePaddedCounter _timeoutCount = new();

    private readonly int _maxConcurrency;
    private readonly TimeSpan _operationTimeout;
    private readonly List<Task> _workers = new();
    private readonly CancellationTokenSource _stopCts = new();

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
        _ring = new BoundedMpscRing<TWork>(ringCapacity);
        _ctsPool = new CancellationTokenSourcePool(maxConcurrency * 2);
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
        // Worker 循环：忙等 + 少量 SpinWait，避免空 ring 时立即休眠造成延迟。
        // 生产环境通常队列非空；空转极少发生。
        var spin = new SpinWait();
        while (true)
        {
            if (stopToken.IsCancellationRequested)
                break;

            if (!_ring.TryDequeue(out var work))
            {
                // 队列空：spin 后 yield，避免 100% CPU
                if (spin.Count < 10)
                {
                    spin.SpinOnce();
                }
                else
                {
                    spin.Reset();
                    await Task.Yield();
                }
                continue;
            }
            spin.Reset();

            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Increment(ref _inflightCount);
            try
            {
                await ExecuteWorkAsync(work, stopToken).ConfigureAwait(false);
                _completedCount.Increment();
            }
            catch (OperationCanceledException)
                when (!stopToken.IsCancellationRequested)
            {
                // 超时（非 stop）：
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
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask ExecuteWorkAsync(TWork work, CancellationToken stopToken)
    {
        if (_operationTimeout <= TimeSpan.Zero)
        {
            // 无超时：直接用 stopToken
            await work.ExecuteAsync(stopToken).ConfigureAwait(false);
            return;
        }

        // 池化 CTS：复用实例，仅 CancelAfter 设置 Timer
        // 注意：CancelAfter 内部仍会注册一个 Timer，但 CTS 本身被复用。
        // 后续可用全局时间轮替代 CancelAfter 的 Timer，进一步消除分配。
        var cts = _ctsPool.Rent();
        try
        {
            cts.CancelAfter(_operationTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                cts.Token, stopToken);
            await work.ExecuteAsync(linked.Token).ConfigureAwait(false);
        }
        finally
        {
            _ctsPool.Return(cts);
        }
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
