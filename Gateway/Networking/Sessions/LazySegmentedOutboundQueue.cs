using System.Threading.Tasks.Sources;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Lazy Segmented MPSC Outbound Queue —— 替换 <see cref="Channel{T}"/> 的零分配空闲连接方案。
/// <para>
/// 空闲连接零段分配：首次 <see cref="TryWrite"/> 才分配首个 <see cref="Segment"/>（16 槽）。
/// 多生产者（业务线程 / Realtime 事件分发器 / Ephemeral pipeline 入队）单消费者
/// （<c>SendLoop</c>/<c>drain</c>/<c>pump</c> 驱动，由 TcpClientSession 状态机保证同时只有一个）。
/// </para>
/// <para>
/// 与 <c>Channel.CreateBounded&lt;OutboundWrite&gt;(N, FullMode=Wait)</c> + <c>TryWrite</c> 的差异：
/// <list type="bullet">
/// <item>空闲连接不预分配段，每连接节省约 87% 出站队列内存（见 OutboundChannel.Benchmarks）；</item>
/// <item>满时 <see cref="TryWrite"/> 返回 <c>false</c>（与 BoundedChannelFullMode.Wait + TryWrite 行为一致）；</item>
/// <item><see cref="TryComplete"/> 后 <see cref="TryWrite"/> 返回 <c>false</c>，
/// <see cref="WaitToReadAsync"/> 排空残留帧后返回 <c>false</c>。</item>
/// </list>
/// </para>
/// <para>
/// <b>线程安全模型</b>：多生产者通过 CAS 在段内保留槽位（<see cref="Segment._producerCursor"/>），
/// 段满时在 <c>_segmentLock</c> 下分配新段并链接。单消费者通过 <see cref="Segment._consumerCursor"/>
/// 顺序读取，无需锁。槽位发布通过 <see cref="Segment._commitMask"/> 位掩码 +
/// <see cref="Interlocked.Or"/> 保证跨核可见性，解决 MPSC 乱序完成问题
/// （保留 slot 2 的生产者先于 slot 1 完成写入时，消费者不会读到未发布的 slot 1）。
/// </para>
/// </summary>
internal sealed class LazySegmentedOutboundQueue : IOutboundQueue, IValueTaskSource<bool>
{
    /// <summary>每段槽位数。16 槽：兼顾首段内存成本（~0.5 KiB）与段分配频率。</summary>
    private const int SegmentSize = 16;
    private const int SegmentSizeMask = SegmentSize - 1;

    private readonly int _capacity;
    private readonly object _segmentLock = new();

    // _tail：消费者端（最旧段，TryRead 推进）。单消费者变更，无需原子。
    // _head：生产者端（最新段，TryWrite 追加）。生产者 Volatile.Read、锁内 Volatile.Write。
    private Segment? _tail;
    private Segment? _head;
    private int _count;
    private int _completed;

    // 单消费者异步信号：MRVTSC 复用，零分配等待。非 readonly：Reset/SetResult 原地修改结构体。
    private ManualResetValueTaskSourceCore<bool> _signal;
    private int _hasWaiter; // 0/1
    private CancellationTokenRegistration _registration;
    private CancellationToken _registeredToken;

    static LazySegmentedOutboundQueue()
    {
        // 编译期校验 SegmentSize 为 2 的幂且 ≤ 31（int 位掩码上限）。
        if (SegmentSize <= 0 || (SegmentSize & SegmentSizeMask) != 0 || SegmentSize > 31)
            throw new InvalidOperationException("SegmentSize must be a power of two <= 31");
    }

    public LazySegmentedOutboundQueue(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(capacity, 0);
        _capacity = capacity;
        _signal.RunContinuationsAsynchronously = true;
    }

    /// <summary>
    /// 多生产者入队。满或已 Complete 时返回 false。
    /// </summary>
    public bool TryWrite(OutboundWrite item)
    {
        if (Volatile.Read(ref _completed) != 0)
            return false;

        // 有界容量：先递增再校验，CAS-safe 防止多生产者同时通过容量检查。
        if (Interlocked.Increment(ref _count) > _capacity)
        {
            Interlocked.Decrement(ref _count);
            return false;
        }

        // 在 head 段保留槽位，段满则锁内分配新段。
        while (true)
        {
            var head = Volatile.Read(ref _head);
            if (head is not null)
            {
                var slot = Interlocked.Increment(ref head._producerCursor) - 1;
                if (slot < SegmentSize)
                {
                    head._items[slot] = item;
                    Interlocked.Or(ref head._commitMask, 1 << slot);
                    SignalWaiter();
                    return true;
                }
                // 当前段已满，落入下方锁路径分配新段。
            }

            AllocateSegment(head);
            // 循环重试：head 已更新，再次尝试保留槽位。
        }
    }

    /// <summary>
    /// 标记队列完成。后续 TryWrite 返回 false；WaitToReadAsync 排空后返回 false。
    /// </summary>
    public void TryComplete()
    {
        Volatile.Write(ref _completed, 1);
        SignalWaiter();
    }

    /// <summary>
    /// 单消费者读取。无已发布项时返回 false。
    /// </summary>
    public bool TryRead(out OutboundWrite item)
    {
        while (true)
        {
            var tail = _tail;
            if (tail is null)
            {
                item = default;
                return false;
            }

            if (tail._consumerCursor < SegmentSize)
            {
                var mask = Volatile.Read(ref tail._commitMask);
                if ((mask & (1 << tail._consumerCursor)) == 0)
                {
                    // 槽位已保留但未发布（MPSC 乱序完成），无内容可读。
                    item = default;
                    return false;
                }

                item = tail._items[tail._consumerCursor];
                tail._items[tail._consumerCursor] = default;
                tail._consumerCursor++;
                Interlocked.Decrement(ref _count);
                return true;
            }

            // 当前段耗尽：仅当存在后继段时推进 _tail。
            // 不推进到 null——否则生产者分配新段后 SignalWaiter 内的 TryPeek 会读到 _tail=null
            // 误判为空 → SetResult(false) → 消费者误退出（丢失唤醒）。
            var next = Volatile.Read(ref tail._next);
            if (next is null)
            {
                item = default;
                return false;
            }
            _tail = next;
        }
    }

    /// <summary>
    /// 单消费者窥视。用于 <c>HasPendingWork()</c> 判断是否有可读项。
    /// 推进耗尽的 <c>_tail</c>（与 TryRead 一致，单消费者无竞态），
    /// 但不推进到 null（同 TryRead 理由）。
    /// </summary>
    public bool TryPeek(out OutboundWrite item)
    {
        while (true)
        {
            var tail = _tail;
            if (tail is null)
            {
                item = default;
                return false;
            }

            if (tail._consumerCursor < SegmentSize)
            {
                var mask = Volatile.Read(ref tail._commitMask);
                if ((mask & (1 << tail._consumerCursor)) == 0)
                {
                    item = default;
                    return false;
                }

                item = tail._items[tail._consumerCursor];
                return true;
            }

            // 段耗尽：仅当存在后继段时推进 _tail（同 TryRead 理由，不推进到 null）。
            var next = Volatile.Read(ref tail._next);
            if (next is null)
            {
                item = default;
                return false;
            }
            _tail = next;
        }
    }

    /// <summary>
    /// 单消费者异步等待可读项。有项返回 true；Complete 且排空后返回 false；取消抛 OCE。
    /// <para>
    /// 信号协议：<see cref="ManualResetValueTaskSourceCore{T}"/> 复用，零分配等待。
    /// 注册等待方后重检（catch 生产者在 fast-path 与注册之间的入队），
    /// 消除丢失唤醒。
    /// </para>
    /// </summary>
    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        // Fast path 1：已有可读项。
        if (TryPeek(out _))
            return new ValueTask<bool>(true);

        // Fast path 2：已 Complete 且无项。
        if (Volatile.Read(ref _completed) != 0)
            return new ValueTask<bool>(false);

        // 先 Reset，再注册取消，最后发布 waiter。取消注册必须先于 waiter 可见：这样任一
        // SignalWaiter/取消回调一旦认领 waiter，GetResult 都能可靠释放对应 registration。
        // token 若已取消，UnsafeRegister 的同步回调此时因 waiter 尚未发布而不会认领；
        // 发布后显式重检 IsCancellationRequested 即可补上该唤醒，不会丢失取消。
        _signal.Reset();
        _registeredToken = cancellationToken;
        _registration = cancellationToken.CanBeCanceled
            ? cancellationToken.UnsafeRegister(
                static state =>
                {
                    var q = (LazySegmentedOutboundQueue)state!;
                    q.TryCancelWaiter();
                },
                this)
            : default;
        Volatile.Write(ref _hasWaiter, 1);

        // 发布后按取消、可读、完成的顺序重检。重检只能通过 CAS 撤销自己仍拥有的 waiter。
        // 若 CAS 失败，说明 SignalWaiter/取消回调已认领本代信号；此时必须返回 MRVTSC-backed
        // ValueTask 等待认领方完成，不能同步返回并 Reset 下一代，否则旧认领方会完成新版本。
        if (cancellationToken.IsCancellationRequested)
            TryCancelWaiter();

        if (TryPeek(out _) && TryWithdrawWaiter())
        {
            DisposeCancellationRegistration();
            return new ValueTask<bool>(true);
        }
        if (Volatile.Read(ref _completed) != 0 && TryWithdrawWaiter())
        {
            DisposeCancellationRegistration();
            return new ValueTask<bool>(false);
        }

        return new ValueTask<bool>(this, _signal.Version);
    }

    /// <summary>
    /// 唤醒等待方。仅成功 CAS 认领当前 waiter 的线程实际 SetResult。
    /// <para>
    /// 始终 SetResult(true)：生产者线程不可触碰 <see cref="_tail"/>（单消费者不变量），
    /// 因此不能调用 <see cref="TryPeek"/> 判断是否有可读项。消费者被唤醒后通过自身的
    /// TryRead/TryPeek 决定继续消费或退出。Complete 且空的场景：消费者被唤醒 →
    /// TryRead 返回 false → 再次 WaitToReadAsync 命中 Fast path 2（_completed）→ 返回 false → 退出。
    /// </para>
    /// </summary>
    private void SignalWaiter()
    {
        if (Interlocked.CompareExchange(ref _hasWaiter, 0, 1) != 1)
            return;

        _signal.SetResult(true);
    }

    /// <summary>
    /// 取消当前 waiter。与入队/完成信号竞争同一个 CAS 线性化点，只有胜者完成 MRVTSC。
    /// </summary>
    private void TryCancelWaiter()
    {
        if (Interlocked.CompareExchange(ref _hasWaiter, 0, 1) != 1)
            return;

        _signal.SetException(new OperationCanceledException(_registeredToken));
    }

    /// <summary>
    /// 注册线程在发布后重检命中时撤销 waiter。失败表示另一个线程已认领本代信号，
    /// 调用方必须返回 MRVTSC-backed ValueTask，等待认领方完成。
    /// </summary>
    private bool TryWithdrawWaiter() =>
        Interlocked.CompareExchange(ref _hasWaiter, 0, 1) == 1;

    private void DisposeCancellationRegistration()
    {
        _registration.Dispose();
        _registration = default;
    }

    /// <summary>
    /// 锁内分配新段并链接到 head。重入后由调用方循环重试槽位保留。
    /// </summary>
    private void AllocateSegment(Segment? observedHead)
    {
        lock (_segmentLock)
        {
            var currentHead = Volatile.Read(ref _head);

            // 情况 1：observedHead 为 null（首段）。
            if (observedHead is null)
            {
                if (currentHead is null)
                {
                    var first = new Segment();
                    Volatile.Write(ref _head, first);
                    Volatile.Write(ref _tail, first);
                }
                // else: 其他生产者已分配首段，循环重试即可。
                return;
            }

            // 情况 2：observedHead 仍为当前 head（段满，需新段）。
            if (ReferenceEquals(currentHead, observedHead))
            {
                var next = new Segment();
                // 链接旧段 → 新段，消费者读完旧段后通过 _next 推进。
                Volatile.Write(ref observedHead._next, next);
                Volatile.Write(ref _head, next);
            }
            // else: 其他生产者已分配新段，循环重试即可。
        }
    }

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) =>
        _signal.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) =>
        _signal.OnCompleted(continuation, state, token, flags);

    bool IValueTaskSource<bool>.GetResult(short token)
    {
        try
        {
            return _signal.GetResult(token);
        }
        finally
        {
            // 正常完成与取消异常都必须释放注册，防止 callback/queue 引用泄漏。
            DisposeCancellationRegistration();
        }
    }

    /// <summary>
    /// 段：固定槽位数组 + 位掩码发布标记 + 单链 next。
    /// <para>
    /// 生产者 CAS 递增 <see cref="_producerCursor"/> 保留槽位，写入后 <see cref="Interlocked.Or"/>
    /// 发布对应 bit。消费者读 <see cref="_commitMask"/> 验证槽位已发布后读取，避免乱序完成。
    /// </para>
    /// </summary>
    private sealed class Segment
    {
        internal readonly OutboundWrite[] _items = new OutboundWrite[SegmentSize];
        // 生产者保留游标：CAS 递增，值域 [0, SegmentSize]。SegmentSize 表示段已满。
        internal int _producerCursor;
        // 消费者游标：单消费者变更，无需原子。指向下一个待读槽位。
        internal int _consumerCursor;
        // 发布位掩码：bit i = 1 表示 slot i 已写入。Interlocked.Or 发布，Volatile.Read 观察。
        internal int _commitMask;
        // 链表后继：生产者锁内 Volatile.Write，消费者 Volatile.Read（经 _tail._next）。
        internal Segment? _next;
    }
}
