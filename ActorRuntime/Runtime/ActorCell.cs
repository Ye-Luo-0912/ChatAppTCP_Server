using System.Buffers;
using System.Runtime.CompilerServices;
using ChatApp.ActorRuntime.Abstractions;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 单 Actor 的状态、Mailbox、控制通道与 Ready Queue 链接。所有字段仅由所属 Shard 消费线程修改。
/// FIFO 前四项直接内嵌，超过后才租用 ArrayPool 环形数组，避免每 Actor 创建 Queue 对象。
/// <para>
/// 控制通道（Completion 槽 + Deadline FIFO）独立于业务 Mailbox：
/// Busy（Suspend 等待 I/O）的 Actor 暂停业务 Mailbox，但仍处理控制消息。
/// 一个 Actor 同一时刻最多一个 Outstanding Operation（Completion 槽容量 1 即为该约束的物化），
/// Deadline 控制消息数受 <see cref="MaxControlDeadlines"/> 上限约束。
/// </para>
/// </summary>
internal sealed class ActorCell<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    private const int InlineCapacity = 4;

    /// <summary>
    /// 每 Actor Deadline 控制上限：未触发（<see cref="PendingDeadlineCount"/>）
    /// 与已触发未消费（Deadline FIFO 内）之和不超过该值。
    /// </summary>
    public const int MaxControlDeadlines = 4;

    public TKey Key;
    public TState State;

    /// <summary>当前激活纪元。由 Shard 单调计数器在 Activate 时分配（不按 Key 重置）。</summary>
    public ActivationId Activation;

    public ActorCellFlags Flags;
    public long LastActiveTimestamp;

    /// <summary>未触发 Deadline 数（已触发进入控制通道的不计入）。Idle Sweep 依据之一。</summary>
    public int PendingDeadlineCount;

    /// <summary>
    /// Deadline 代际：TryScheduleOrReplace / CancelDeadlines 时自增，
    /// 使时间轮中尚未触发的旧条目在触发时被识别为过期（惰性取消）。
    /// </summary>
    public uint DeadlineEpoch;

    /// <summary>是否存在未完成的异步操作。为 true 时 TrySubmitOperation 拒绝新提交。</summary>
    public bool HasOutstandingOperation;

    // 侵入式 Ready Queue 链接；Scheduled 标志确保每个 Cell 至多入队一次。
    public ActorCell<TKey, TState, TMessage>? ReadyNext;

    private InlineMailbox _inlineMailbox;
    private ActorMailboxItem<TMessage>[]? _fifoBuffer;
    private int _fifoBufferMask;
    private int _fifoHead;
    private int _fifoCount;

    private ActorMailboxItem<TMessage> _latestMessage;
    private bool _hasLatest;

    // 控制通道：Completion 单槽（Outstanding Operation 唯一），不受普通 Mailbox 是否为空影响。
    private ActorMailboxItem<TMessage> _completion;
    private bool _hasCompletion;

    // 控制通道：Deadline FIFO（内联，容量 MaxControlDeadlines）。
    private InlineDeadlineControl _deadlineControl;
    private int _deadlineHead;
    private int _deadlineCount;

    private ActorCell()
    {
        Key = default!;
    }

    public static ActorCell<TKey, TState, TMessage> Create(
        in TKey key,
        ActivationId activation,
        long timestamp)
    {
        return new ActorCell<TKey, TState, TMessage>
        {
            Key = key,
            Activation = activation,
            LastActiveTimestamp = timestamp,
            Flags = ActorCellFlags.Active
        };
    }

    public bool IsActive => (Flags & ActorCellFlags.Active) != 0;
    public bool IsBusy => (Flags & ActorCellFlags.Busy) != 0;
    public bool IsScheduled => (Flags & ActorCellFlags.Scheduled) != 0;

    /// <summary>已触发未消费的 Deadline 控制消息数。</summary>
    public int PendingDeadlineControlCount => _deadlineCount;

    /// <summary>控制通道是否有待处理消息（Completion 槽或 Deadline FIFO）。</summary>
    public bool HasPendingControl => _hasCompletion || _deadlineCount > 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueueMessage(
        in ActorMailboxItem<TMessage> item,
        ActorMailboxMode mode,
        int fifoCapacity,
        out bool becameReady,
        out ActorMailboxItem<TMessage> replaced,
        out bool hasReplaced)
    {
        becameReady = false;
        replaced = default;
        hasReplaced = false;

        if (!IsActive)
            return ActorPostStatus.ActorClosed;

        if (mode == ActorMailboxMode.LatestOnly)
        {
            var hadLatest = _hasLatest;
            if (hadLatest)
            {
                replaced = _latestMessage;
                hasReplaced = true;
            }

            _latestMessage = item;
            _hasLatest = true;
            becameReady = !hadLatest;
            return hadLatest
                ? ActorPostStatus.Replaced
                : ActorPostStatus.Accepted;
        }

        if (_fifoCount >= fifoCapacity)
            return ActorPostStatus.MailboxFull;

        becameReady = _fifoCount == 0 && !_hasLatest && !_hasCompletion && _deadlineCount == 0;
        EnsureFifoStorage(_fifoCount + 1, fifoCapacity);

        if (_fifoBuffer is null)
        {
            _inlineMailbox[(_fifoHead + _fifoCount) & (InlineCapacity - 1)] = item;
        }
        else
        {
            _fifoBuffer[(_fifoHead + _fifoCount) & _fifoBufferMask] = item;
        }

        _fifoCount++;
        return ActorPostStatus.Accepted;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueueCompletion(
        in ActorMailboxItem<TMessage> item,
        out bool replaced,
        out ActorMailboxItem<TMessage> replacedItem)
    {
        replaced = false;
        replacedItem = default;
        if (_hasCompletion)
            return ActorPostStatus.MailboxFull;

        _completion = item;
        _hasCompletion = true;
        return ActorPostStatus.Accepted;
    }

    /// <summary>
    /// 向 Deadline 控制 FIFO 投递已触发的 Deadline 消息。
    /// 容量由调度侧不变量保证（未触发 + 已触发未消费 ≤ <see cref="MaxControlDeadlines"/>），
    /// 正常路径不会返回 false；false 表示内部不变量被破坏。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueueDeadline(in ActorMailboxItem<TMessage> item)
    {
        if (_deadlineCount >= MaxControlDeadlines)
            return false;

        _deadlineControl[(_deadlineHead + _deadlineCount) & (MaxControlDeadlines - 1)] = item;
        _deadlineCount++;
        return true;
    }

    /// <summary>
    /// 按优先级出队：Completion → Deadline 控制 →（<paramref name="controlOnly"/> 为 false 时）业务 Mailbox。
    /// Busy Actor 传 controlOnly: true，仅处理控制消息。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(
        bool controlOnly,
        out ActorMailboxItem<TMessage> item,
        out bool wasCompletion)
    {
        if (_hasCompletion)
        {
            item = _completion;
            _completion = default;
            _hasCompletion = false;
            HasOutstandingOperation = false;
            wasCompletion = true;
            return true;
        }

        if (_deadlineCount > 0)
        {
            var index = _deadlineHead & (MaxControlDeadlines - 1);
            item = _deadlineControl[index];
            _deadlineControl[index] = default;
            _deadlineHead = (_deadlineHead + 1) & (MaxControlDeadlines - 1);
            _deadlineCount--;
            wasCompletion = false;
            return true;
        }

        if (controlOnly)
        {
            item = default;
            wasCompletion = false;
            return false;
        }

        if (_hasLatest)
        {
            item = _latestMessage;
            _latestMessage = default;
            _hasLatest = false;
            wasCompletion = false;
            return true;
        }

        if (_fifoCount == 0)
        {
            item = default;
            wasCompletion = false;
            return false;
        }

        if (_fifoBuffer is null)
        {
            var index = _fifoHead & (InlineCapacity - 1);
            item = _inlineMailbox[index];
            _inlineMailbox[index] = default;
            _fifoHead = (_fifoHead + 1) & (InlineCapacity - 1);
        }
        else
        {
            var index = _fifoHead & _fifoBufferMask;
            item = _fifoBuffer[index];
            _fifoBuffer[index] = default;
            _fifoHead = (_fifoHead + 1) & _fifoBufferMask;
        }

        _fifoCount--;
        wasCompletion = false;
        return true;
    }

    public int PendingCount =>
        _fifoCount + (_hasLatest ? 1 : 0) + (_hasCompletion ? 1 : 0) + _deadlineCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkBusy() => Flags |= ActorCellFlags.Busy;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ClearBusy() => Flags &= ~ActorCellFlags.Busy;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkScheduled() => Flags |= ActorCellFlags.Scheduled;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ClearScheduled() => Flags &= ~ActorCellFlags.Scheduled;

    public void Deactivate()
    {
        Flags &= ~(ActorCellFlags.Active | ActorCellFlags.Busy | ActorCellFlags.Scheduled);
        ReadyNext = null;
    }

    public void ReleaseStorage()
    {
        if (_fifoBuffer is null)
            return;

        ArrayPool<ActorMailboxItem<TMessage>>.Shared.Return(
            _fifoBuffer,
            clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<ActorMailboxItem<TMessage>>());
        _fifoBuffer = null;
        _fifoBufferMask = 0;
        _fifoHead = 0;
    }

    private void EnsureFifoStorage(int required, int logicalCapacity)
    {
        if (_fifoBuffer is null && required <= InlineCapacity)
            return;

        if (_fifoBuffer is not null && required <= _fifoBufferMask + 1)
            return;

        var requested = NextPowerOfTwo(Math.Max(8, Math.Min(required * 2, logicalCapacity)));
        var newBuffer = ArrayPool<ActorMailboxItem<TMessage>>.Shared.Rent(requested);
        var newMask = requested - 1;

        for (var i = 0; i < _fifoCount; i++)
        {
            if (_fifoBuffer is null)
            {
                var oldIndex = (_fifoHead + i) & (InlineCapacity - 1);
                newBuffer[i] = _inlineMailbox[oldIndex];
                _inlineMailbox[oldIndex] = default;
            }
            else
            {
                var oldIndex = (_fifoHead + i) & _fifoBufferMask;
                newBuffer[i] = _fifoBuffer[oldIndex];
                _fifoBuffer[oldIndex] = default;
            }
        }

        if (_fifoBuffer is not null)
        {
            ArrayPool<ActorMailboxItem<TMessage>>.Shared.Return(
                _fifoBuffer,
                clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<ActorMailboxItem<TMessage>>());
        }

        _fifoBuffer = newBuffer;
        _fifoBufferMask = newMask;
        _fifoHead = 0;
    }

    private static int NextPowerOfTwo(int value)
    {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }

    [InlineArray(InlineCapacity)]
    private struct InlineMailbox
    {
        private ActorMailboxItem<TMessage> _element0;
    }

    [InlineArray(MaxControlDeadlines)]
    private struct InlineDeadlineControl
    {
        private ActorMailboxItem<TMessage> _element0;
    }
}

[Flags]
internal enum ActorCellFlags : byte
{
    None = 0,
    Active = 1,
    Busy = 2,
    Scheduled = 4
}
