using System.Buffers;
using System.Runtime.CompilerServices;
using ChatApp.ActorRuntime.Abstractions;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 单 Actor 的状态、Mailbox 与 Ready Queue 链接。所有字段仅由所属 Shard 消费线程修改。
/// FIFO 前四项直接内嵌，超过后才租用 ArrayPool 环形数组，避免每 Actor 创建 Queue 对象。
/// </summary>
internal sealed class ActorCell<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    private const int InlineCapacity = 4;

    public TKey Key;
    public TState State;
    public uint Generation;
    public ActorCellFlags Flags;
    public long LastActiveTimestamp;

    // 侵入式 Ready Queue 链接；Scheduled 标志确保每个 Cell 至多入队一次。
    public ActorCell<TKey, TState, TMessage>? ReadyNext;

    private InlineMailbox _inlineMailbox;
    private ActorMailboxItem<TMessage>[]? _fifoBuffer;
    private int _fifoBufferMask;
    private int _fifoHead;
    private int _fifoCount;

    private ActorMailboxItem<TMessage> _latestMessage;
    private bool _hasLatest;

    // Suspend Actor 的 Completion 独立高优先级槽，不受普通 Mailbox 是否为空影响。
    private ActorMailboxItem<TMessage> _completion;
    private bool _hasCompletion;

    private ActorCell()
    {
        Key = default!;
    }

    public static ActorCell<TKey, TState, TMessage> Create(
        in TKey key,
        uint generation,
        long timestamp)
    {
        return new ActorCell<TKey, TState, TMessage>
        {
            Key = key,
            Generation = generation,
            LastActiveTimestamp = timestamp,
            Flags = ActorCellFlags.Active
        };
    }

    public bool IsActive => (Flags & ActorCellFlags.Active) != 0;
    public bool IsBusy => (Flags & ActorCellFlags.Busy) != 0;
    public bool IsScheduled => (Flags & ActorCellFlags.Scheduled) != 0;

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

        becameReady = _fifoCount == 0 && !_hasLatest && !_hasCompletion;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(
        bool completionOnly,
        out ActorMailboxItem<TMessage> item,
        out bool wasCompletion)
    {
        if (_hasCompletion)
        {
            item = _completion;
            _completion = default;
            _hasCompletion = false;
            wasCompletion = true;
            return true;
        }

        if (completionOnly)
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
        _fifoCount + (_hasLatest ? 1 : 0) + (_hasCompletion ? 1 : 0);

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
}

[Flags]
internal enum ActorCellFlags : byte
{
    None = 0,
    Active = 1,
    Busy = 2,
    Scheduled = 4
}
