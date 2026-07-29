using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// 有界多生产者多消费者 (MPMC) 环形队列。基于 Dmitry Vyukov 的 MPMC bounded queue 算法。
/// <para>
/// 与 <see cref="BoundedMpscRing{T}"/> 的区别：消费端使用 CAS 推进 <c>_tail</c>，
/// 保证多消费者安全。竞争失败的消费者返回 false，由调用方重试。
/// </para>
/// <para>
/// 用于 <see cref="Scheduling.DomainWorkLane{TWork}"/> 的共享 Worker 池：
/// 多个 Worker 同时从同一 Ring 出队，需要 CAS 保证安全。
/// </para>
/// </summary>
internal sealed class BoundedMpmcRing<T> where T : struct
{
    private readonly int _mask;
    private readonly Cell[] _buffer;
    private PaddedLong _head;
    private PaddedLong _tail;

    public BoundedMpmcRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 2);
        if ((capacity & (capacity - 1)) != 0)
            throw new ArgumentOutOfRangeException(
                nameof(capacity), "capacity must be a power of two");
        _mask = capacity - 1;
        _buffer = new Cell[capacity];
        for (var i = 0; i < capacity; i++)
        {
            _buffer[i] = new Cell { Sequence = i, Value = default! };
        }

        _head.Value = 0;
        _tail.Value = 0;
    }

    public int Capacity => _mask + 1;

    public int Count
    {
        get
        {
            var head = Interlocked.Read(ref _head.Value);
            var tail = Volatile.Read(ref _tail.Value);
            var diff = head - tail;
            return diff < 0 ? 0 : (int)Math.Min(diff, Capacity);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(in T item)
    {
        var pos = Interlocked.Read(ref _head.Value);
        for (;;)
        {
            ref var first = ref MemoryMarshal.GetArrayDataReference(_buffer);
            ref var cell = ref Unsafe.Add(
                ref first,
                (nint)(pos & _mask));
            var seq = Volatile.Read(ref cell.Sequence);
            var diff = seq - pos;

            if (diff == 0)
            {
                if (Interlocked.CompareExchange(ref _head.Value, pos + 1, pos) == pos)
                {
                    cell.Value = item;
                    Volatile.Write(ref cell.Sequence, pos + 1);
                    return true;
                }

                pos = Interlocked.Read(ref _head.Value);
            }
            else if (diff < 0)
            {
                return false;
            }
            else
            {
                pos = Interlocked.Read(ref _head.Value);
            }
        }
    }

    /// <summary>
    /// 多消费者出队。队列为空或竞争失败时返回 false。
    /// <para>
    /// CAS 推进 <c>_tail</c>：竞争失败者返回 false，由调用方重试。
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T item)
    {
        var tail = Volatile.Read(ref _tail.Value);
        ref var first = ref MemoryMarshal.GetArrayDataReference(_buffer);
        ref var cell = ref Unsafe.Add(
            ref first,
            (nint)(tail & _mask));
        var seq = Volatile.Read(ref cell.Sequence);
        var diff = seq - (tail + 1);

        if (diff == 0)
        {
            if (Interlocked.CompareExchange(ref _tail.Value, tail + 1, tail) != tail)
            {
                item = default!;
                return false;
            }

            item = cell.Value!;
            cell.Value = default!;
            Volatile.Write(ref cell.Sequence, tail + Capacity);
            return true;
        }

        item = default!;
        return false;
    }

    public int Drain(Span<T> output, int max)
    {
        var n = 0;
        var limit = Math.Min(max, output.Length);
        while (n < limit && TryDequeue(out output[n]!))
            n++;
        return n;
    }

    [StructLayout(LayoutKind.Sequential, Size = 64)]
    private struct Cell
    {
        public long Sequence;
        public T Value;
    }

    [StructLayout(LayoutKind.Sequential, Size = 64)]
    private struct PaddedLong
    {
#pragma warning disable CS0649
        public long Value;
#pragma warning restore CS0649
    }
}
