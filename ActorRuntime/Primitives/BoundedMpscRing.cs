using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// 有界多生产者单消费者 (MPSC) 环形队列。基于 Dmitry Vyukov 的 MPMC bounded queue 算法，
/// 简化为单消费者：消费端无需 CAS，仅原子读 cursor。
/// <para>
/// 设计要点：
/// <list type="bullet">
/// <item>容量必须为 2 的幂，掩码 <c>capacity - 1</c> 替代取模；</item>
/// <item>每个槽位独立持有 sequence number（带缓存行对齐避免 false sharing）；</item>
/// <item>生产者通过 CAS 推进 <c>_head</c>，每个槽位写入后发布 sequence；</item>
/// <item>单消费者无锁推进 <c>_tail</c>，仅读取 sequence 判断可用性；</item>
/// <item>满返回 false 而非阻塞；调用方按 <see cref="ActorPostStatus.ShardOverloaded"/> 处理。</item>
/// </list>
/// </para>
/// <para>
/// 用于每 Shard 的 Ingress Ring：多线程 TCP Read / I/O Completion → 单 Shard Consumer。
/// </para>
/// </summary>
internal sealed class BoundedMpscRing<T> where T : struct
{
    private readonly int _mask;
    private readonly Cell[] _buffer;
    // _head: 生产者 cursor。CAS 推进。初始 0。
    private PaddedLong _head;
    // _tail: 单消费者 cursor。无需原子（单线程读写），但与 Cell.Sequence 通过 volatile 协调可见性。
    private PaddedLong _tail;

    public BoundedMpscRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 2);
        if ((capacity & (capacity - 1)) != 0)
            throw new ArgumentOutOfRangeException(
                nameof(capacity), "capacity must be a power of two");
        _mask = capacity - 1;
        _buffer = new Cell[capacity];
        // 初始化每个 cell 的 sequence 为其下标：cell[i].Sequence = i。
        // 这使首个生产者 (pos=0) 看到 cell[0].Sequence == 0 == pos，CAS 成功。
        for (var i = 0; i < capacity; i++)
        {
            _buffer[i] = new Cell { Sequence = i, Value = default! };
        }

        _head.Value = 0;
        _tail.Value = 0;
    }

    public int Capacity => _mask + 1;

    /// <summary>当前队列中元素数量（近似值，多生产者并发下不保证精确）。</summary>
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

    /// <summary>
    /// 多生产者安全入队。队列满时返回 false（不阻塞）。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(in T item)
    {
        var pos = Interlocked.Read(ref _head.Value);
        for (;;)
        {
            // pos 已通过 2^N mask 约束在 [0, Capacity)；
            // Unsafe.Add 消除热路径数组边界检查。
            ref var first = ref MemoryMarshal.GetArrayDataReference(_buffer);
            ref var cell = ref Unsafe.Add(
                ref first,
                (nint)(pos & _mask));
            var seq = Volatile.Read(ref cell.Sequence);
            var diff = seq - pos;

            if (diff == 0)
            {
                // 槽位空闲：CAS 推进 head。失败说明其他生产者抢先，重读 head 重试。
                if (Interlocked.CompareExchange(ref _head.Value, pos + 1, pos) == pos)
                {
                    cell.Value = item;
                    Volatile.Write(ref cell.Sequence, pos + 1);
                    return true;
                }

                // CAS 失败：另一生产者拿了这个槽位。重读 head 继续。
                pos = Interlocked.Read(ref _head.Value);
            }
            else if (diff < 0)
            {
                // seq < pos 说明队列已满（消费者尚未推进 _tail 到 pos - capacity）。
                return false;
            }
            else
            {
                // diff > 0：槽位已被另一生产者写入但尚未消费。重读 head 寻找下一槽位。
                pos = Interlocked.Read(ref _head.Value);
            }
        }
    }

    /// <summary>
    /// 单消费者或多消费者出队。队列为空或竞争失败时返回 false。
    /// <para>
    /// 通过 CAS 推进 <c>_tail</c> 保证多消费者安全：竞争失败的调用方应重试（spin/yield）。
    /// 单消费者场景（如 Shard Consumer Loop）CAS 必然成功，无额外开销。
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T item)
    {
        var tail = Volatile.Read(ref _tail.Value);
        // tail 已通过 mask 约束，局部使用 Unsafe.Add 不暴露任意指针。
        ref var first = ref MemoryMarshal.GetArrayDataReference(_buffer);
        ref var cell = ref Unsafe.Add(
            ref first,
            (nint)(tail & _mask));
        var seq = Volatile.Read(ref cell.Sequence);
        var diff = seq - (tail + 1);

        if (diff == 0)
        {
            // 槽位可读：CAS 推进 tail。MPSC 场景下单消费者必然成功；
            // MPMC 场景下（如 DomainWorkLane）竞争失败者返回 false 由调用方重试。
            if (Interlocked.CompareExchange(ref _tail.Value, tail + 1, tail) != tail)
            {
                item = default!;
                return false;
            }

            // CAS 成功：本消费者独占该槽位，安全读值并发布新 sequence。
            item = cell.Value!;
            // 在发布槽位可复用前清除引用，避免消息/Key 被 Ring 保留一整圈。
            cell.Value = default!;
            Volatile.Write(ref cell.Sequence, tail + Capacity);
            return true;
        }

        item = default!;
        return false;
    }

    /// <summary>
    /// 批量消费直到队列空或 <paramref name="max"/> 条。返回实际消费数。
    /// </summary>
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
