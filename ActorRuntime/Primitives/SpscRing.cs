using System.Runtime.CompilerServices;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// 单生产者单消费者 (SPSC) 环形队列。生产者与消费者必须在同一线程（Shard Loop），
/// 因此无需任何原子操作或内存屏障——普通字段读写即可。
/// <para>
/// 用于 Shard 内的 Ready Queue：Producer 是 Consumer 自己（处理完一个 Actor 后入队 Ready），
/// 或同线程的其他逻辑。跨线程入队请使用 <see cref="BoundedMpscRing{T}"/>。
/// </para>
/// </summary>
internal sealed class SpscRing<T>
{
    private readonly T?[] _slots;
    private readonly int _mask;
    private int _head;
    private int _tail;
    private int _count;

    public SpscRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(capacity, 0);
        if ((capacity & (capacity - 1)) != 0)
            throw new ArgumentOutOfRangeException(
                nameof(capacity), "capacity must be a power of two");
        _mask = capacity - 1;
        _slots = new T[capacity];
    }

    public int Capacity => _mask + 1;
    public int Count => _count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T item)
    {
        if (_count > _mask)
            return false;
        _slots[_head & _mask] = item;
        _head++;
        _count++;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T? item)
    {
        if (_count == 0)
        {
            item = default;
            return false;
        }
        item = _slots[_tail & _mask];
        _slots[_tail & _mask] = default; // 释放引用避免 GC 泄漏
        _tail++;
        _count--;
        return true;
    }

    public void Clear()
    {
        while (TryDequeue(out _)) { }
        _head = 0;
        _tail = 0;
        _count = 0;
    }
}
