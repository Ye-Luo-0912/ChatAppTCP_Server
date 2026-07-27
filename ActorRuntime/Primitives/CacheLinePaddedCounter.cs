using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// 缓存行对齐的 64-bit 原子计数器。避免相邻字段间的 false sharing。
/// <para>
/// 在多 Shard 共享的统计结构中，每个计数器独占一条缓存线（64B），
/// 防止一个 Shard 写入使其邻近 Shard 的缓存行失效。
/// </para>
/// </summary>
public sealed class CacheLinePaddedCounter
{
    private PaddedLong _value;

    public long Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Interlocked.Read(ref _value.Value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Increment() => Interlocked.Increment(ref _value.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Decrement() => Interlocked.Decrement(ref _value.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Add(long delta) => Interlocked.Add(ref _value.Value, delta);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Set(long value) => Interlocked.Exchange(ref _value.Value, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Read() => Interlocked.Read(ref _value.Value);

    // Size=64 强制结构体占满一条缓存行（8B Value + 56B 自动填充）。
    [StructLayout(LayoutKind.Sequential, Size = 64)]
    private struct PaddedLong
    {
#pragma warning disable CS0649
        public long Value;
#pragma warning restore CS0649
    }
}
