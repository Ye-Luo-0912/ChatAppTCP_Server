using System.Runtime.CompilerServices;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 全局活跃 Actor 配额。所有 Shard 共享一个实例：
/// 激活时 <see cref="TryAcquire"/>，Deactivate 时 <see cref="Release"/>。
/// 与每 Shard 上限（ActorCellTable.Count）共同构成双层准入。
/// </summary>
internal sealed class GlobalActorAdmissionQuota
{
    private readonly int _max;
    private int _count;

    public GlobalActorAdmissionQuota(int max)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(max, 0);
        _max = max;
    }

    public int Count => Volatile.Read(ref _count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAcquire()
    {
        var current = Volatile.Read(ref _count);
        while (current < _max)
        {
            var observed = Interlocked.CompareExchange(
                ref _count,
                current + 1,
                current);
            if (observed == current)
                return true;

            current = observed;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Release()
    {
        var remaining = Interlocked.Decrement(ref _count);
        if (remaining < 0)
        {
            // Release/TryAcquire 配对由 Shard 单写者保证；为负表示内部不变量被破坏。
            Interlocked.Increment(ref _count);
            System.Diagnostics.Debug.Fail("GlobalActorAdmissionQuota released more than acquired.");
        }
    }
}
