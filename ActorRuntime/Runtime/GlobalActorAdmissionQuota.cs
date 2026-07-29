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

    /// <summary>
    /// 非消耗式检查：是否还有配额可用。用于 TryTellDurable 的生产侧保守检查。
    /// 注意：返回 true 不保证后续 TryAcquire 成功（竞态窗口内其他线程可能先消耗）。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool CanAcquire() => Volatile.Read(ref _count) < _max;

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
