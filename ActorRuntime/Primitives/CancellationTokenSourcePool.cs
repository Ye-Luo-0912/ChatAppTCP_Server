using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// <see cref="CancellationTokenSource"/> 池：复用 CTS 实例，消除 Per-op CTS + Timer 分配。
/// <para>
/// .NET 8+ 的 <c>CancellationTokenSource.TryReset()</c> 允许复用已取消或已到期的 CTS，
/// 避免每次操作都 <c>new CancellationTokenSource()</c> + <c>CancelAfter</c>
/// （后者内部注册一个 Timer）。
/// </para>
/// <para>
/// 设计取舍：本池仅管理"独立超时 CTS"，不通过 <c>CreateLinkedTokenSource</c> 联动 stop token。
/// 原因：<c>CreateLinkedTokenSource</c> 每次都分配新 CTS + 内部委托；而 stop 信号可通过
/// worker 循环退出 + 显式 <c>cts.Cancel()</c> 传递，避免 linked CTS 的双层分配。
/// 调用方在 worker 循环中 catch <c>OperationCanceledException</c> 时检查 stop token 即可区分
/// 超时与停机。
/// </para>
/// <para>
/// 典型用法（领域 Executor Worker）：
/// <code>
/// var cts = _ctsPool.Rent();
/// try { cts.CancelAfter(_timeout); await work.ExecuteAsync(cts.Token); }
/// finally { _ctsPool.Return(cts); }
/// </code>
/// </para>
/// <para>
/// 线程安全：内部用 <see cref="ConcurrentStack{T}"/>（无锁 LIFO）。栈为空时回退到
/// <c>new</c>（不阻塞调用方）。池上限避免无限增长。
/// </para>
/// </summary>
internal sealed class CancellationTokenSourcePool
{
    private readonly ConcurrentStack<CancellationTokenSource> _stack = new();
    private readonly int _maxCapacity;
    // 独立原子计数：避免热路径查询 ConcurrentStack.Count 的内部锁竞争。
    private int _count;

    /// <param name="maxCapacity">池上限。超过此数量的归还实例直接丢弃。默认 256。</param>
    public CancellationTokenSourcePool(int maxCapacity = 256)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxCapacity, 0);
        _maxCapacity = maxCapacity;
    }

    /// <summary>
    /// 租用一个已 Reset 的 CTS。调用方随后通过 <c>CancelAfter</c> 设置超时。
    /// <para>
    /// 不做 linked token 联动——避免每次分配委托。
    /// Stop 信号由 worker 循环通过显式 <c>cts.Cancel()</c> 或停止读取队列传递。
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CancellationTokenSource Rent()
    {
        while (_stack.TryPop(out var cts))
        {
            Interlocked.Decrement(ref _count);
            if (cts.TryReset())
                return cts;
            // TryReset 失败（CTS 不可重置，如已 Dispose）：丢弃
            cts.Dispose();
        }
        return new CancellationTokenSource();
    }

    /// <summary>
    /// 归还 CTS 到池。池满时直接 Dispose。
    /// <para>
    /// 已取消/已到期的实例仍可复用（下次 Rent 会 TryReset）。
    /// 不要归还已 Dispose 的 CTS。
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Return(CancellationTokenSource cts)
    {
        ArgumentNullException.ThrowIfNull(cts);
        if (Volatile.Read(ref _count) >= _maxCapacity)
        {
            cts.Dispose();
            return;
        }
        _stack.Push(cts);
        Interlocked.Increment(ref _count);
    }
}
