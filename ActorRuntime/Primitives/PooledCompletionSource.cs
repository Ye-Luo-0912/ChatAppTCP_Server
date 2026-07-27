using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using ChatApp.ActorRuntime.Abstractions;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// 基于 <see cref="ManualResetValueTaskSourceCore{TResult}"/> 的池化 Completion 源。
/// <para>
/// 用于 Query 等"调用方需要等待结果"的场景：Behavior 在消息中携带 <see cref="Handle"/>，
/// I/O 完成后调用 <see cref="TrySetResult"/> 唤醒调用方的 <c>ValueTask&lt;TResult&gt;</c>。
/// </para>
/// <para>
/// v1 实现：每次 <see cref="Rent"/> 创建新实例（无对象池）。
/// Generation 用于防止 ABA：实例归还后重新 Rent 时 Generation 自增，
/// 旧的 <see cref="Handle"/> 调用 <see cref="TrySetResult"/> 时会因 Generation 不匹配而失败。
/// </para>
/// <para>
/// v2 优化点：用 <c>ConcurrentQueue&lt;PooledCompletionSource&lt;TResult&gt;&gt;</c>
/// 或 slab + free list 实现真正的有界对象池，消除每次 Rent 的分配。
/// </para>
/// </summary>
public sealed class PooledCompletionSource<TResult> : IValueTaskSource<TResult>
{
    private ManualResetValueTaskSourceCore<TResult> _core;
    private uint _generation;
    private bool _completed;

    public PooledCompletionSource()
    {
        _core.RunContinuationsAsynchronously = false;
    }

    /// <summary>当前句柄。Rent 后有效；TrySetResult/TrySetException 后保持有效但 Generation 已过期。</summary>
    public CompletionHandle Handle { get; private set; }

    /// <summary>
    /// 租用一个新源。v1：分配新实例。v2：从对象池取。
    /// <para>
    /// 调用方应在调用 <c>runtime.TryTell(key, message)</c> 前调用 Rent，
    /// 并在 TryTell 失败时立即调用 <see cref="TrySetException"/> 释放。
    /// </para>
    /// </summary>
#pragma warning disable CA1000
    // CA1000：泛型类型上的静态成员。Rent 是工厂方法的语义需要——
    // 调用方无法在不指定 TResult 的情况下访问此类。抑制此规则。
    public static PooledCompletionSource<TResult> Rent()
#pragma warning restore CA1000
    {
        var src = new PooledCompletionSource<TResult>();
        src._generation++;
        src._completed = false;
        src.Handle = new CompletionHandle
        {
            SlotIndex = 0, // v1: 单实例，无 pool index
            Generation = src._generation,
            ResultTypeId = typeof(TResult).GetHashCode()
        };
        src._core.Reset();
        return src;
    }

    /// <summary>获取 ValueTask。调用一次。完成前不得重复调用。</summary>
    public ValueTask<TResult> AsValueTask() => new(this, _core.Version);

    /// <summary>设置成功结果。Generation 不匹配或已设置过时返回 false（迟到的旧句柄）。</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TrySetResult(in CompletionHandle handle, TResult result)
    {
        if (handle.Generation != _generation || handle.SlotIndex != 0 || _completed)
            return false;
        _completed = true;
        _core.SetResult(result);
        return true;
    }

    /// <summary>设置异常。Generation 不匹配或已设置过时返回 false。</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TrySetException(in CompletionHandle handle, Exception error)
    {
        if (handle.Generation != _generation || handle.SlotIndex != 0 || _completed)
            return false;
        _completed = true;
        _core.SetException(error);
        return true;
    }

    TResult IValueTaskSource<TResult>.GetResult(short token) => _core.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<TResult>.GetStatus(short token) => _core.GetStatus(token);
    void IValueTaskSource<TResult>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
