using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// Actor Key → ActorCell 映射表。单线程（Shard Consumer）访问，使用普通 <see cref="Dictionary{TKey,TValue}"/>
/// 配合 <see cref="CollectionsMarshal.GetValueRefOrAddDefault"/> 获取 ref 避免 double-lookup。
/// <para>
/// v1 实现：每 Actor 一个 class 实例（ActorCell）。后续可替换为 slab（ActorCell[] + free list）
/// 提升缓存局部性，但接口不变。
/// </para>
/// </summary>
internal sealed class ActorCellTable<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    private readonly Dictionary<TKey, ActorCell<TKey, TState, TMessage>> _cells;

    public ActorCellTable(int initialCapacity)
    {
        _cells = new Dictionary<TKey, ActorCell<TKey, TState, TMessage>>(initialCapacity);
    }

    public int Count => _cells.Count;

    /// <summary>
    /// 获取或创建 ActorCell。仅由 Shard Consumer 单线程调用——无需原子操作。
    /// 返回 ref 允许调用方原地修改 cell 状态（包括 State 字段）。
    /// </summary>
    public ref ActorCell<TKey, TState, TMessage>? GetOrAddRef(in TKey key)
    {
        // CollectionsMarshal 提供 ref 避免 double-lookup（TryGetValue + 一次 indexer 写）。
        // ref 字典在 .NET 10 支持并发读但单写——满足 Shard Consumer 单写场景。
        ref var cell = ref CollectionsMarshal.GetValueRefOrAddDefault(_cells, key, out var exists);
        if (!exists)
        {
            cell = null; // 占位，调用方将通过 returned ref 设置
        }
        return ref cell;
    }

    public bool TryGetValue(in TKey key, out ActorCell<TKey, TState, TMessage> cell)
        => _cells.TryGetValue(key, out cell!);

    /// <summary>枚举所有 cell 用于 Snapshot 或 DeactivateAll。单线程访问。</summary>
    public Dictionary<TKey, ActorCell<TKey, TState, TMessage>>.ValueCollection Values => _cells.Values;

    /// <summary>枚举所有键值对用于需要 key 的清理操作（如 SweepIdleActors）。单线程访问。</summary>
    public Dictionary<TKey, ActorCell<TKey, TState, TMessage>>.Enumerator GetEnumerator()
        => _cells.GetEnumerator();

    public bool Remove(in TKey key) => _cells.Remove(key);

    public void Clear() => _cells.Clear();
}
