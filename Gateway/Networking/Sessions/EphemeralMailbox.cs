using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Ephemeral latest-state mailbox：按 <see cref="EphemeralKey"/> 分槽，同 key 覆盖旧帧保留最新状态。
/// <para>
/// Typing key = (KindTyping, hash(conversationId))，Presence key = (KindPresence, userId)。
/// 与出站 FIFO 独立：调用者在 FIFO 写入 flush sentinel 唤醒发送循环排空本 mailbox。
/// </para>
/// <para>
/// 从 <see cref="TcpClientSession"/> 抽取以隔离 keyed storage 与 drain 逻辑。
/// 预算管理、帧 retain、sentinel 协调仍由 session 持有。
/// </para>
/// </summary>
internal sealed class EphemeralMailbox
{
    private readonly ConcurrentDictionary<EphemeralKey, EphemeralEntry> _entries = new();

    public bool IsEmpty => _entries.IsEmpty;

    /// <summary>
    /// 原子存储：同 key 覆盖旧帧（返回旧条目供调用者 dispose+release），不同 key 独立共存。
    /// <para>
    /// 使用 TryUpdate/TryAdd CAS 循环而非索引赋值，避免与 <see cref="Drain"/> 的 TryRemove 竞争：
    /// 若 drain 在 TryGetValue 与赋值之间 TryRemove 了条目，非原子赋值会重新插入已移除的 key，
    /// 随后 oldEntry.Frame.Dispose() 会释放 drain 正在发送的帧（use-after-free）。
    /// </para>
    /// </summary>
    /// <returns>被覆盖的旧条目（如有）；调用者负责 dispose 旧帧与释放预算。null 表示新插入。</returns>
    public EphemeralEntry? TryStore(EphemeralKey key, EphemeralEntry newEntry)
    {
        while (true)
        {
            if (_entries.TryGetValue(key, out var existing))
            {
                if (_entries.TryUpdate(key, newEntry, existing))
                    return existing;
                // CAS 失败：条目已被 drain 移除或被其他线程更新，重试。
                continue;
            }

            if (_entries.TryAdd(key, newEntry))
                return null;
            // TryAdd 失败：其他线程先添加了，重试（走 TryUpdate 分支）。
        }
    }

    /// <summary>
    /// 原子收集并移除所有条目。调用者负责发送、dispose 帧与释放预算。
    /// <para>
    /// TryRemove 保证每个条目只被本次 drain 处理一次，与 <see cref="TryStore"/> 的 CAS 配合
    /// 避免 dispose 正在发送的帧。
    /// </para>
    /// </summary>
    /// <returns>待处理条目列表；mailbox 为空时返回 null。</returns>
    public List<EphemeralEntry>? Drain()
    {
        if (_entries.IsEmpty)
            return null;

        List<EphemeralEntry>? result = null;
        foreach (var kvp in _entries)
        {
            if (_entries.TryRemove(kvp.Key, out var entry))
                (result ??= new List<EphemeralEntry>()).Add(entry);
        }
        return result;
    }
}
