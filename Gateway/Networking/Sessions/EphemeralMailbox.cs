namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Ephemeral latest-state mailbox：按 <see cref="EphemeralKey"/> 分槽，同 key 覆盖旧帧保留最新状态。
/// <para>
/// Typing key = (KindTyping, hash(conversationId))，Presence key = (KindPresence, userId)。
/// 与出站 FIFO 独立：调用者在 FIFO 写入 flush sentinel 唤醒发送循环排空本 mailbox。
/// </para>
/// <para>
/// 实现为<b>开放寻址数组 + lock</b>，替代 <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/>：
/// <list type="bullet">
/// <item>常见 2～8 个 distinct key，固定 8 槽数组足够，无需 ConcurrentDictionary 的 node 分配；</item>
/// <item>线性探测（open addressing）查找，无哈希桶链表；</item>
/// <item>Drain 复用内部 <see cref="List{T}"/>，避免每次排空分配新 List；</item>
/// <item>lock 持有时间极短（扫描 ≤8 槽），且 ephemeral 为低优先级路径，锁竞争可忽略。</item>
/// </list>
/// </para>
/// <para>
/// 线程安全模型：TryStore 与 Drain 可并发（ephemeral pipeline 多线程 store，send loop 单线程 drain）。
/// lock 保证原子性：TryStore 的查找+覆盖/插入不会与 Drain 的收集+清除交错。
/// </para>
/// <para>
/// 预算管理、帧 retain、sentinel 协调仍由 <see cref="TcpClientSession"/> 持有。
/// </para>
/// </summary>
internal sealed class EphemeralMailbox
{
    private readonly object _lock = new();
    // 开放寻址槽：(Key, Entry) 对。Key.Kind == 0 标记空槽（KindTyping=1, KindPresence=2 均非 0）。
    // 初始 8 槽覆盖常见 2～8 个 distinct ephemeral key；满时翻倍扩容。
    private (EphemeralKey Key, EphemeralEntry Entry)[] _slots = new (EphemeralKey, EphemeralEntry)[8];
    private int _count;

    // 可复用的 drain 缓冲：避免每次排空分配新 List。
    // 安全性：Drain 由 send loop 单线程调用（同一 session 不会并发 drain），
    // 下次 Drain 前上次返回的 List 已被调用方处理完毕（所有帧已 dispose/发送）。
    private readonly List<EphemeralEntry> _drainList = new();

    public bool IsEmpty => Volatile.Read(ref _count) == 0;

    /// <summary>
    /// 原子存储：同 key 覆盖旧帧（返回旧条目供调用者 dispose+release），不同 key 独立共存。
    /// <para>
    /// 线性探测开放寻址：先扫描已存在 key 做覆盖，再找空槽插入，满则扩容。
    /// </para>
    /// </summary>
    /// <returns>被覆盖的旧条目（如有）；调用者负责 dispose 旧帧与释放预算。null 表示新插入。</returns>
    public EphemeralEntry? TryStore(EphemeralKey key, EphemeralEntry newEntry)
    {
        lock (_lock)
        {
            // 先扫描已存在 key：覆盖旧条目（不增加 _count）。
            for (int i = 0; i < _slots.Length; i++)
            {
                ref var slot = ref _slots[i];
                if (slot.Key == key)
                {
                    var old = slot.Entry;
                    slot.Entry = newEntry;
                    return old;
                }
            }

            // 新 key：找第一个空槽（Kind == 0）插入。
            for (int i = 0; i < _slots.Length; i++)
            {
                ref var slot = ref _slots[i];
                if (slot.Key.Kind == 0)
                {
                    slot.Key = key;
                    slot.Entry = newEntry;
                    _count++;
                    return null;
                }
            }

            // 所有槽已被不同 key 占用：扩容后插入。
            var oldLen = _slots.Length;
            Array.Resize(ref _slots, oldLen * 2);
            _slots[oldLen] = (key, newEntry);
            _count++;
            return null;
        }
    }

    /// <summary>
    /// 原子收集并移除所有条目。调用者负责发送、dispose 帧与释放预算。
    /// <para>
    /// 复用内部 <see cref="_drainList"/>，避免每次排空分配新 List。
    /// 返回的 List 引用在下次 Drain 前有效（send loop 单线程保证不会并发 drain）。
    /// </para>
    /// </summary>
    /// <returns>待处理条目列表（复用实例，非 null 时 Count > 0）；mailbox 为空时返回 null。</returns>
    public List<EphemeralEntry>? Drain()
    {
        lock (_lock)
        {
            if (_count == 0)
                return null;

            _drainList.Clear();
            for (int i = 0; i < _slots.Length; i++)
            {
                ref var slot = ref _slots[i];
                if (slot.Key.Kind != 0)
                {
                    _drainList.Add(slot.Entry);
                    slot = default;
                }
            }
            _count = 0;
            return _drainList;
        }
    }
}
