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
/// <para>
/// 硬上限：<see cref="MaxEphemeralKeys"/> 限制同时持有的 distinct key 数量，防止慢客户端累积大量
/// 不同 Presence/Typing key 导致开放寻址数组无限扩容。达到上限后新 key 被拒绝（<see cref="TryStore"/>
/// 返回 <paramref name="rejected"/>=true），由调用者 dispose 帧并释放预算。
/// </para>
/// </summary>
internal sealed class EphemeralMailbox
{
    /// <summary>
    /// 同时持有的 distinct ephemeral key 硬上限。
    /// 初始 8 槽翻倍扩容序列 8→16→32→64，到达 64 后不再扩容而是拒绝新 key。
    /// 64 远超常见 2～8 个 distinct key，不影响正常使用；仅在异常累积场景（慢客户端 + 大量不同 key）下触发拒绝。
    /// </summary>
    public const int MaxEphemeralKeys = 64;

    private readonly object _lock = new();
    // 开放寻址槽：(Key, Entry) 对。Key.Kind == 0 标记空槽（KindTyping=1, KindPresence=2 均非 0）。
    // 初始 8 槽覆盖常见 2～8 个 distinct ephemeral key；满时翻倍扩容，但不超过 MaxEphemeralKeys。
    private (EphemeralKey Key, EphemeralEntry Entry)[] _slots = new (EphemeralKey, EphemeralEntry)[8];
    private int _count;
    // lock 内递增并写入 EphemeralEntry，作为条件移除的 ABA 防护。
    // 同一 SharedOutboundFrame 可被重复存入同一 key，不能只比较 Frame 引用。
    private long _nextEntryVersion;

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
    /// <para>
    /// 硬上限：当 distinct key 数量已达 <see cref="MaxEphemeralKeys"/> 时，新 key（未命中已存在 key）
    /// 不再存储也不再扩容，<paramref name="rejected"/> 置 true，调用者负责 dispose 新帧与释放预算。
    /// 已存在 key 的覆盖不受限制（不增加 _count）。
    /// </para>
    /// </summary>
    /// <param name="rejected">true 表示因达到 <see cref="MaxEphemeralKeys"/> 上限新 key 被拒绝存储；
    /// 此时返回值为 null（无旧条目），新条目未被存储，调用者必须 dispose 新帧并释放预算。</param>
    /// <param name="storedEntry">实际写入槽位、带唯一版本的条目；拒绝时为 default。
    /// 关闭竞态回滚必须把它作为 expected entry 传给 <see cref="TryRemove"/>。</param>
    /// <returns>被覆盖的旧条目（如有）；调用者负责 dispose 旧帧与释放预算。null 表示新插入或被拒绝（用 <paramref name="rejected"/> 区分）。</returns>
    public EphemeralEntry? TryStore(
        EphemeralKey key,
        EphemeralEntry newEntry,
        out bool rejected,
        out EphemeralEntry storedEntry)
    {
        rejected = false;
        storedEntry = default;
        lock (_lock)
        {
            var entryVersion = ++_nextEntryVersion;
            // 0 保留给 default；long 回绕在现实生命周期内不可达，但仍保持哨兵不变量。
            if (entryVersion == 0)
                entryVersion = ++_nextEntryVersion;
            storedEntry = new EphemeralEntry(
                newEntry.Frame,
                newEntry.ByteCount,
                entryVersion);

            // 先扫描已存在 key：覆盖旧条目（不增加 _count）。
            for (int i = 0; i < _slots.Length; i++)
            {
                ref var slot = ref _slots[i];
                if (slot.Key == key)
                {
                    var old = slot.Entry;
                    slot.Entry = storedEntry;
                    return old;
                }
            }

            // 新 key：检查是否已达 MaxEphemeralKeys 硬上限。
            // 达上限时拒绝存储（不扩容、不覆盖），调用者负责 dispose 新帧与释放预算。
            // 此检查在插入前进行，保证无论数组容量如何 _count 永不超过 MaxEphemeralKeys。
            if (_count >= MaxEphemeralKeys)
            {
                rejected = true;
                storedEntry = default;
                return null;
            }

            // 找第一个空槽（Kind == 0）插入。
            for (int i = 0; i < _slots.Length; i++)
            {
                ref var slot = ref _slots[i];
                if (slot.Key.Kind == 0)
                {
                    slot.Key = key;
                    slot.Entry = storedEntry;
                    _count++;
                    return null;
                }
            }

            // 所有槽已被不同 key 占用：扩容后插入（扩容后仍受 MaxEphemeralKeys 检查保护）。
            var oldLen = _slots.Length;
            Array.Resize(ref _slots, oldLen * 2);
            _slots[oldLen] = (key, storedEntry);
            _count++;
            return null;
        }
    }

    /// <summary>
    /// 仅当 <paramref name="key"/> 当前仍指向 <paramref name="expectedEntry"/> 时移除。
    /// 用于连接关闭窗口中撤销刚存入、但未能写入 flush sentinel 的条目。
    /// <para>
    /// 不能调用 <see cref="Drain"/> 做失败回滚：发送线程可能正在遍历其复用 List，
    /// 并发 Drain 会 Clear 同一实例，导致丢帧或重复释放。这里在 mailbox lock 内只摘除
    /// 当前生产者拥有的精确条目；若它已被覆盖或被 Drain 取得，所有权留给胜者。
    /// </para>
    /// </summary>
    public bool TryRemove(
        EphemeralKey key,
        EphemeralEntry expectedEntry,
        out EphemeralEntry removedEntry)
    {
        lock (_lock)
        {
            for (var index = 0; index < _slots.Length; index++)
            {
                ref var slot = ref _slots[index];
                if (slot.Key == key && slot.Entry == expectedEntry)
                {
                    removedEntry = slot.Entry;
                    slot = default;
                    _count--;
                    return true;
                }
            }
        }

        removedEntry = default;
        return false;
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
