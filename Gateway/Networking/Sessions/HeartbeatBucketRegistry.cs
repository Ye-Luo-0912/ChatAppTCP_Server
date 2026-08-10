using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 心跳分桶注册表：替代每 tick 全量 <c>_sessions.ToArray()</c> 扫描。
/// <para>
/// 维护两类桶：
/// <list type="bullet">
/// <item><see cref="_connectionBuckets"/>：按 <c>connectionId % bucketCount</c> 分桶，
///   存放每连接的租约刷新任务。桶内 <see cref="ConcurrentDictionary{TKey,TValue}"/> 支持并发注册/注销。</item>
/// <item><see cref="_userBuckets"/>：按 <c>userId % bucketCount</c> 分桶，存放每用户的 presence 刷新引用计数。
///   同一用户的多连接无论落在哪个 connectionId 桶，都只在该用户的 user 桶内计一次 presence 刷新。</item>
/// </list>
/// </para>
/// <para>
/// 注册/注销时机：
/// <list type="bullet">
/// <item>连接建立 → <see cref="RegisterConnection"/>（按 connectionId 入连接桶）；</item>
/// <item>认证成功 → <see cref="RegisterUser"/>（按 userId 入用户桶，引用计数 +1）；</item>
/// <item>连接断开 → <see cref="Unregister"/>（同时移除连接桶与用户桶，用户桶引用计数 -1，归零移除）。</item>
/// </list>
/// 用户桶的引用计数保证：同一用户的多连接断开一个不会移除其他连接的 presence 刷新义务。
/// </para>
/// <para>
/// 每 tick 调用方只需枚举一个连接桶 + 一个用户桶，复杂度从 O(N) 降至 O(N/bucketCount)。
/// 不再每 tick 创建 <c>HashSet&lt;long&gt;</c> / <c>List&lt;Task&gt;</c> / 闭包。
/// </para>
/// </summary>
internal sealed class HeartbeatBucketRegistry
{
    private readonly int _bucketCount;
    private readonly ConcurrentDictionary<uint, TcpClientSession>[] _connectionBuckets;
    // 用户桶：userId → 当前该用户在所有 connectionId 桶中的活跃连接数。
    // 引用计数 > 0 时该用户需在本 user 桶的 tick 内刷新一次 presence；归零时移除。
    private readonly ConcurrentDictionary<long, int>[] _userBuckets;

    public HeartbeatBucketRegistry(int bucketCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(bucketCount, 0);
        _bucketCount = bucketCount;
        _connectionBuckets = new ConcurrentDictionary<uint, TcpClientSession>[bucketCount];
        _userBuckets = new ConcurrentDictionary<long, int>[bucketCount];
        for (var i = 0; i < bucketCount; i++)
        {
            _connectionBuckets[i] = new ConcurrentDictionary<uint, TcpClientSession>();
            _userBuckets[i] = new ConcurrentDictionary<long, int>();
        }
    }

    /// <summary>连接建立时调用：按 connectionId 入连接桶。</summary>
    public void RegisterConnection(TcpClientSession session)
    {
        var bucket = (int)(session.ConnectionId % (uint)_bucketCount);
        _connectionBuckets[bucket][session.ConnectionId] = session;
    }

    /// <summary>
    /// 认证成功时调用：按 userId 入用户桶，引用计数 +1。
    /// 同一用户多连接会累加计数；任一连接断开时计数 -1，归零移除。
    /// </summary>
    public void RegisterUser(long userId)
    {
        if (userId <= 0)
            return;
        var bucket = (int)((ulong)userId % (uint)_bucketCount);
        _userBuckets[bucket].AddOrUpdate(userId, addValue: 1, updateValueFactory: (_, c) => c + 1);
    }

    /// <summary>
    /// 连接断开时调用：移除连接桶条目；若该会话已认证则递减用户桶引用计数，归零移除。
    /// </summary>
    public void Unregister(TcpClientSession session)
    {
        var connBucket = (int)(session.ConnectionId % (uint)_bucketCount);
        // connectionId 可复用。旧 session 的延迟 finally 只能移除自身引用，
        // 不能按裸 ID 删除已覆盖到同一 bucket 的后继 session。
        ((ICollection<KeyValuePair<uint, TcpClientSession>>)
                _connectionBuckets[connBucket])
            .Remove(new KeyValuePair<uint, TcpClientSession>(
                session.ConnectionId,
                session));

        if (session.UserId > 0)
        {
            var userBucket = (int)((ulong)session.UserId % (uint)_bucketCount);
            var bucket = _userBuckets[userBucket];
            // 原子递减并条件删除，防止引用计数删除竞态：
            // 线程 A 递减到 0 → 线程 B 把 0 递增到 1 → 线程 A 执行 TryRemove
            // 会错误移除仍有在线连接的用户。使用 ICollection.Remove(KeyValuePair)
            // 仅当 value 匹配时才删除（原子 CAS 语义）。
            var newCount = bucket.AddOrUpdate(
                session.UserId,
                addValue: 0,
                updateValueFactory: (_, c) => c > 0 ? c - 1 : 0);
            if (newCount <= 0)
            {
                // 条件删除：仅当 value 仍为 0 时才移除。
                // 若在递减与删除之间有新连接 RegisterUser 把 0→1，则不删除。
                ((ICollection<KeyValuePair<long, int>>)bucket)
                    .Remove(new KeyValuePair<long, int>(session.UserId, 0));
            }
        }
    }

    /// <summary>获取指定连接桶的所有会话快照（调用方按 tick 轮换 bucketIndex）。</summary>
    public ICollection<TcpClientSession> GetConnectionBucket(int bucketIndex)
    {
        var bucket = _connectionBuckets[bucketIndex];
        return bucket.Values;
    }

    /// <summary>获取指定用户桶的所有 userId 快照（调用方按 tick 轮换 bucketIndex）。</summary>
    public ICollection<long> GetUserBucket(int bucketIndex)
    {
        var bucket = _userBuckets[bucketIndex];
        return bucket.Keys;
    }

    /// <summary>当前已注册连接总数（仅供指标/诊断，遍历所有桶）。</summary>
    public int TotalConnections
    {
        get
        {
            var sum = 0;
            for (var i = 0; i < _bucketCount; i++)
                sum += _connectionBuckets[i].Count;
            return sum;
        }
    }
}
