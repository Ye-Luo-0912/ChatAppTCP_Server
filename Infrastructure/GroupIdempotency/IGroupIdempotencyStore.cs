using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.TcpGateway.Infrastructure.GroupIdempotency;

/// <summary>
/// 群组命令幂等存储：按 (UserId, Operation, RequestId) 缓存 Realtime 返回的
/// <see cref="GroupConversationResult"/>，避免客户端重试时重复 Redis/NATS 往返。
/// 两个实现：内存 L1（<see cref="ChatApp.TcpGateway.Gateway.Commands.Groups.GroupRequestIdempotencyCache"/>）
/// 与 Redis L2（<see cref="RedisGroupIdempotencyStore"/>）。
/// </summary>
public interface IGroupIdempotencyStore
{
    /// <summary>
    /// 尝试获取缓存的结果。返回 Hit（含缓存结果）、Miss（未命中/已过期）或
    /// Conflict（同一 RequestId 但 PayloadHash 不匹配）。
    /// </summary>
    /// <param name="payloadHash">命令负载指纹（五.1：SHA-256 hex 字符串，跨进程稳定）。
    /// 用于检测同一 (UserId, Operation, RequestId) 但不同负载的冲突。</param>
    ValueTask<GroupIdempotencyLookup> TryGetAsync(
        long userId,
        int operation,
        string requestId,
        string payloadHash,
        CancellationToken cancellationToken);

    /// <summary>
    /// 缓存 Realtime 返回的结果。尽力而为：容量超限或失败时可能静默跳过。
    /// <para>
    /// 五.2：Redis L2 实现为条件写——仅当 key 不存在或已存指纹相同时写入，
    /// 不覆盖不同指纹的既有结果（避免并发 Miss 后最后写入者覆盖前者）。
    /// </para>
    /// </summary>
    ValueTask TryAddAsync(
        long userId,
        int operation,
        string requestId,
        string payloadHash,
        GroupConversationResult result,
        CancellationToken cancellationToken);

    /// <summary>
    /// 移除指定用户的全部缓存条目（如登出时清理）。Redis L2 为尽力而为。
    /// </summary>
    void EvictUser(long userId);
}

/// <summary>
/// 幂等缓存查找结果：区分命中、未命中与冲突。
/// </summary>
public readonly record struct GroupIdempotencyLookup
{
    /// <summary>缓存的结果；未命中或冲突时为 null。</summary>
    public GroupConversationResult? Result { get; }

    /// <summary>是否为冲突（同一 RequestId 但负载指纹不匹配）。</summary>
    public bool IsConflict { get; }

    private GroupIdempotencyLookup(GroupConversationResult? result, bool isConflict)
    {
        Result = result;
        IsConflict = isConflict;
    }

    /// <summary>未命中（缓存中不存在或已过期）。</summary>
    public static GroupIdempotencyLookup Miss => default;

    /// <summary>冲突（同一 RequestId 但负载指纹不匹配）。</summary>
    public static GroupIdempotencyLookup Conflict => new(null, isConflict: true);

    /// <summary>命中缓存。</summary>
    public static GroupIdempotencyLookup Hit(GroupConversationResult result) =>
        new(result, isConflict: false);

    /// <summary>是否命中缓存（有缓存结果且无冲突）。</summary>
    public bool IsHit => !IsConflict && Result is not null;
}