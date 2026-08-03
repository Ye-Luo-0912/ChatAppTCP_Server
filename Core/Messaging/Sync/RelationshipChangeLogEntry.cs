namespace ChatApp.TcpGateway.Core.Messaging.Sync;

/// <summary>
/// 关系变更日志条目（wire，S2C）。
/// <para>
/// 关系列表幂等增量同步的原子单元。每次关系变更（Send / Accept / Decline / Remove /
/// Block / Unblock）都在业务事务内写入一条日志，携带全局单调递增的 <see cref="ChangeSequence"/>。
/// <see cref="Operation"/> = <see cref="RelationshipChangeOperation.Upsert"/> 时客户端按
/// <see cref="ResourceId"/> upsert 本地条目（payload 反映最新状态）；
/// <see cref="RelationshipChangeOperation.Delete"/> 时客户端按 <see cref="ResourceId"/> 移除
/// （tombstone）。
/// </para>
/// </summary>
public sealed class RelationshipChangeLogEntry
{
    /// <summary>全局单调递增的变更序号（跨用户 / 列表类型）。</summary>
    public required long ChangeSequence { get; init; }

    /// <summary>变更操作（Upsert / Delete）。</summary>
    public required RelationshipChangeOperation Operation { get; init; }

    /// <summary>资源 Id（好友请求 Id / 友谊 Id / 黑名单记录 Id）。</summary>
    public required string ResourceId { get; init; }

    /// <summary>对方用户 Id（列表中的 peer）。</summary>
    public required long UserId { get; init; }

    /// <summary>最新状态（Pending / Accepted / Blocked 等；仅 Upsert 有值）。</summary>
    public string? Status { get; init; }

    /// <summary>好友请求附言（仅 FriendRequests 列表）。</summary>
    public string? Message { get; init; }

    /// <summary>资源建立时间（Unix ms）。</summary>
    public long CreatedAtMs { get; init; }

    /// <summary>变更发生时间（Unix ms）。</summary>
    public long OccurredAtMs { get; init; }

    /// <summary>幂等请求 Id（用于去重 / 审计）。</summary>
    public string? RequestId { get; init; }
}