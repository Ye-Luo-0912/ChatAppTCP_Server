namespace ChatApp.TcpGateway.Core.Messaging.Relationships;

/// <summary>
/// 关系列表变更下行通知（PacketCommand.RelationshipListChanged = 153）。
/// 服务端在好友请求、好友关系、拉黑列表发生变化时下发，提示客户端刷新对应列表。
/// </summary>
public sealed class RelationshipListChangedUpdate
{
    /// <summary>
    /// 资源类型：
    /// "friend-request" = 好友请求列表；
    /// "friendship" = 好友列表；
    /// "blocked-user" = 拉黑列表。
    /// </summary>
    public string? Resource { get; set; }

    /// <summary>
    /// 动作语义：
    /// friend-request: "Pending"/"Accepted"/"Declined" 等请求状态；
    /// friendship: "changed"/"deleted"；
    /// blocked-user: "blocked"/"unblocked"。
    /// </summary>
    public string? Action { get; set; }

    /// <summary>资源 Id（请求 Id / 友谊 Id / 拉黑记录 Id）。客户端可据此去重。</summary>
    public string? ResourceId { get; set; }

    /// <summary>触发变更的对端用户 Id。</summary>
    public long ActorUserId { get; set; }

    /// <summary>可选附言（好友请求消息）。</summary>
    public string? Message { get; set; }

    /// <summary>事件发生时间（UTC 毫秒）。</summary>
    public long OccurredAtMs { get; set; }
}
