namespace ChatApp.TcpGateway.Core.Messaging.Relationships;

/// <summary>
/// 主线四：关系条目（好友 / 好友请求 / 黑名单）。
/// </summary>
public sealed class RelationshipItem
{
    /// <summary>对方用户 Id。</summary>
    public required long UserId { get; init; }

    /// <summary>资源 Id（好友请求 Id / 友谊 Id / 黑名单记录 Id）。</summary>
    public required string ResourceId { get; init; }

    /// <summary>状态（Pending / Accepted / Blocked 等）。</summary>
    public string? Status { get; init; }

    /// <summary>好友请求附言（仅 FriendRequests 列表）。</summary>
    public string? Message { get; init; }

    /// <summary>关系建立时间（Unix ms）。</summary>
    public long CreatedAtMs { get; init; }
}
