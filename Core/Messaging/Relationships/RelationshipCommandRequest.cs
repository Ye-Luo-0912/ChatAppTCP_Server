namespace ChatApp.TcpGateway.Core.Messaging.Relationships;

/// <summary>
/// 主线四：关系操作命令（C2S）。
/// <para>
/// 统一命令格式（类似 GroupConversationCommand）：通过 <see cref="Operation"/> 区分操作类型。
/// Realtime 侧负责权限校验与业务规则（不可自加好友、不可重复拉黑等）。
/// </para>
/// </summary>
public sealed class RelationshipCommandRequest
{
    public required string RequestId { get; init; }

    /// <summary>操作类型。</summary>
    public required RelationshipOperation Operation { get; init; }

    /// <summary>目标用户 Id（对方）。</summary>
    public long? TargetUserId { get; init; }

    /// <summary>好友请求附言（仅 SendFriendRequest 时使用）。</summary>
    public string? Message { get; init; }

    /// <summary>好友请求 Id（仅 RespondFriendRequest 时使用：接受或拒绝指定请求）。</summary>
    public string? RequestIdToRespond { get; init; }
}

/// <summary>
/// 关系操作类型。
/// </summary>
public enum RelationshipOperation : byte
{
    /// <summary>发送好友请求。</summary>
    SendFriendRequest = 1,

    /// <summary>接受好友请求。</summary>
    AcceptFriendRequest = 2,

    /// <summary>拒绝好友请求。</summary>
    DeclineFriendRequest = 3,

    /// <summary>删除好友。</summary>
    RemoveFriend = 4,

    /// <summary>拉黑用户。</summary>
    BlockUser = 5,

    /// <summary>取消拉黑。</summary>
    UnblockUser = 6
}
