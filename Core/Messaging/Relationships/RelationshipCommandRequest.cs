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
