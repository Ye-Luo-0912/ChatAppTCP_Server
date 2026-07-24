namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

/// <summary>
/// 下行：某成员将会话已读水位推进到指定消息（DM 与群聊共用）。
/// </summary>
public sealed class ConversationReadUpdate
{
    public required string ConversationId { get; init; }
    public required long ReaderUserId { get; init; }
    public required string LastReadMessageId { get; init; }
    public long LastReadAtMs { get; init; }
}
