namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class UnreadCountChanged
{
    public required string ConversationId { get; init; }
    public int UnreadCount { get; init; }
    public string? LastReadMessageId { get; init; }
    public long? LastReadAtMs { get; init; }
}
