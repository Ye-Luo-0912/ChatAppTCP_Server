namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationMarkReadResponse
{
    public required string RequestId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public int UnreadCount { get; init; }
    public string? LastReadMessageId { get; init; }
    public long? LastReadAtMs { get; init; }
    public bool Changed { get; init; }
}
