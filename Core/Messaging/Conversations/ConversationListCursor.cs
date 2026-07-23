namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationListCursor
{
    public bool IsPinned { get; init; }
    public long? PinnedAtMs { get; init; }
    public long? LastMessageAtMs { get; init; }
    public required string ConversationId { get; init; }
}
