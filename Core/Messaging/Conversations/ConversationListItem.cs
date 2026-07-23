namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationListItem
{
    public required string ConversationId { get; init; }
    public ConversationType Type { get; init; } = ConversationType.Direct;
    public long? PeerUserId { get; init; }
    public string? LastMessageId { get; init; }
    public string? LastMessagePreview { get; init; }
    public long? LastMessageAtMs { get; init; }
    public long? LastSenderUserId { get; init; }
    public int UnreadCount { get; init; }
    public string? LastReadMessageId { get; init; }
    public long? LastReadAtMs { get; init; }
    public bool IsPinned { get; init; }
    public long? PinnedAtMs { get; init; }
    public bool IsMuted { get; init; }
    public long? MutedUntilMs { get; init; }
}
