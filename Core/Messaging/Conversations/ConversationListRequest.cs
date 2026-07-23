namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationListRequest
{
    public string? RequestId { get; init; }
    public bool? BeforeIsPinned { get; init; }
    public long? BeforePinnedAtMs { get; init; }
    public long? BeforeLastMessageAtMs { get; init; }
    public string? BeforeConversationId { get; init; }
    public int Limit { get; init; } = 50;
}
