namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationMarkReadRequest
{
    public string? RequestId { get; init; }
    public required string ConversationId { get; init; }
    public long? ReadAtMs { get; init; }
    public string? ReadMessageId { get; init; }
}
