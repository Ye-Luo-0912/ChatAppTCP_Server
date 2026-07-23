namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageRecalledUpdate
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public long RecalledAtMs { get; init; }
}
