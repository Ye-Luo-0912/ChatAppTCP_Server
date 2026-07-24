namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageEditedUpdate
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public required string Content { get; init; }
    public int EditVersion { get; init; }
    public long EditedAtMs { get; init; }
}
