namespace ChatApp.TcpGateway.Core.Messaging.History;

public sealed class MessageHistoryItem
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public string? ConversationId { get; init; }
    public required string Content { get; init; }
    public long ReceivedAtMs { get; init; }
    public long? DeliveredAtMs { get; init; }
    public long? ReadAtMs { get; init; }
    public long? RecalledAtMs { get; init; }

    public IReadOnlyList<AttachmentRef>? Attachments { get; init; }

    public string? ReplyToMessageId { get; init; }
    public long? ReplyToSenderUserId { get; init; }
    public string? ReplyToPreview { get; init; }

    public string? ForwardedFromMessageId { get; init; }
    public long? ForwardedFromSenderUserId { get; init; }
    public string? ForwardedFromPreview { get; init; }
}
