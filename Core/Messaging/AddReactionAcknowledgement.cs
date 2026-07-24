namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class AddReactionAcknowledgement
{
    public required string RequestId { get; init; }
    public string? MessageId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public string? Emoji { get; init; }
    public long? OccurredAtMs { get; init; }
    public int? EmojiCount { get; init; }
}
