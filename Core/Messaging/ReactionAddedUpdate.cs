namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class ReactionAddedUpdate
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long ReactorUserId { get; init; }
    public long MessageSenderUserId { get; init; }
    public long MessageReceiverUserId { get; init; }
    public required string Emoji { get; init; }
    public int EmojiCount { get; init; }
    public long OccurredAtMs { get; init; }
}
