namespace ChatApp.TcpGateway.Core.Messaging.History;

public sealed class MessageReactionSummary
{
    public required string Emoji { get; init; }
    public int Count { get; init; }
    public bool ReactedByMe { get; init; }
}
