namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class RemoveReactionRequest
{
    public string? RequestId { get; init; }
    public required string MessageId { get; init; }
    public required string Emoji { get; init; }
}
