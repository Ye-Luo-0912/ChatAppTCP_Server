namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class AddReactionRequest
{
    public string? RequestId { get; init; }
    public required string MessageId { get; init; }
    public required string Emoji { get; init; }
}
