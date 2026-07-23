namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageRecallRequest
{
    public string? RequestId { get; init; }
    public required string MessageId { get; init; }
}
