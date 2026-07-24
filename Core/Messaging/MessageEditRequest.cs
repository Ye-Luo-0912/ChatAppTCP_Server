namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageEditRequest
{
    public string? RequestId { get; init; }
    public required string MessageId { get; init; }
    public required string Content { get; init; }
}
