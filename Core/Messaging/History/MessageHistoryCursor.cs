namespace ChatApp.TcpGateway.Core.Messaging.History;

public sealed class MessageHistoryCursor
{
    public long ReceivedAtMs { get; init; }
    public required string MessageId { get; init; }
}