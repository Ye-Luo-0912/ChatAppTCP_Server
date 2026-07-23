namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed class ConversationSyncWatermark
{
    public required string ConversationId { get; init; }
    public long AfterReceivedAtMs { get; init; }
    public required string AfterMessageId { get; init; }
}
