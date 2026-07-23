namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed class SyncBootstrapRequest
{
    public string? RequestId { get; init; }
    public int ListLimit { get; init; } = 50;
    public int HistoryLimitPerConversation { get; init; } = 20;
    public int MaxConversationsWithHistory { get; init; } = 10;
    public IReadOnlyList<ConversationSyncWatermark>? Watermarks { get; init; }
}
