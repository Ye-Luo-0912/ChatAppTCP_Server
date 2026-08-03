namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed class SyncBootstrapRequest
{
    public string? RequestId { get; init; }
    public int ListLimit { get; init; } = 50;
    public int HistoryLimitPerConversation { get; init; } = 20;
    public int MaxConversationsWithHistory { get; init; } = 10;
    public IReadOnlyList<ConversationSyncWatermark>? Watermarks { get; init; }

    /// <summary>
    /// 关系列表增量同步水位（C2S）。null 或空表示不请求关系同步。
    /// </summary>
    public IReadOnlyList<RelationshipSyncWatermark>? RelationshipWatermarks { get; init; }

    /// <summary>关系列表分页大小。null 或 0 表示默认值 50。</summary>
    public int? RelationshipListLimit { get; init; }
}
