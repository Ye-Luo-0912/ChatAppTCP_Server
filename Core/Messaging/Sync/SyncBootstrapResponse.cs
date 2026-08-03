using ChatApp.TcpGateway.Core.Messaging.Conversations;

namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed record SyncBootstrapResponse
{
    public required string RequestId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public long ServerTimeMs { get; init; }
    public IReadOnlyList<ConversationListItem> Conversations { get; init; } =
        Array.Empty<ConversationListItem>();
    public ConversationListCursor? ConversationsNextCursor { get; init; }
    public bool ConversationsHasMore { get; init; }
    public IReadOnlyList<ConversationHistoryCatchUp> CatchUps { get; init; } =
        Array.Empty<ConversationHistoryCatchUp>();

    /// <summary>
    /// Conversations whose client watermarks require full recovery (not incremental catch-up).
    /// </summary>
    public IReadOnlyList<SyncCursorResetRequired> ResetsRequired { get; init; } =
        Array.Empty<SyncCursorResetRequired>();

    /// <summary>
    /// 关系列表增量同步结果。null 或空表示未请求关系同步。
    /// </summary>
    public IReadOnlyList<RelationshipCatchUp>? RelationshipCatchUps { get; init; }
}
