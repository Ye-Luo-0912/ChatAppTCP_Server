using ChatApp.TcpGateway.Core.Messaging.Conversations;

namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed class SyncBootstrapResponse
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
}
