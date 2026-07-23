using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;

namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed class ConversationHistoryCatchUp
{
    public required string ConversationId { get; init; }
    public IReadOnlyList<MessageHistoryItem> Items { get; init; } =
        Array.Empty<MessageHistoryItem>();
    public bool HasMore { get; init; }
    public MessageHistoryCursor? NextCursor { get; init; }
}
