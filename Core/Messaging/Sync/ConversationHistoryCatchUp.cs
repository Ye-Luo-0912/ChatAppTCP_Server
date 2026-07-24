using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;

namespace ChatApp.TcpGateway.Core.Messaging.Sync;

public sealed record ConversationHistoryCatchUp
{
    public required string ConversationId { get; init; }
    public IReadOnlyList<MessageHistoryItem> Items { get; init; } =
        [];
    public bool HasMore { get; init; }
    public MessageHistoryCursor? NextCursor { get; init; }
}
