namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed record ConversationListResponse
{
    public required string RequestId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public IReadOnlyList<ConversationListItem> Items { get; init; } =
        Array.Empty<ConversationListItem>();
    public ConversationListCursor? NextCursor { get; init; }
    public bool HasMore { get; init; }
}
