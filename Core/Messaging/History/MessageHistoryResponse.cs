namespace ChatApp.TcpGateway.Core.Messaging.History;

public sealed class MessageHistoryResponse
{
    public required string RequestId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public IReadOnlyList<MessageHistoryItem> Items { get; init; } =
        Array.Empty<MessageHistoryItem>();
    public MessageHistoryCursor? NextCursor { get; init; }
    public bool HasMore { get; init; }
}