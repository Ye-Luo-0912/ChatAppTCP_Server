namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationSetPrefsResponse
{
    public required string RequestId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public bool IsPinned { get; init; }
    public bool IsMuted { get; init; }
    public long? MutedUntilMs { get; init; }
    public bool Changed { get; init; }
}
