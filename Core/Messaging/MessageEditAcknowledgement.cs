namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageEditAcknowledgement
{
    public required string RequestId { get; init; }
    public string? MessageId { get; init; }
    public bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public string? Content { get; init; }
    public int? EditVersion { get; init; }
    public long? EditedAtMs { get; init; }
}
