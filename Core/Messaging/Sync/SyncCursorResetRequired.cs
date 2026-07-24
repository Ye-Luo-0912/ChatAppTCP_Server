namespace ChatApp.TcpGateway.Core.Messaging.Sync;

/// <summary>
/// Resync hint for a conversation whose client watermark is invalid or unusable.
/// </summary>
public sealed class SyncCursorResetRequired
{
    public required string ConversationId { get; init; }
    public required SyncCursorResetReason Reason { get; init; }
    public string? TipMessageId { get; init; }
    public long? TipReceivedAtMs { get; init; }
    public long? ClientAfterReceivedAtMs { get; init; }
    public string? ClientAfterMessageId { get; init; }
}
