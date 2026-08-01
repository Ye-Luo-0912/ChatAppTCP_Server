namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

/// <summary>
/// P0-6：会话解散 wire DTO（S2C）。
/// </summary>
public sealed class ConversationDissolvedUpdate
{
    public required string ConversationId { get; init; }
    public long ActorUserId { get; init; }
    public long OccurredAtMs { get; init; }
}
