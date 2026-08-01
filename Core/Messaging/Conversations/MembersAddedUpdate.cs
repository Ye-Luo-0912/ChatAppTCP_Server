namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

/// <summary>
/// P0-6：群成员批量加入 wire DTO（S2C）。
/// </summary>
public sealed class MembersAddedUpdate
{
    public required string ConversationId { get; init; }
    public required long[] AddedUserIds { get; init; }
    public long ActorUserId { get; init; }
    public string? Title { get; init; }
    public long OccurredAtMs { get; init; }
}
