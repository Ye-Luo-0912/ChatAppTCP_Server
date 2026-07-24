namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class MemberJoinedUpdate
{
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public ConversationMemberRole Role { get; init; } = ConversationMemberRole.Member;
    public long ActorUserId { get; init; }
    public string? Title { get; init; }
    public long OccurredAtMs { get; init; }
}

public sealed class MemberLeftUpdate
{
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public long OccurredAtMs { get; init; }
}

public sealed class MemberRemovedUpdate
{
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public long ActorUserId { get; init; }
    public long OccurredAtMs { get; init; }
}

public sealed class RoleChangedUpdate
{
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public ConversationMemberRole NewRole { get; init; }
    public ConversationMemberRole? PreviousRole { get; init; }
    public long ActorUserId { get; init; }
    public long OccurredAtMs { get; init; }
}
