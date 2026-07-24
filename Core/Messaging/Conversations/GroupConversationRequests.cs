namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class CreateGroupRequest
{
    public required string RequestId { get; init; }
    public required string Title { get; init; }
    public IReadOnlyList<long>? MemberUserIds { get; init; }
}

public sealed class CreateGroupResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public string? Title { get; init; }
    public IReadOnlyList<ConversationMemberItem>? Members { get; init; }
}

public sealed class AddGroupMembersRequest
{
    public required string RequestId { get; init; }
    public required string ConversationId { get; init; }
    public required IReadOnlyList<long> MemberUserIds { get; init; }
}

public sealed class AddGroupMembersResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public IReadOnlyList<ConversationMemberItem>? Members { get; init; }
}

public sealed class RemoveGroupMemberRequest
{
    public required string RequestId { get; init; }
    public required string ConversationId { get; init; }
    public required long TargetUserId { get; init; }
}

public sealed class RemoveGroupMemberResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
}

public sealed class LeaveGroupRequest
{
    public required string RequestId { get; init; }
    public required string ConversationId { get; init; }
}

public sealed class LeaveGroupResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
}

public sealed class ChangeMemberRoleRequest
{
    public required string RequestId { get; init; }
    public required string ConversationId { get; init; }
    public required long TargetUserId { get; init; }
    public required ConversationMemberRole NewRole { get; init; }
}

public sealed class ChangeMemberRoleResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
}

public sealed class ListGroupMembersRequest
{
    public required string RequestId { get; init; }
    public required string ConversationId { get; init; }
}

public sealed class ListGroupMembersResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public IReadOnlyList<ConversationMemberItem>? Members { get; init; }
}
