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

    /// <summary>页大小（1-200）。null 或 0 表示默认值 50。</summary>
    public int? PageSize { get; init; }

    /// <summary>分页游标（opaque）。null 表示首页。由上一次响应的 NextCursor 提供。</summary>
    public string? Cursor { get; init; }
}

public sealed class ListGroupMembersResponse
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public IReadOnlyList<ConversationMemberItem>? Members { get; init; }

    /// <summary>下一页游标。null 表示无更多数据。</summary>
    public string? NextCursor { get; init; }

    /// <summary>是否还有更多成员可分页获取。</summary>
    public bool HasMore { get; init; }
}
