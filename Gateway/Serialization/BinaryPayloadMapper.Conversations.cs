using ChatApp.TcpGateway.Core.Messaging.Conversations;
using SharedAddGroupMembersRequest = ChatApp.Shared.Protocol.Tcp.TcpAddGroupMembersRequest;
using SharedAddGroupMembersResponse = ChatApp.Shared.Protocol.Tcp.TcpAddGroupMembersResponse;
using SharedChangeMemberRoleRequest = ChatApp.Shared.Protocol.Tcp.TcpChangeMemberRoleRequest;
using SharedChangeMemberRoleResponse = ChatApp.Shared.Protocol.Tcp.TcpChangeMemberRoleResponse;
using SharedConversationChangedUpdate = ChatApp.Shared.Protocol.Tcp.ConversationChangedUpdate;
using SharedConversationDissolvedUpdate = ChatApp.Shared.Protocol.Tcp.TcpConversationDissolvedUpdate;
using SharedConversationListPage = ChatApp.Shared.Protocol.Tcp.ConversationListPage;
using SharedConversationListRequest = ChatApp.Shared.Protocol.Tcp.ConversationListRequest;
using SharedConversationReadUpdate = ChatApp.Shared.Protocol.Tcp.ConversationReadUpdate;
using SharedCreateGroupRequest = ChatApp.Shared.Protocol.Tcp.TcpCreateGroupRequest;
using SharedCreateGroupResponse = ChatApp.Shared.Protocol.Tcp.TcpCreateGroupResponse;
using SharedDissolveGroupRequest = ChatApp.Shared.Protocol.Tcp.TcpDissolveGroupRequest;
using SharedDissolveGroupResponse = ChatApp.Shared.Protocol.Tcp.TcpDissolveGroupResponse;
using SharedLeaveGroupRequest = ChatApp.Shared.Protocol.Tcp.TcpLeaveGroupRequest;
using SharedLeaveGroupResponse = ChatApp.Shared.Protocol.Tcp.TcpLeaveGroupResponse;
using SharedListGroupMembersRequest = ChatApp.Shared.Protocol.Tcp.TcpListGroupMembersRequest;
using SharedListGroupMembersResponse = ChatApp.Shared.Protocol.Tcp.TcpListGroupMembersResponse;
using SharedMarkReadRequest = ChatApp.Shared.Protocol.Tcp.ConversationMarkReadRequest;
using SharedMarkReadResponse = ChatApp.Shared.Protocol.Tcp.ConversationMarkReadResponse;
using SharedMemberJoinedUpdate = ChatApp.Shared.Protocol.Tcp.TcpMemberJoinedUpdate;
using SharedMemberLeftUpdate = ChatApp.Shared.Protocol.Tcp.TcpMemberLeftUpdate;
using SharedMemberRemovedUpdate = ChatApp.Shared.Protocol.Tcp.TcpMemberRemovedUpdate;
using SharedMessageReadReceiptItem = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptItem;
using SharedMessageReadReceiptQueryRequest = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptQueryRequest;
using SharedMessageReadReceiptQueryResponse = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptQueryResponse;
using SharedMembersAddedUpdate = ChatApp.Shared.Protocol.Tcp.TcpMembersAddedUpdate;
using SharedRemoveGroupMemberRequest = ChatApp.Shared.Protocol.Tcp.TcpRemoveGroupMemberRequest;
using SharedRemoveGroupMemberResponse = ChatApp.Shared.Protocol.Tcp.TcpRemoveGroupMemberResponse;
using SharedRoleChangedUpdate = ChatApp.Shared.Protocol.Tcp.TcpRoleChangedUpdate;
using SharedSetPrefsRequest = ChatApp.Shared.Protocol.Tcp.ConversationSetPrefsRequest;
using SharedSetPrefsResponse = ChatApp.Shared.Protocol.Tcp.ConversationSetPrefsResponse;
using SharedUnreadCountChanged = ChatApp.Shared.Protocol.Tcp.UnreadCountChanged;

namespace ChatApp.TcpGateway.Gateway.Serialization;

/// <summary>
/// 会话列表 / 已读水位 / 偏好 / 群组命令的本地 ↔ 共享映射。
/// </summary>
internal static partial class BinaryPayloadMapper
{
    // ──────────── 会话列表 ────────────

    private static SharedConversationListRequest ToShared(ConversationListRequest request) => new()
    {
        RequestId = request.RequestId,
        BeforeIsPinned = request.BeforeIsPinned,
        BeforePinnedAtMs = request.BeforePinnedAtMs,
        BeforeLastMessageAtMs = request.BeforeLastMessageAtMs,
        BeforeConversationId = request.BeforeConversationId,
        Limit = request.Limit
    };

    private static ConversationListRequest ToLocal(SharedConversationListRequest request) => new()
    {
        RequestId = request.RequestId,
        BeforeIsPinned = request.BeforeIsPinned,
        BeforePinnedAtMs = request.BeforePinnedAtMs,
        BeforeLastMessageAtMs = request.BeforeLastMessageAtMs,
        BeforeConversationId = request.BeforeConversationId,
        Limit = request.Limit
    };

    /// <summary>本地 ConversationListResponse(record，Realtime 契约 item) ↔ 共享 ConversationListPage。</summary>
    private static SharedConversationListPage ToShared(ConversationListResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        Items = MapListItems(response.Items),
        NextCursor = MapCursor(response.NextCursor),
        HasMore = response.HasMore
    };

    private static ConversationListResponse ToLocal(SharedConversationListPage response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        Items = MapListItems(response.Items),
        NextCursor = MapCursor(response.NextCursor),
        HasMore = response.HasMore
    };

    // ──────────── 已读 / 偏好 ────────────

    private static SharedMarkReadRequest ToShared(ConversationMarkReadRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        ReadAtMs = request.ReadAtMs,
        ReadMessageId = request.ReadMessageId
    };

    private static ConversationMarkReadRequest ToLocal(SharedMarkReadRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        ReadAtMs = request.ReadAtMs,
        ReadMessageId = request.ReadMessageId
    };

    private static SharedMarkReadResponse ToShared(ConversationMarkReadResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        UnreadCount = response.UnreadCount,
        LastReadMessageId = response.LastReadMessageId,
        LastReadAtMs = response.LastReadAtMs,
        Changed = response.Changed
    };

    private static ConversationMarkReadResponse ToLocal(SharedMarkReadResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        UnreadCount = response.UnreadCount,
        LastReadMessageId = response.LastReadMessageId,
        LastReadAtMs = response.LastReadAtMs,
        Changed = response.Changed
    };

    private static SharedSetPrefsRequest ToShared(ConversationSetPrefsRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        Pinned = request.Pinned,
        Muted = request.Muted,
        MutedUntilMs = request.MutedUntilMs
    };

    private static ConversationSetPrefsRequest ToLocal(SharedSetPrefsRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        Pinned = request.Pinned,
        Muted = request.Muted,
        MutedUntilMs = request.MutedUntilMs
    };

    private static SharedSetPrefsResponse ToShared(ConversationSetPrefsResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        IsPinned = response.IsPinned,
        IsMuted = response.IsMuted,
        MutedUntilMs = response.MutedUntilMs,
        Changed = response.Changed
    };

    private static ConversationSetPrefsResponse ToLocal(SharedSetPrefsResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        IsPinned = response.IsPinned,
        IsMuted = response.IsMuted,
        MutedUntilMs = response.MutedUntilMs,
        Changed = response.Changed
    };

    // ──────────── 会话事件 ────────────

    private static SharedConversationChangedUpdate ToShared(ConversationChanged update) => new()
    {
        ConversationId = update.ConversationId,
        Type = ToSharedConversationType(update.Type),
        PeerUserId = update.PeerUserId,
        Title = update.Title,
        LastMessageId = update.LastMessageId,
        LastMessagePreview = update.LastMessagePreview,
        LastMessageAtMs = update.LastMessageAtMs,
        LastSenderUserId = update.LastSenderUserId,
        IsPinned = update.IsPinned,
        IsMuted = update.IsMuted,
        MutedUntilMs = update.MutedUntilMs
    };

    private static ConversationChanged ToLocal(SharedConversationChangedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        Type = ToLocalConversationType(update.Type),
        PeerUserId = update.PeerUserId,
        Title = update.Title,
        LastMessageId = update.LastMessageId,
        LastMessagePreview = update.LastMessagePreview,
        LastMessageAtMs = update.LastMessageAtMs,
        LastSenderUserId = update.LastSenderUserId,
        IsPinned = update.IsPinned,
        IsMuted = update.IsMuted,
        MutedUntilMs = update.MutedUntilMs
    };

    private static SharedUnreadCountChanged ToShared(UnreadCountChanged update) => new()
    {
        ConversationId = update.ConversationId,
        UnreadCount = update.UnreadCount,
        LastReadMessageId = update.LastReadMessageId,
        LastReadAtMs = update.LastReadAtMs
    };

    private static UnreadCountChanged ToLocal(SharedUnreadCountChanged update) => new()
    {
        ConversationId = update.ConversationId,
        UnreadCount = update.UnreadCount,
        LastReadMessageId = update.LastReadMessageId,
        LastReadAtMs = update.LastReadAtMs
    };

    private static SharedConversationReadUpdate ToShared(ConversationReadUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        ReaderUserId = update.ReaderUserId,
        LastReadMessageId = update.LastReadMessageId,
        LastReadAtMs = update.LastReadAtMs
    };

    private static ConversationReadUpdate ToLocal(SharedConversationReadUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        ReaderUserId = update.ReaderUserId,
        LastReadMessageId = update.LastReadMessageId,
        LastReadAtMs = update.LastReadAtMs
    };

    // ──────────── 已读回执查询 ────────────

    private static SharedMessageReadReceiptQueryRequest ToShared(MessageReadReceiptQueryRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        MessageId = request.MessageId,
        Cursor = request.Cursor,
        PageSize = request.PageSize
    };

    private static MessageReadReceiptQueryRequest ToLocal(SharedMessageReadReceiptQueryRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        MessageId = request.MessageId,
        Cursor = request.Cursor,
        PageSize = request.PageSize
    };

    private static SharedMessageReadReceiptQueryResponse ToShared(MessageReadReceiptQueryResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        ReadCount = response.ReadCount,
        TotalMemberCount = response.TotalMemberCount,
        IsSmallGroup = response.IsSmallGroup,
        Readers = MapReadReceiptItems(response.Readers),
        NextCursor = response.NextCursor,
        HasMore = response.HasMore
    };

    private static MessageReadReceiptQueryResponse ToLocal(SharedMessageReadReceiptQueryResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        ReadCount = response.ReadCount,
        TotalMemberCount = response.TotalMemberCount,
        IsSmallGroup = response.IsSmallGroup,
        Readers = MapReadReceiptItems(response.Readers),
        NextCursor = response.NextCursor,
        HasMore = response.HasMore
    };

    // ──────────── 群组命令 ────────────

    private static SharedCreateGroupRequest ToShared(CreateGroupRequest request) => new()
    {
        RequestId = request.RequestId,
        Title = request.Title,
        MemberUserIds = request.MemberUserIds
    };

    private static CreateGroupRequest ToLocal(SharedCreateGroupRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        Title = request.Title,
        MemberUserIds = request.MemberUserIds
    };

    private static SharedCreateGroupResponse ToShared(CreateGroupResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        Title = response.Title,
        Members = MapMembers(response.Members)
    };

    private static CreateGroupResponse ToLocal(SharedCreateGroupResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        Title = response.Title,
        Members = MapMembers(response.Members)
    };

    private static SharedAddGroupMembersRequest ToShared(AddGroupMembersRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        MemberUserIds = request.MemberUserIds
    };

    private static AddGroupMembersRequest ToLocal(SharedAddGroupMembersRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        ConversationId = request.ConversationId,
        MemberUserIds = request.MemberUserIds
    };

    private static SharedAddGroupMembersResponse ToShared(AddGroupMembersResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        Members = MapMembers(response.Members)
    };

    private static AddGroupMembersResponse ToLocal(SharedAddGroupMembersResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        Members = MapMembers(response.Members)
    };

    private static SharedRemoveGroupMemberRequest ToShared(RemoveGroupMemberRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        TargetUserId = request.TargetUserId
    };

    private static RemoveGroupMemberRequest ToLocal(SharedRemoveGroupMemberRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        ConversationId = request.ConversationId,
        TargetUserId = request.TargetUserId
    };

    private static SharedRemoveGroupMemberResponse ToShared(RemoveGroupMemberResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static RemoveGroupMemberResponse ToLocal(SharedRemoveGroupMemberResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static SharedLeaveGroupRequest ToShared(LeaveGroupRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId
    };

    private static LeaveGroupRequest ToLocal(SharedLeaveGroupRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        ConversationId = request.ConversationId
    };

    private static SharedLeaveGroupResponse ToShared(LeaveGroupResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static LeaveGroupResponse ToLocal(SharedLeaveGroupResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static SharedDissolveGroupRequest ToShared(DissolveGroupRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId
    };

    private static DissolveGroupRequest ToLocal(SharedDissolveGroupRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        ConversationId = request.ConversationId
    };

    private static SharedDissolveGroupResponse ToShared(DissolveGroupResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static DissolveGroupResponse ToLocal(SharedDissolveGroupResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static SharedChangeMemberRoleRequest ToShared(ChangeMemberRoleRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        TargetUserId = request.TargetUserId,
        NewRole = ToSharedRole(request.NewRole)
    };

    private static ChangeMemberRoleRequest ToLocal(SharedChangeMemberRoleRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        ConversationId = request.ConversationId,
        TargetUserId = request.TargetUserId,
        NewRole = ToLocalRole(request.NewRole)
    };

    private static SharedChangeMemberRoleResponse ToShared(ChangeMemberRoleResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static ChangeMemberRoleResponse ToLocal(SharedChangeMemberRoleResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId
    };

    private static SharedListGroupMembersRequest ToShared(ListGroupMembersRequest request) => new()
    {
        RequestId = request.RequestId,
        ConversationId = request.ConversationId,
        PageSize = request.PageSize,
        Cursor = request.Cursor
    };

    private static ListGroupMembersRequest ToLocal(SharedListGroupMembersRequest request) => new()
    {
        RequestId = request.RequestId ?? string.Empty,
        ConversationId = request.ConversationId,
        PageSize = request.PageSize,
        Cursor = request.Cursor
    };

    private static SharedListGroupMembersResponse ToShared(ListGroupMembersResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        Members = MapMembers(response.Members),
        NextCursor = response.NextCursor,
        HasMore = response.HasMore
    };

    private static ListGroupMembersResponse ToLocal(SharedListGroupMembersResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ConversationId = response.ConversationId,
        Members = MapMembers(response.Members),
        NextCursor = response.NextCursor,
        HasMore = response.HasMore
    };

    // ──────────── 群组事件 ────────────

    private static SharedMemberJoinedUpdate ToShared(MemberJoinedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        Role = ToSharedRole(update.Role),
        ActorUserId = update.ActorUserId,
        Title = update.Title,
        OccurredAtMs = update.OccurredAtMs
    };

    private static MemberJoinedUpdate ToLocal(SharedMemberJoinedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        Role = ToLocalRole(update.Role),
        ActorUserId = update.ActorUserId,
        Title = update.Title,
        OccurredAtMs = update.OccurredAtMs
    };

    private static SharedMemberLeftUpdate ToShared(MemberLeftUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static MemberLeftUpdate ToLocal(SharedMemberLeftUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static SharedMemberRemovedUpdate ToShared(MemberRemovedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        ActorUserId = update.ActorUserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static MemberRemovedUpdate ToLocal(SharedMemberRemovedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        ActorUserId = update.ActorUserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static SharedRoleChangedUpdate ToShared(RoleChangedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        NewRole = ToSharedRole(update.NewRole),
        PreviousRole = update.PreviousRole is { } previous ? ToSharedRole(previous) : null,
        ActorUserId = update.ActorUserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static RoleChangedUpdate ToLocal(SharedRoleChangedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        UserId = update.UserId,
        NewRole = ToLocalRole(update.NewRole),
        PreviousRole = update.PreviousRole is { } previous ? ToLocalRole(previous) : null,
        ActorUserId = update.ActorUserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static SharedMembersAddedUpdate ToShared(MembersAddedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        AddedUserIds = update.AddedUserIds,
        ActorUserId = update.ActorUserId,
        Title = update.Title,
        OccurredAtMs = update.OccurredAtMs
    };

    private static MembersAddedUpdate ToLocal(SharedMembersAddedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        AddedUserIds = update.AddedUserIds.ToArray(),
        ActorUserId = update.ActorUserId,
        Title = update.Title,
        OccurredAtMs = update.OccurredAtMs
    };

    private static SharedConversationDissolvedUpdate ToShared(ConversationDissolvedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        ActorUserId = update.ActorUserId,
        OccurredAtMs = update.OccurredAtMs
    };

    private static ConversationDissolvedUpdate ToLocal(SharedConversationDissolvedUpdate update) => new()
    {
        ConversationId = update.ConversationId,
        ActorUserId = update.ActorUserId,
        OccurredAtMs = update.OccurredAtMs
    };
}
