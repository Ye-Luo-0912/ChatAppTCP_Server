using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using RealtimeGroupConversationCommand =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationCommand;
using RealtimeGroupConversationOperation =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationOperation;
using RealtimeConversationMemberRole =
    ChatApp.Realtime.Abstractions.Conversations.ConversationMemberRole;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群组变更命令（AddMembers / RemoveMember / Leave / ChangeRole / ListMembers）。
/// <para>
/// 这五个命令共享同一形态：构造 <see cref="RealtimeGroupConversationCommand"/> 调用
/// <see cref="IRealtimeMessageBus.MutateGroupConversationAsync"/>，将结果映射为对应响应类型，
/// 通过 <see cref="SendGroupCommandAsync{TResponse}"/> 统一处理成功/失败路径。
/// </para>
/// <para>
/// 各 handler 仅在：1) 请求校验项，2) Operation 枚举，3) command 字段填充，4) 响应映射 — 四处有差异。
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler
{
    private async ValueTask HandleAddGroupMembersRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _addGroupMembersRequestCodec.Deserialize(payload);

        // 显式分支 request is null 以帮助编译器流分析，避免后续 CS8602。
        if (request is null)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.AddGroupMembersResponse,
                _addGroupMembersResponseCodec,
                new AddGroupMembersResponse
                {
                    RequestId = string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "添加成员请求参数无效。"
                });
            return;
        }

        // 廉价结构校验：RequestId 长度、ConversationId 长度、成员数量上限、正 ID、去重。
        var normalizedMemberIds = ValidateAddMembersRequest(
            request.RequestId,
            request.ConversationId,
            request.MemberUserIds,
            out _);

        if (normalizedMemberIds is null)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.AddGroupMembersResponse,
                _addGroupMembersResponseCodec,
                new AddGroupMembersResponse
                {
                    RequestId = request.RequestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "添加成员请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.AddGroupMembersRequest,
                new RealtimeGroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = RealtimeGroupConversationOperation.AddMembers,
                    ConversationId = request.ConversationId.Trim(),
                    MemberUserIds = normalizedMemberIds,
                    ActorSessionId = session.SessionId
                },
                result => new AddGroupMembersResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Members = MapMembers(result.Members)
                },
                PacketCommand.AddGroupMembersResponse,
                _addGroupMembersResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleRemoveGroupMemberRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _removeGroupMemberRequestCodec.Deserialize(payload);
        var requestId = request?.RequestId ?? string.Empty;

        if (request is null
            || !ValidateMutateRequest(request.RequestId, request.ConversationId, out _)
            || request.TargetUserId <= 0)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.RemoveGroupMemberResponse,
                _removeGroupMemberResponseCodec,
                new RemoveGroupMemberResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "移除成员请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.RemoveGroupMemberRequest,
                new RealtimeGroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = RealtimeGroupConversationOperation.RemoveMember,
                    ConversationId = request.ConversationId.Trim(),
                    TargetUserId = request.TargetUserId,
                    ActorSessionId = session.SessionId
                },
                result => new RemoveGroupMemberResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.RemoveGroupMemberResponse,
                _removeGroupMemberResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleLeaveGroupRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _leaveGroupRequestCodec.Deserialize(payload);
        var requestId = request?.RequestId ?? string.Empty;

        if (request is null
            || !ValidateMutateRequest(request.RequestId, request.ConversationId, out _))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.LeaveGroupResponse,
                _leaveGroupResponseCodec,
                new LeaveGroupResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "退群请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.LeaveGroupRequest,
                new RealtimeGroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = RealtimeGroupConversationOperation.Leave,
                    ConversationId = request.ConversationId.Trim(),
                    ActorSessionId = session.SessionId
                },
                result => new LeaveGroupResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.LeaveGroupResponse,
                _leaveGroupResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleChangeMemberRoleRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _changeMemberRoleRequestCodec.Deserialize(payload);
        var requestId = request?.RequestId ?? string.Empty;

        // 廉价结构校验：通用字段 + TargetUserId > 0 + NewRole 枚举合法性。
        // 权限校验（仅 Owner 可改角色）由 Realtime 侧判定。
        if (request is null
            || !ValidateMutateRequest(request.RequestId, request.ConversationId, out _)
            || request.TargetUserId <= 0
            || !IsValidMemberRole(request.NewRole))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.ChangeMemberRoleResponse,
                _changeMemberRoleResponseCodec,
                new ChangeMemberRoleResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "角色变更请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.ChangeMemberRoleRequest,
                new RealtimeGroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = RealtimeGroupConversationOperation.ChangeRole,
                    ConversationId = request.ConversationId.Trim(),
                    TargetUserId = request.TargetUserId,
                    NewRole = (RealtimeConversationMemberRole)(byte)request.NewRole,
                    ActorSessionId = session.SessionId
                },
                result => new ChangeMemberRoleResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.ChangeMemberRoleResponse,
                _changeMemberRoleResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleListGroupMembersRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _listGroupMembersRequestCodec.Deserialize(payload);
        var requestId = request?.RequestId ?? string.Empty;

        if (request is null
            || !ValidateMutateRequest(request.RequestId, request.ConversationId, out _))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.ListGroupMembersResponse,
                _listGroupMembersResponseCodec,
                new ListGroupMembersResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "成员列表请求参数无效。"
                });
            return;
        }

        // P2-1：捕获分页参数供 map 闭包使用。
        // Realtime 侧返回全量成员（按 role, joined_at_ms, user_id 升序），
        // Gateway 本地执行 keyset 分页，避免改动 RealtimeServices 协议。
        // 幂等缓存保存全量结果，不同分页参数命中同一缓存后各自切片。
        var pageSize = request.PageSize;
        var cursor = request.Cursor;

        await SendGroupCommandAsync(
                session,
                PacketCommand.ListGroupMembersRequest,
                new RealtimeGroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = RealtimeGroupConversationOperation.ListMembers,
                    ConversationId = request.ConversationId.Trim(),
                    ActorSessionId = session.SessionId
                },
                result =>
                {
                    if (!result.Succeeded)
                    {
                        return new ListGroupMembersResponse
                        {
                            RequestId = result.RequestId,
                            Succeeded = false,
                            ErrorCode = result.ErrorCode,
                            ErrorMessage = result.ErrorMessage,
                            ConversationId = result.ConversationId
                        };
                    }

                    var all = MapMembers(result.Members) ?? Array.Empty<ConversationMemberItem>();
                    var (page, nextCursor, hasMore) = PaginateMembers(all, pageSize, cursor);
                    return new ListGroupMembersResponse
                    {
                        RequestId = result.RequestId,
                        Succeeded = true,
                        ErrorCode = result.ErrorCode,
                        ErrorMessage = result.ErrorMessage,
                        ConversationId = result.ConversationId,
                        Members = page,
                        NextCursor = nextCursor,
                        HasMore = hasMore
                    };
                },
                PacketCommand.ListGroupMembersResponse,
                _listGroupMembersResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
