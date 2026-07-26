using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeGroupConversationCommand =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationCommand;
using RealtimeGroupConversationOperation =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationOperation;
using RealtimeGroupConversationResult =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationResult;
using RealtimeConversationMemberItem =
    ChatApp.Realtime.Abstractions.Conversations.ConversationMemberItem;
using RealtimeConversationMemberRole =
    ChatApp.Realtime.Abstractions.Conversations.ConversationMemberRole;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群组会话相关命令处理器（CreateGroupRequest / AddGroupMembersRequest / RemoveGroupMemberRequest /
/// LeaveGroupRequest / ChangeMemberRoleRequest / ListGroupMembersRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec 与 <see cref="IRealtimeMessageBus"/>，
/// 不再依赖 service 私有字段。行为与原内联 handler 完全等价（校验顺序、错误码、metric 与日志事件）。
/// </para>
/// </summary>
internal sealed class GroupCommandHandler : ICommandHandler
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<CreateGroupRequest> _createGroupRequestCodec;
    private readonly IPayloadCodec<CreateGroupResponse> _createGroupResponseCodec;
    private readonly IPayloadCodec<AddGroupMembersRequest> _addGroupMembersRequestCodec;
    private readonly IPayloadCodec<AddGroupMembersResponse> _addGroupMembersResponseCodec;
    private readonly IPayloadCodec<RemoveGroupMemberRequest> _removeGroupMemberRequestCodec;
    private readonly IPayloadCodec<RemoveGroupMemberResponse> _removeGroupMemberResponseCodec;
    private readonly IPayloadCodec<LeaveGroupRequest> _leaveGroupRequestCodec;
    private readonly IPayloadCodec<LeaveGroupResponse> _leaveGroupResponseCodec;
    private readonly IPayloadCodec<ChangeMemberRoleRequest> _changeMemberRoleRequestCodec;
    private readonly IPayloadCodec<ChangeMemberRoleResponse> _changeMemberRoleResponseCodec;
    private readonly IPayloadCodec<ListGroupMembersRequest> _listGroupMembersRequestCodec;
    private readonly IPayloadCodec<ListGroupMembersResponse> _listGroupMembersResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<GroupCommandHandler> _logger;

    public GroupCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<CreateGroupRequest> createGroupRequestCodec,
        IPayloadCodec<CreateGroupResponse> createGroupResponseCodec,
        IPayloadCodec<AddGroupMembersRequest> addGroupMembersRequestCodec,
        IPayloadCodec<AddGroupMembersResponse> addGroupMembersResponseCodec,
        IPayloadCodec<RemoveGroupMemberRequest> removeGroupMemberRequestCodec,
        IPayloadCodec<RemoveGroupMemberResponse> removeGroupMemberResponseCodec,
        IPayloadCodec<LeaveGroupRequest> leaveGroupRequestCodec,
        IPayloadCodec<LeaveGroupResponse> leaveGroupResponseCodec,
        IPayloadCodec<ChangeMemberRoleRequest> changeMemberRoleRequestCodec,
        IPayloadCodec<ChangeMemberRoleResponse> changeMemberRoleResponseCodec,
        IPayloadCodec<ListGroupMembersRequest> listGroupMembersRequestCodec,
        IPayloadCodec<ListGroupMembersResponse> listGroupMembersResponseCodec,
        GatewayMetrics metrics,
        ILogger<GroupCommandHandler> logger)
    {
        _messageBus = messageBus;
        _createGroupRequestCodec = createGroupRequestCodec;
        _createGroupResponseCodec = createGroupResponseCodec;
        _addGroupMembersRequestCodec = addGroupMembersRequestCodec;
        _addGroupMembersResponseCodec = addGroupMembersResponseCodec;
        _removeGroupMemberRequestCodec = removeGroupMemberRequestCodec;
        _removeGroupMemberResponseCodec = removeGroupMemberResponseCodec;
        _leaveGroupRequestCodec = leaveGroupRequestCodec;
        _leaveGroupResponseCodec = leaveGroupResponseCodec;
        _changeMemberRoleRequestCodec = changeMemberRoleRequestCodec;
        _changeMemberRoleResponseCodec = changeMemberRoleResponseCodec;
        _listGroupMembersRequestCodec = listGroupMembersRequestCodec;
        _listGroupMembersResponseCodec = listGroupMembersResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        ICommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.CreateGroupRequest => HandleCreateGroupRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.AddGroupMembersRequest => HandleAddGroupMembersRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.RemoveGroupMemberRequest => HandleRemoveGroupMemberRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.LeaveGroupRequest => HandleLeaveGroupRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.ChangeMemberRoleRequest => HandleChangeMemberRoleRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.ListGroupMembersRequest => HandleListGroupMembersRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    private async ValueTask HandleCreateGroupRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _createGroupRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || request.RequestId.Length > 64
            || string.IsNullOrWhiteSpace(request.Title)
            || request.Title.Trim().Length > 128)
        {
            _metrics.ProtocolError();
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = request?.RequestId ?? string.Empty,
                Succeeded = false,
                ErrorCode = "invalid_request",
                ErrorMessage = "创建群请求参数无效。"
            });
            return;
        }

        try
        {
            var result = await _messageBus.MutateGroupConversationAsync(
                    new RealtimeGroupConversationCommand
                    {
                        RequestId = request.RequestId,
                        ActorUserId = session.UserId,
                        Operation = RealtimeGroupConversationOperation.Create,
                        Title = request.Title.Trim(),
                        MemberUserIds = request.MemberUserIds,
                        ActorSessionId = session.SessionId
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = result.RequestId,
                Succeeded = result.Succeeded,
                ErrorCode = result.ErrorCode,
                ErrorMessage = result.ErrorMessage,
                ConversationId = result.ConversationId,
                Title = result.Title,
                Members = MapMembers(result.Members)
            });
        }
        catch (Exception ex)
        {
            _metrics.CommandFailed(PacketCommand.CreateGroupRequest);
            _logger.CommandFailed(
                PacketCommand.CreateGroupRequest,
                session.ConnectionId,
                request.RequestId,
                ex);
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = request.RequestId,
                Succeeded = false,
                ErrorCode = "group_unavailable",
                ErrorMessage = "群服务暂时不可用。"
            });
        }
    }

    private async ValueTask HandleAddGroupMembersRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _addGroupMembersRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.MemberUserIds is not { Count: > 0 })
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.AddGroupMembersResponse,
                _addGroupMembersResponseCodec,
                new AddGroupMembersResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
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
                    MemberUserIds = request.MemberUserIds,
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
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.TargetUserId <= 0)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.RemoveGroupMemberResponse,
                _removeGroupMemberResponseCodec,
                new RemoveGroupMemberResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
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
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.LeaveGroupResponse,
                _leaveGroupResponseCodec,
                new LeaveGroupResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
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
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.TargetUserId <= 0)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.ChangeMemberRoleResponse,
                _changeMemberRoleResponseCodec,
                new ChangeMemberRoleResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
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
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.ListGroupMembersResponse,
                _listGroupMembersResponseCodec,
                new ListGroupMembersResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "成员列表请求参数无效。"
                });
            return;
        }

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
                result => new ListGroupMembersResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Members = MapMembers(result.Members)
                },
                PacketCommand.ListGroupMembersResponse,
                _listGroupMembersResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask SendGroupCommandAsync<TResponse>(
        TcpClientSession session,
        PacketCommand requestCommand,
        RealtimeGroupConversationCommand command,
        Func<RealtimeGroupConversationResult, TResponse> map,
        PacketCommand responseCommand,
        IPayloadCodec<TResponse> responseCodec,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await _messageBus.MutateGroupConversationAsync(command, cancellationToken)
                .ConfigureAwait(false);
            SendGroupMutateResponse(session, responseCommand, responseCodec, map(result));
        }
        catch (Exception ex)
        {
            _metrics.CommandFailed(requestCommand);
            _logger.CommandFailed(
                requestCommand,
                session.ConnectionId,
                command.RequestId,
                ex);
            SendGroupMutateResponse(
                session,
                responseCommand,
                responseCodec,
                map(RealtimeGroupConversationResult.Failed(
                    command.RequestId,
                    "group_unavailable",
                    "群服务暂时不可用。")));
        }
    }

    private void SendCreateGroupResponse(TcpClientSession session, CreateGroupResponse response) =>
        SendGroupMutateResponse(
            session,
            PacketCommand.CreateGroupResponse,
            _createGroupResponseCodec,
            response);

    private static void SendGroupMutateResponse<TResponse>(
        TcpClientSession session,
        PacketCommand command,
        IPayloadCodec<TResponse> codec,
        TResponse response)
    {
        using var frame = OutboundFrameFactory.Create(command, codec, response);
        session.TryQueue(frame);
    }

    private static ConversationMemberItem[]? MapMembers(
        IReadOnlyList<RealtimeConversationMemberItem>? members)
    {
        if (members is null)
            return null;
        return members
            .Select(static m => new ConversationMemberItem
            {
                UserId = m.UserId,
                Role = (ConversationMemberRole)(byte)m.Role,
                JoinedAtMs = m.JoinedAtMs
            })
            .ToArray();
    }
}
