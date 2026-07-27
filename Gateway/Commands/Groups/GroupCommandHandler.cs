using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
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
/// <para>
/// 各命令的校验、调用与响应逻辑拆分至 partial 文件：
/// <list type="bullet">
/// <item><see cref="GroupCommandHandler.Create"/> — CreateGroup（特殊路径，不使用通用 Mutate 助手）</item>
/// <item><see cref="GroupCommandHandler.Mutate"/> — AddMembers / RemoveMember / Leave / ChangeRole / ListMembers
///   （均通过 <c>SendGroupCommandAsync</c> 走 <c>MutateGroupConversationAsync</c>）</item>
/// <item><see cref="GroupCommandHandler.Helpers"/> — 通用发送/映射助手</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler : ICommandHandler
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
        CommandContext context,
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
}
