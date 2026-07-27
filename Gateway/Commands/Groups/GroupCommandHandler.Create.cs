using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeGroupConversationCommand =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationCommand;
using RealtimeGroupConversationOperation =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationOperation;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// CreateGroup 命令的校验、调用与响应。
/// <para>
/// 与其他 Mutate 命令不同：Create 不通过 <c>SendGroupCommandAsync</c> 通用助手，
/// 因为它需要构造 <see cref="CreateGroupResponse"/> 的特殊字段（Title/Members），
/// 并且校验逻辑包含 Title 长度上限。
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler
{
    private async ValueTask HandleCreateGroupRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _createGroupRequestCodec.Deserialize(payload);

        // 廉价结构校验：RequestId 长度、Title 长度、初始成员数量上限、正 ID、去重。
        // 权限校验（任何已认证用户可建群）与累计成员上限由 Realtime 侧判定。
        // 显式分支 request is null 以帮助编译器流分析，避免后续 CS8602。
        if (request is null)
        {
            _metrics.ProtocolError();
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = string.Empty,
                Succeeded = false,
                ErrorCode = "invalid_request",
                ErrorMessage = "创建群请求参数无效。"
            });
            return;
        }

        var normalizedMemberIds = ValidateCreateGroupRequest(
            request.RequestId,
            request.Title,
            request.MemberUserIds,
            session.UserId,
            out _);

        if (normalizedMemberIds is null)
        {
            _metrics.ProtocolError();
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = request.RequestId,
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
                        MemberUserIds = normalizedMemberIds,
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
        // 会话取消（断线/超时/停机）时让取消正常传播，不返回错误响应——
        // 连接已断，响应无处可去；错误响应会误导客户端认为命令失败而非连接中断。
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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

    private void SendCreateGroupResponse(TcpClientSession session, CreateGroupResponse response) =>
        SendGroupMutateResponse(
            session,
            PacketCommand.CreateGroupResponse,
            _createGroupResponseCodec,
            response);
}
