using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using RealtimeGroupConversationCommand =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationCommand;
using ChatApp.TcpGateway.Gateway.Serialization;
using RealtimeGroupConversationOperation =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationOperation;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群解散命令（DissolveGroupRequest → MutateGroupConversationAsync(Dissolve) → DissolveGroupResponse）。
/// <para>
/// 仅群主可解散（权限由 Realtime 侧判定）；解散成功后 Realtime 广播 ConversationDissolvedUpdate(166)，
/// 网关不在此处额外回包更新，客户端以该推送为准清理本地会话。
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler
{
    private async ValueTask HandleDissolveGroupRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            session,
            PacketCommand.DissolveGroupRequest,
            _dissolveGroupRequestCodec,
            payload);
        var requestId = request?.RequestId ?? string.Empty;

        // 廉价结构校验：通用字段。权限校验（仅 Owner 可解散）由 Realtime 侧判定。
        if (request is null
            || !ValidateMutateRequest(request.RequestId, request.ConversationId, out _))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.DissolveGroupResponse,
                _dissolveGroupResponseCodec,
                new DissolveGroupResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "解散群聊请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.DissolveGroupRequest,
                new RealtimeGroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = RealtimeGroupConversationOperation.Dissolve,
                    ConversationId = request.ConversationId.Trim(),
                    ActorSessionId = session.SessionId
                },
                result => new DissolveGroupResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.DissolveGroupResponse,
                _dissolveGroupResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
