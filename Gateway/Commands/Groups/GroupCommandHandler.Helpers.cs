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
using RealtimeGroupConversationResult =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationResult;
using RealtimeConversationMemberItem =
    ChatApp.Realtime.Abstractions.Conversations.ConversationMemberItem;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群组命令通用助手：发送/映射/异常映射。
/// <para>
/// <see cref="SendGroupCommandAsync{TResponse}"/> 封装 Mutate 命令的成功/异常双路径：
/// 成功时映射结果并通过 <see cref="SendGroupMutateResponse{TResponse}"/> 发送；
/// 异常时记录 metric/log，并构造 "group_unavailable" 失败响应（避免连接因服务侧异常被关闭）。
/// </para>
/// <para>
/// <see cref="MapMembers"/> 将 Realtime 侧成员投影为 wire 协议成员，
/// 通过 <c>(byte)</c> 强制转换角色枚举，确保两端枚举值显式对齐。
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler
{
    private async ValueTask SendGroupCommandAsync<TResponse>(
        TcpClientSession session,
        PacketCommand requestCommand,
        RealtimeGroupConversationCommand command,
        Func<RealtimeGroupConversationResult, TResponse> map,
        PacketCommand responseCommand,
        IPayloadCodec<TResponse> responseCodec,
        CancellationToken cancellationToken)
    {
        // 幂等快速路径：缓存命中时直接返回缓存的 Realtime 结果，跳过 Redis/NATS 往返。
        // 仅对 mutate 命令生效（CreateGroup 走独立路径，见 HandleCreateGroupRequestAsync）。
        // ListMembers（查询）同样受益：重复翻页请求可直接返回缓存。
        if (_idempotencyCache is { } cache)
        {
            var cached = cache.TryGet(command.ActorUserId, command.RequestId);
            if (cached is not null)
            {
                SendGroupMutateResponse(session, responseCommand, responseCodec, map(cached));
                return;
            }
        }

        try
        {
            var result = await _messageBus.MutateGroupConversationAsync(command, cancellationToken)
                .ConfigureAwait(false);

            // 缓存 Realtime 正常返回的结果（含业务失败）。
            // 异常路径的 group_unavailable 不经过此处，不会被缓存，确保瞬态故障可重试。
            if (_idempotencyCache is { } cacheForAdd)
            {
                cacheForAdd.TryAdd(command.ActorUserId, command.RequestId, result);
            }

            SendGroupMutateResponse(session, responseCommand, responseCodec, map(result));
        }
        // 会话取消（断线/超时/停机）时让取消正常传播，不返回错误响应——
        // 连接已断，响应无处可去；错误响应会误导客户端认为命令失败而非连接中断。
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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
