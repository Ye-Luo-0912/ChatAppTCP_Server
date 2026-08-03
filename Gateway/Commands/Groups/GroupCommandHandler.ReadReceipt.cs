using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using RealtimeGroupConversationCommand =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationCommand;
using RealtimeGroupConversationOperation =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationOperation;
using RealtimeGroupConversationResult =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationResult;
using RealtimeMessageReader =
    ChatApp.Realtime.Abstractions.Stores.MessageReader;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// P1-4：群消息已读回执查询（MessageReadReceiptQueryRequest）。
/// <para>
/// 仅消息发送者有权查询。小群返回完整 reader 列表（keyset 分页），大群返回已读人数聚合。
/// 通过 <see cref="IRealtimeMessageBus.QueryReadReceiptsAsync"/> 走 Realtime 侧 NATS 请求/回复，
/// 权限校验（消息发送者）由 Realtime 侧判定。
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler
{
    private async ValueTask HandleMessageReadReceiptQueryRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageReadReceiptQueryRequestCodec.Deserialize(payload);
        var requestId = request?.RequestId ?? string.Empty;

        // 廉价结构校验：RequestId / ConversationId / MessageId 非空。
        if (request is null
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || string.IsNullOrWhiteSpace(request.MessageId))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.MessageReadReceiptQueryResponse,
                _messageReadReceiptQueryResponseCodec,
                new MessageReadReceiptQueryResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "已读回执查询请求参数无效。"
                });
            return;
        }

        try
        {
            var result = await _messageBus.QueryReadReceiptsAsync(
                    new RealtimeGroupConversationCommand
                    {
                        RequestId = request.RequestId,
                        ActorUserId = session.UserId,
                        Operation = RealtimeGroupConversationOperation.QueryReadReceipts,
                        ConversationId = request.ConversationId.Trim(),
                        MessageId = request.MessageId,
                        Cursor = request.Cursor,
                        PageSize = request.PageSize
                    },
                    cancellationToken)
                .ConfigureAwait(false);

            SendGroupMutateResponse(
                session,
                PacketCommand.MessageReadReceiptQueryResponse,
                _messageReadReceiptQueryResponseCodec,
                MapReadReceiptQueryResponse(result));
        }
        // 会话取消（断线/超时/停机）时让取消正常传播，不返回错误响应。
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.CommandFailed(PacketCommand.MessageReadReceiptQueryRequest);
            _logger.CommandFailed(
                PacketCommand.MessageReadReceiptQueryRequest,
                session.ConnectionId,
                request.RequestId,
                ex);
            SendGroupMutateResponse(
                session,
                PacketCommand.MessageReadReceiptQueryResponse,
                _messageReadReceiptQueryResponseCodec,
                new MessageReadReceiptQueryResponse
                {
                    RequestId = request.RequestId,
                    Succeeded = false,
                    ErrorCode = "group_unavailable",
                    ErrorMessage = "群服务暂时不可用。"
                });
        }
    }

    private static MessageReadReceiptQueryResponse MapReadReceiptQueryResponse(
        RealtimeGroupConversationResult result)
    {
        if (!result.Succeeded)
        {
            return new MessageReadReceiptQueryResponse
            {
                RequestId = result.RequestId,
                Succeeded = false,
                ErrorCode = result.ErrorCode,
                ErrorMessage = result.ErrorMessage,
                ConversationId = result.ConversationId
            };
        }

        return new MessageReadReceiptQueryResponse
        {
            RequestId = result.RequestId,
            Succeeded = true,
            ConversationId = result.ConversationId,
            ReadCount = result.ReadCount,
            TotalMemberCount = result.TotalMemberCount,
            IsSmallGroup = result.IsSmallGroup,
            Readers = MapReaders(result.Readers),
            NextCursor = result.NextCursor,
            HasMore = result.HasMore
        };
    }

    private static MessageReadReceiptItem[]? MapReaders(
        IReadOnlyList<RealtimeMessageReader>? readers)
    {
        if (readers is null)
            return null;
        return readers
            .Select(static r => new MessageReadReceiptItem
            {
                UserId = r.UserId,
                ReadAtMs = r.ReadAtMs
            })
            .ToArray();
    }
}