using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using ChatApp.TcpGateway.Gateway.Serialization;
using RealtimeMessageHistoryQuery =
    ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryQuery;

namespace ChatApp.TcpGateway.Gateway.Commands.Queries;

/// <summary>
/// MessageHistoryRequest 查询处理部分（partial）。
/// 包含历史消息分页查询、字节预算截断与响应发送。
/// </summary>
internal sealed partial class HistoryQueryCommandHandler
{
    private async ValueTask HandleMessageHistoryRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            session,
            PacketCommand.MessageHistoryRequest,
            _messageHistoryRequestCodec,
            payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        // Keep the response envelope correlatable even when the request is
        // rejected before Realtime is called. Do not reflect an over-limit
        // value into the response payload.
        var responseConversationId = request.ConversationId?.Length <= 64
            ? request.ConversationId ?? string.Empty
            : string.Empty;
        var hasBeforeTime = request.BeforeReceivedAtMs.HasValue;
        var hasBeforeMessage = !string.IsNullOrWhiteSpace(
            request.BeforeMessageId);
        var hasAfterTime = request.AfterReceivedAtMs.HasValue;
        var hasAfterMessage = !string.IsNullOrWhiteSpace(
            request.AfterMessageId);
        if (requestId.Length > 64
            || request.Limit < 0
            || request.Limit > PacketProtocol.HistoryPageMaxItems
            || hasBeforeTime != hasBeforeMessage
            || hasAfterTime != hasAfterMessage
            || (hasBeforeTime && hasAfterTime)
            || (hasAfterTime && string.IsNullOrWhiteSpace(request.ConversationId))
            || request.BeforeReceivedAtMs is <= 0
            || request.AfterReceivedAtMs is <= 0
            || request.BeforeMessageId?.Length > 64
            || request.AfterMessageId?.Length > 64
            || request.ConversationId?.Length > 64)
        {
            _metrics.HistoryQueryFailed();
            SendMessageHistoryResponse(
                session,
                new MessageHistoryResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    ConversationId = responseConversationId,
                    Succeeded = false,
                    ErrorCode = "invalid_history_request",
                    ErrorMessage = "历史消息请求参数无效。"
                });
            return;
        }

        var query = new RealtimeMessageHistoryQuery
        {
            RequestId = requestId,
            UserId = session.UserId,
            ConversationId = request.ConversationId,
            BeforeReceivedAtMs = request.BeforeReceivedAtMs,
            BeforeMessageId = request.BeforeMessageId,
            // 网关 DTO 字段名为 AfterReceivedAtMs，但 Realtime 侧 After 模式按
            // changed_at_ms 过滤和排序（变更水位），因此映射到 AfterChangedAtMs。
            // 详见 ChatApp.Realtime.Abstractions.MessageHistoryQuery 注释。
            AfterChangedAtMs = request.AfterReceivedAtMs,
            AfterMessageId = request.AfterMessageId,
            Limit = request.Limit
        };

        try
        {
            var page = await _messageBus
                .QueryMessageHistoryAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            var mappedItems = page.Items
                .Select(static item => new MessageHistoryItem
                {
                    MessageId = item.MessageId,
                    ClientMessageId = item.ClientMessageId,
                    SenderUserId = item.SenderUserId,
                    ReceiverUserId = item.ReceiverUserId,
                    ConversationId = item.ConversationId,
                    Content = item.Content,
                    ReceivedAtMs = item.ReceivedAtMs,
                    DeliveredAtMs = item.DeliveredAtMs,
                    ReadAtMs = item.ReadAtMs,
                    RecalledAtMs = item.RecalledAtMs,
                    EditVersion = item.EditVersion,
                    EditedAtMs = item.EditedAtMs,
                    ChangedAtMs = item.ChangedAtMs,
                    Attachments = HistoryWireMapper.MapAttachments(item.Attachments),
                    Reactions = HistoryWireMapper.MapReactions(item.Reactions),
                    ReplyToMessageId = item.ReplyToMessageId,
                    ReplyToSenderUserId = item.ReplyToSenderUserId,
                    ReplyToPreview = item.ReplyToPreview,
                    ForwardedFromMessageId = item.ForwardedFromMessageId,
                    ForwardedFromSenderUserId = item.ForwardedFromSenderUserId,
                    ForwardedFromPreview = item.ForwardedFromPreview,
                    MentionedUserIds = item.MentionedUserIds,
                    MentionedRoles = item.MentionedRoles
                })
                .ToArray();

            var originalNextCursor = page.NextCursor is null
                ? null
                : new MessageHistoryCursor
                {
                    ReceivedAtMs = page.NextCursor.ReceivedAtMs,
                    ChangedAtMs = page.NextCursor.ChangedAtMs,
                    MessageId = page.NextCursor.MessageId
                };

            // 按字节预算截断，确保响应可装入单帧 TCP Payload。
            // 截断时以第 k 条（最后保留条目）派生新 NextCursor，HasMore=true。
            // outcome = ItemTooLarge 时返回 item_too_large 错误，避免无法推进游标的空页。
            var response = ResponseByteBudget.Truncate(
                new MessageHistoryResponse
                {
                    RequestId = requestId,
                    ConversationId = responseConversationId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    Items = mappedItems,
                    NextCursor = originalNextCursor,
                    HasMore = page.HasMore
                },
                mappedItems.Length,
                _messageHistoryResponseCodec,
                PacketProtocol.WireResponseSoftLimit,
                PacketProtocol.WireResponseHardLimit,
                static (original, k) =>
                {
                    if (k >= original.Items.Count)
                    {
                        return original;
                    }

                    var prefix = k <= 0
                        ? Array.Empty<MessageHistoryItem>()
                        : original.Items.Take(k).ToArray();
                    var cursor = k > 0
                        ? new MessageHistoryCursor
                        {
                            ReceivedAtMs = prefix[k - 1].ReceivedAtMs,
                            ChangedAtMs = prefix[k - 1].ChangedAtMs > 0
                                ? prefix[k - 1].ChangedAtMs
                                : null,
                            MessageId = prefix[k - 1].MessageId
                        }
                        : null;
                    return original with
                    {
                        Items = prefix,
                        NextCursor = cursor,
                        HasMore = true
                    };
                },
                out var outcome);

            if (outcome == TruncateOutcome.ItemTooLarge)
            {
                _metrics.CommandFailed(PacketCommand.MessageHistoryRequest);
                SendMessageHistoryResponse(
                    session,
                    new MessageHistoryResponse
                    {
                        RequestId = requestId,
                        ConversationId = responseConversationId,
                        Succeeded = false,
                        ErrorCode = "item_too_large",
                        ErrorMessage = "单条消息超出单帧 Payload 硬上限，无法通过分页返回。"
                    });
                return;
            }

            if (outcome == TruncateOutcome.EnvelopeTooLarge)
            {
                _metrics.CommandFailed(PacketCommand.MessageHistoryRequest);
                SendMessageHistoryResponse(
                    session,
                    new MessageHistoryResponse
                    {
                        RequestId = requestId,
                        ConversationId = responseConversationId,
                        Succeeded = false,
                        ErrorCode = "response_too_large",
                        ErrorMessage = "响应信封超过单帧 Payload 硬上限。"
                    });
                return;
            }

            SendMessageHistoryResponse(session, response);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.HistoryQueryFailed();
            _metrics.CommandFailed(PacketCommand.MessageHistoryRequest);
            _logger.CommandFailed(
                PacketCommand.MessageHistoryRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendMessageHistoryResponse(
                session,
                new MessageHistoryResponse
                {
                    RequestId = requestId,
                    ConversationId = responseConversationId,
                    Succeeded = false,
                    ErrorCode = "history_service_unavailable",
                    ErrorMessage = "历史消息服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendMessageHistoryResponse(
        TcpClientSession session,
        MessageHistoryResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageHistoryPage,
            _messageHistoryResponseCodec,
            session,
            response);
        session.TryQueue(outboundFrame);
    }
}
