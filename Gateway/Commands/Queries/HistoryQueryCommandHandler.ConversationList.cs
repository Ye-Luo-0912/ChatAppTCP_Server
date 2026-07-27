using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeConversationListQuery =
    ChatApp.Realtime.Abstractions.Conversations.ConversationListQuery;

namespace ChatApp.TcpGateway.Gateway.Commands.Queries;

/// <summary>
/// ConversationListRequest 查询处理部分（partial）。
/// 包含会话列表分页查询、字节预算截断与响应发送。
/// </summary>
internal sealed partial class HistoryQueryCommandHandler
{
    private async ValueTask HandleConversationListRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _conversationListRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        var hasCursorId = !string.IsNullOrWhiteSpace(request.BeforeConversationId);
        var hasCursorPinned = request.BeforeIsPinned.HasValue;
        if (requestId.Length > 64
            || request.Limit < 0
            || request.Limit > PacketProtocol.ConversationListMaxItems
            || hasCursorId != hasCursorPinned
            || request.BeforeLastMessageAtMs is <= 0
            || request.BeforePinnedAtMs is <= 0
            || request.BeforeConversationId?.Length > 64)
        {
            _metrics.HistoryQueryFailed();
            SendConversationListResponse(
                session,
                new ConversationListResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_conversation_list_request",
                    ErrorMessage = "会话列表请求参数无效。"
                });
            return;
        }

        var query = new RealtimeConversationListQuery
        {
            RequestId = requestId,
            UserId = session.UserId,
            BeforeIsPinned = request.BeforeIsPinned,
            BeforePinnedAtMs = request.BeforePinnedAtMs,
            BeforeLastMessageAtMs = request.BeforeLastMessageAtMs,
            BeforeConversationId = request.BeforeConversationId,
            Limit = request.Limit
        };

        try
        {
            var page = await _messageBus
                .QueryConversationListAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            var mappedItems = page.Items
                .Select(static item => new ConversationListItem
                {
                    ConversationId = item.ConversationId,
                    Type = (ConversationType)(byte)item.Type,
                    PeerUserId = item.PeerUserId,
                    Title = item.Title,
                    LastMessageId = item.LastMessageId,
                    LastMessagePreview = item.LastMessagePreview,
                    LastMessageAtMs = item.LastMessageAtMs,
                    LastSenderUserId = item.LastSenderUserId,
                    UnreadCount = item.UnreadCount,
                    LastReadMessageId = item.LastReadMessageId,
                    LastReadAtMs = item.LastReadAtMs,
                    IsPinned = item.IsPinned,
                    PinnedAtMs = item.PinnedAtMs,
                    IsMuted = item.IsMuted,
                    MutedUntilMs = item.MutedUntilMs
                })
                .ToArray();

            var originalNextCursor = page.NextCursor is null
                ? null
                : new ConversationListCursor
                {
                    IsPinned = page.NextCursor.IsPinned,
                    PinnedAtMs = page.NextCursor.PinnedAtMs,
                    LastMessageAtMs = page.NextCursor.LastMessageAtMs,
                    ConversationId = page.NextCursor.ConversationId
                };

            // 按字节预算截断，确保响应可装入单帧 TCP Payload。
            // 截断时以第 k 条（最后保留条目）派生新 NextCursor，HasMore=true。
            // outcome = ItemTooLarge 时返回 item_too_large 错误，避免无法推进游标的空页。
            var response = ResponseByteBudget.Truncate(
                new ConversationListResponse
                {
                    RequestId = page.RequestId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    Items = mappedItems,
                    NextCursor = originalNextCursor,
                    HasMore = page.HasMore
                },
                mappedItems.Length,
                _conversationListResponseCodec,
                PacketProtocol.WireResponseSoftLimit,
                PacketProtocol.WireResponseHardLimit,
                static (original, k) =>
                {
                    if (k >= original.Items.Count)
                    {
                        return original;
                    }

                    var prefix = k <= 0
                        ? Array.Empty<ConversationListItem>()
                        : original.Items.Take(k).ToArray();
                    var cursor = k > 0
                        ? new ConversationListCursor
                        {
                            IsPinned = prefix[k - 1].IsPinned,
                            PinnedAtMs = prefix[k - 1].PinnedAtMs,
                            LastMessageAtMs = prefix[k - 1].LastMessageAtMs,
                            ConversationId = prefix[k - 1].ConversationId
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
                _metrics.CommandFailed(PacketCommand.ConversationListRequest);
                SendConversationListResponse(
                    session,
                    new ConversationListResponse
                    {
                        RequestId = page.RequestId,
                        Succeeded = false,
                        ErrorCode = "item_too_large",
                        ErrorMessage = "单条会话超出单帧 Payload 硬上限，无法通过分页返回。"
                    });
                return;
            }

            if (outcome == TruncateOutcome.EnvelopeTooLarge)
            {
                _metrics.CommandFailed(PacketCommand.ConversationListRequest);
                SendConversationListResponse(
                    session,
                    new ConversationListResponse
                    {
                        RequestId = page.RequestId,
                        Succeeded = false,
                        ErrorCode = "response_too_large",
                        ErrorMessage = "响应信封超过单帧 Payload 硬上限。"
                    });
                return;
            }

            SendConversationListResponse(session, response);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.HistoryQueryFailed();
            _metrics.CommandFailed(PacketCommand.ConversationListRequest);
            _logger.CommandFailed(
                PacketCommand.ConversationListRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendConversationListResponse(
                session,
                new ConversationListResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "conversation_list_unavailable",
                    ErrorMessage = "会话列表服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendConversationListResponse(
        TcpClientSession session,
        ConversationListResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ConversationListPage,
            _conversationListResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }
}
