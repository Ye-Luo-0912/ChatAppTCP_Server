using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeSyncBootstrapQuery =
    ChatApp.Realtime.Abstractions.Sync.SyncBootstrapQuery;
using RealtimeConversationSyncWatermark =
    ChatApp.Realtime.Abstractions.Sync.ConversationSyncWatermark;

namespace ChatApp.TcpGateway.Gateway.Commands.Queries;

/// <summary>
/// SyncBootstrapRequest 查询处理部分（partial）。
/// 包含多会话同步引导查询、分段字节预算截断与响应发送。
/// </summary>
internal sealed partial class HistoryQueryCommandHandler
{
    private async ValueTask HandleSyncBootstrapRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _syncBootstrapRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || request.ListLimit < 0
            || request.ListLimit > PacketProtocol.ConversationListMaxItems
            || request.HistoryLimitPerConversation < 0
            || request.HistoryLimitPerConversation > PacketProtocol.SyncMaxHistoryPerConversation
            || request.MaxConversationsWithHistory < 0
            || request.MaxConversationsWithHistory > PacketProtocol.SyncMaxConversationsWithHistory
            || request.Watermarks?.Count > PacketProtocol.SyncMaxWatermarks
            || request.Watermarks?.Any(static watermark =>
                string.IsNullOrWhiteSpace(watermark.ConversationId)
                || watermark.ConversationId.Length > 64
                || string.IsNullOrWhiteSpace(watermark.AfterMessageId)
                || watermark.AfterMessageId.Length > 64
                || watermark.AfterReceivedAtMs <= 0) == true
            || request.RelationshipWatermarks?.Count > PacketProtocol.SyncMaxWatermarks
            || request.RelationshipWatermarks?.Any(static watermark =>
                (byte)watermark.ListType < 1
                || (byte)watermark.ListType > 3
                || watermark.AfterSequence < 0) == true
            || request.RelationshipListLimit is int rll and (< 0 or > PacketProtocol.ConversationListMaxItems))
        {
            _metrics.HistoryQueryFailed();
            SendSyncBootstrapResponse(
                session,
                new SyncBootstrapResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_sync_bootstrap_request",
                    ErrorMessage = "同步引导请求参数无效。"
                });
            return;
        }

        var query = new RealtimeSyncBootstrapQuery
        {
            RequestId = requestId,
            UserId = session.UserId,
            DeviceIdHash = session.DeviceIdHash,
            ListLimit = request.ListLimit,
            HistoryLimitPerConversation = request.HistoryLimitPerConversation,
            MaxConversationsWithHistory = request.MaxConversationsWithHistory,
            Watermarks = request.Watermarks?
                .Select(static watermark => new RealtimeConversationSyncWatermark
                {
                    ConversationId = watermark.ConversationId,
                    // v1 wire 字段仍名为 afterReceivedAtMs；Realtime 内部已统一为
                    // changed_at_ms 水位，避免编辑/撤回/Reaction 被增量同步漏掉。
                    AfterChangedAtMs = watermark.AfterReceivedAtMs,
                    AfterMessageId = watermark.AfterMessageId
                })
                .ToArray(),
            RelationshipWatermarks = request.RelationshipWatermarks,
            RelationshipListLimit = request.RelationshipListLimit
        };

        try
        {
            var page = await _messageBus
                .QuerySyncBootstrapAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            // 会话列表与关系同步模型直接来自 ChatApp.Realtime.Contracts。
            var mappedConversations = page.Conversations.ToArray();
            var originalConversationsCursor = page.ConversationsNextCursor;

            var mappedCatchUps = page.CatchUps
                .Select(static catchUp => new ConversationHistoryCatchUp
                {
                    ConversationId = catchUp.ConversationId,
                    Items = catchUp.Items
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
                            Attachments = AttachmentWireMapper.Map(item.Attachments),
                            Reactions = item.Reactions,
                            ReplyToMessageId = item.ReplyToMessageId,
                            ReplyToSenderUserId = item.ReplyToSenderUserId,
                            ReplyToPreview = item.ReplyToPreview,
                            ForwardedFromMessageId = item.ForwardedFromMessageId,
                            ForwardedFromSenderUserId = item.ForwardedFromSenderUserId,
                            ForwardedFromPreview = item.ForwardedFromPreview
                        })
                        .ToArray(),
                    HasMore = catchUp.HasMore,
                    NextCursor = catchUp.NextCursor is null
                        ? null
                        : new MessageHistoryCursor
                        {
                            ReceivedAtMs = catchUp.NextCursor.ReceivedAtMs,
                            MessageId = catchUp.NextCursor.MessageId
                        }
                })
                .ToArray();

            var mappedResets = page.ResetsRequired
                .Select(static reset => new SyncCursorResetRequired
                {
                    ConversationId = reset.ConversationId,
                    Reason = reset.Reason,
                    TipMessageId = reset.TipMessageId,
                    // 保持 v1 JSON 字段兼容；值的语义已经升级为 changed_at_ms。
                    TipReceivedAtMs = reset.TipChangedAtMs,
                    ClientAfterReceivedAtMs = reset.ClientAfterChangedAtMs,
                    ClientAfterMessageId = reset.ClientAfterMessageId
                })
                .ToArray();

            // 按字节预算截断 SyncBootstrap 响应。
            var conversationsBudget = PacketProtocol.WireResponseSoftLimit / 2;
            var perCatchUpBudget = mappedCatchUps.Length > 0
                ? (PacketProtocol.WireResponseSoftLimit - conversationsBudget) / mappedCatchUps.Length
                : 0;

            var truncatedConversations = ResponseByteBudget.TruncateArray(
                mappedConversations,
                _conversationListItemCodec,
                conversationsBudget,
                PacketProtocol.WireResponseHardLimit,
                static (items, k) => k <= 0
                    ? Array.Empty<ConversationListItem>()
                    : items.Take(k).ToArray(),
                out var conversationsOutcome);

            if (conversationsOutcome == TruncateOutcome.ItemTooLarge)
            {
                _metrics.CommandFailed(PacketCommand.SyncBootstrapRequest);
                SendSyncBootstrapResponse(
                    session,
                    new SyncBootstrapResponse
                    {
                        RequestId = page.RequestId,
                        Succeeded = false,
                        ErrorCode = "item_too_large",
                        ErrorMessage = "单条会话超出单帧 Payload 硬上限，无法通过分页返回。"
                    });
                return;
            }

            if (conversationsOutcome == TruncateOutcome.EnvelopeTooLarge)
            {
                _metrics.CommandFailed(PacketCommand.SyncBootstrapRequest);
                SendSyncBootstrapResponse(
                    session,
                    new SyncBootstrapResponse
                    {
                        RequestId = page.RequestId,
                        Succeeded = false,
                        ErrorCode = "response_too_large",
                        ErrorMessage = "响应信封超过单帧 Payload 硬上限。"
                    });
                return;
            }

            var conversationsWasTruncated = truncatedConversations.Length < mappedConversations.Length;
            var conversationsCursor = conversationsWasTruncated
                ? (truncatedConversations.Length > 0
                    ? new ConversationListCursor(
                        truncatedConversations[^1].IsPinned,
                        truncatedConversations[^1].PinnedAtMs,
                        truncatedConversations[^1].LastMessageAtMs,
                        truncatedConversations[^1].ConversationId)
                    : null)
                : originalConversationsCursor;
            var conversationsHasMore = conversationsWasTruncated || page.ConversationsHasMore;

            var truncatedCatchUps = new ConversationHistoryCatchUp[mappedCatchUps.Length];
            for (var i = 0; i < mappedCatchUps.Length; i++)
            {
                var catchUp = mappedCatchUps[i];
                var truncatedItems = ResponseByteBudget.TruncateArray(
                    catchUp.Items,
                    _messageHistoryItemCodec,
                    perCatchUpBudget,
                    PacketProtocol.WireResponseHardLimit,
                    static (items, k) => k <= 0
                        ? Array.Empty<MessageHistoryItem>()
                        : items.Take(k).ToArray(),
                    out var itemsOutcome);

                if (itemsOutcome == TruncateOutcome.ItemTooLarge)
                {
                    _metrics.CommandFailed(PacketCommand.SyncBootstrapRequest);
                    SendSyncBootstrapResponse(
                        session,
                        new SyncBootstrapResponse
                        {
                            RequestId = page.RequestId,
                            Succeeded = false,
                            ErrorCode = "item_too_large",
                            ErrorMessage = "单条消息超出单帧 Payload 硬上限，无法通过分页返回。"
                        });
                    return;
                }

                if (itemsOutcome == TruncateOutcome.EnvelopeTooLarge)
                {
                    _metrics.CommandFailed(PacketCommand.SyncBootstrapRequest);
                    SendSyncBootstrapResponse(
                        session,
                        new SyncBootstrapResponse
                        {
                            RequestId = page.RequestId,
                            Succeeded = false,
                            ErrorCode = "response_too_large",
                            ErrorMessage = "响应信封超过单帧 Payload 硬上限。"
                        });
                    return;
                }

                var itemsWereTruncated = truncatedItems.Length < catchUp.Items.Count;
                var catchUpCursor = itemsWereTruncated
                    ? (truncatedItems.Length > 0
                        ? new MessageHistoryCursor
                        {
                            ReceivedAtMs = truncatedItems[^1].ReceivedAtMs,
                            MessageId = truncatedItems[^1].MessageId
                        }
                        : null)
                    : catchUp.NextCursor;

                truncatedCatchUps[i] = catchUp with
                {
                    Items = truncatedItems,
                    NextCursor = catchUpCursor,
                    HasMore = itemsWereTruncated || catchUp.HasMore
                };
            }

            var response = new SyncBootstrapResponse
            {
                RequestId = page.RequestId,
                Succeeded = page.Succeeded,
                ErrorCode = page.ErrorCode,
                ErrorMessage = page.ErrorMessage,
                ServerTimeMs = page.ServerTimeMs,
                Conversations = truncatedConversations,
                ConversationsNextCursor = conversationsCursor,
                ConversationsHasMore = conversationsHasMore,
                CatchUps = truncatedCatchUps,
                ResetsRequired = mappedResets,
                RelationshipCatchUps = page.RelationshipCatchUps is null || page.RelationshipCatchUps.Count == 0
                    ? null
                    : page.RelationshipCatchUps
            };

            var totalSize = ResponseByteBudget.MeasurePayload(
                _syncBootstrapResponseCodec,
                response,
                PacketProtocol.WireResponseHardLimit);
            while (totalSize < 0 && truncatedCatchUps.Length > 0)
            {
                truncatedCatchUps = truncatedCatchUps[..^1];
                response = response with { CatchUps = truncatedCatchUps };
                totalSize = ResponseByteBudget.MeasurePayload(
                    _syncBootstrapResponseCodec,
                    response,
                    PacketProtocol.WireResponseHardLimit);
            }

            SendSyncBootstrapResponse(session, response);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.HistoryQueryFailed();
            _metrics.CommandFailed(PacketCommand.SyncBootstrapRequest);
            _logger.CommandFailed(
                PacketCommand.SyncBootstrapRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendSyncBootstrapResponse(
                session,
                new SyncBootstrapResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "sync_bootstrap_unavailable",
                    ErrorMessage = "同步引导服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendSyncBootstrapResponse(
        TcpClientSession session,
        SyncBootstrapResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.SyncBootstrapResponse,
            _syncBootstrapResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }
}
