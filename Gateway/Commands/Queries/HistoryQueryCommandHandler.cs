using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeMessageHistoryQuery =
    ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryQuery;
using RealtimeConversationListQuery =
    ChatApp.Realtime.Abstractions.Conversations.ConversationListQuery;
using RealtimeSyncBootstrapQuery =
    ChatApp.Realtime.Abstractions.Sync.SyncBootstrapQuery;
using RealtimeConversationSyncWatermark =
    ChatApp.Realtime.Abstractions.Sync.ConversationSyncWatermark;

namespace ChatApp.TcpGateway.Gateway.Commands.Queries;

/// <summary>
/// 查询类命令处理器（MessageHistoryRequest / ConversationListRequest / SyncBootstrapRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="IRealtimeMessageBus"/>，不再依赖 service 私有字段。
/// 行为与原内联 handler 完全等价（校验顺序、错误码、metric 与日志事件、字节预算截断逻辑）。
/// </para>
/// </summary>
internal sealed class HistoryQueryCommandHandler : ICommandHandler
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<MessageHistoryRequest> _messageHistoryRequestCodec;
    private readonly IPayloadCodec<MessageHistoryResponse> _messageHistoryResponseCodec;
    private readonly IPayloadCodec<MessageHistoryItem[]> _messageHistoryItemCodec;
    private readonly IPayloadCodec<ConversationListRequest> _conversationListRequestCodec;
    private readonly IPayloadCodec<ConversationListResponse> _conversationListResponseCodec;
    private readonly IPayloadCodec<ConversationListItem[]> _conversationListItemCodec;
    private readonly IPayloadCodec<SyncBootstrapRequest> _syncBootstrapRequestCodec;
    private readonly IPayloadCodec<SyncBootstrapResponse> _syncBootstrapResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<HistoryQueryCommandHandler> _logger;

    public HistoryQueryCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<MessageHistoryRequest> messageHistoryRequestCodec,
        IPayloadCodec<MessageHistoryResponse> messageHistoryResponseCodec,
        IPayloadCodec<MessageHistoryItem[]> messageHistoryItemCodec,
        IPayloadCodec<ConversationListRequest> conversationListRequestCodec,
        IPayloadCodec<ConversationListResponse> conversationListResponseCodec,
        IPayloadCodec<ConversationListItem[]> conversationListItemCodec,
        IPayloadCodec<SyncBootstrapRequest> syncBootstrapRequestCodec,
        IPayloadCodec<SyncBootstrapResponse> syncBootstrapResponseCodec,
        GatewayMetrics metrics,
        ILogger<HistoryQueryCommandHandler> logger)
    {
        _messageBus = messageBus;
        _messageHistoryRequestCodec = messageHistoryRequestCodec;
        _messageHistoryResponseCodec = messageHistoryResponseCodec;
        _messageHistoryItemCodec = messageHistoryItemCodec;
        _conversationListRequestCodec = conversationListRequestCodec;
        _conversationListResponseCodec = conversationListResponseCodec;
        _conversationListItemCodec = conversationListItemCodec;
        _syncBootstrapRequestCodec = syncBootstrapRequestCodec;
        _syncBootstrapResponseCodec = syncBootstrapResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        ICommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.MessageHistoryRequest => HandleMessageHistoryRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.ConversationListRequest => HandleConversationListRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.SyncBootstrapRequest => HandleSyncBootstrapRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    private async ValueTask HandleMessageHistoryRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageHistoryRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
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
            AfterReceivedAtMs = request.AfterReceivedAtMs,
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
                    Attachments = AttachmentWireMapper.Map(item.Attachments),
                    Reactions = item.Reactions?
                        .Select(static reaction => new MessageReactionSummary
                        {
                            Emoji = reaction.Emoji,
                            Count = reaction.Count,
                            ReactedByMe = reaction.ReactedByMe
                        })
                        .ToArray(),
                    ReplyToMessageId = item.ReplyToMessageId,
                    ReplyToSenderUserId = item.ReplyToSenderUserId,
                    ReplyToPreview = item.ReplyToPreview,
                    ForwardedFromMessageId = item.ForwardedFromMessageId,
                    ForwardedFromSenderUserId = item.ForwardedFromSenderUserId,
                    ForwardedFromPreview = item.ForwardedFromPreview
                })
                .ToArray();

            var originalNextCursor = page.NextCursor is null
                ? null
                : new MessageHistoryCursor
                {
                    ReceivedAtMs = page.NextCursor.ReceivedAtMs,
                    MessageId = page.NextCursor.MessageId
                };

            // 按字节预算截断，确保响应可装入单帧 TCP Payload。
            // 截断时以第 k 条（最后保留条目）派生新 NextCursor，HasMore=true。
            var response = ResponseByteBudget.Truncate(
                new MessageHistoryResponse
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
                            MessageId = prefix[k - 1].MessageId
                        }
                        : null;
                    return original with
                    {
                        Items = prefix,
                        NextCursor = cursor,
                        HasMore = true
                    };
                });

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
            response);
        session.TryQueue(outboundFrame);
    }

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
                });

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
                || watermark.AfterReceivedAtMs <= 0) == true)
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
                    AfterReceivedAtMs = watermark.AfterReceivedAtMs,
                    AfterMessageId = watermark.AfterMessageId
                })
                .ToArray()
        };

        try
        {
            var page = await _messageBus
                .QuerySyncBootstrapAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            var mappedConversations = page.Conversations
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

            var originalConversationsCursor = page.ConversationsNextCursor is null
                ? null
                : new ConversationListCursor
                {
                    IsPinned = page.ConversationsNextCursor.IsPinned,
                    PinnedAtMs = page.ConversationsNextCursor.PinnedAtMs,
                    LastMessageAtMs = page.ConversationsNextCursor.LastMessageAtMs,
                    ConversationId = page.ConversationsNextCursor.ConversationId
                };

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
                            Reactions = item.Reactions?
                                .Select(static reaction => new MessageReactionSummary
                                {
                                    Emoji = reaction.Emoji,
                                    Count = reaction.Count,
                                    ReactedByMe = reaction.ReactedByMe
                                })
                                .ToArray(),
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
                    Reason = (SyncCursorResetReason)(byte)reset.Reason,
                    TipMessageId = reset.TipMessageId,
                    TipReceivedAtMs = reset.TipReceivedAtMs,
                    ClientAfterReceivedAtMs = reset.ClientAfterReceivedAtMs,
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
                    : items.Take(k).ToArray());

            var conversationsWasTruncated = truncatedConversations.Length < mappedConversations.Length;
            var conversationsCursor = conversationsWasTruncated
                ? (truncatedConversations.Length > 0
                    ? new ConversationListCursor
                    {
                        IsPinned = truncatedConversations[^1].IsPinned,
                        PinnedAtMs = truncatedConversations[^1].PinnedAtMs,
                        LastMessageAtMs = truncatedConversations[^1].LastMessageAtMs,
                        ConversationId = truncatedConversations[^1].ConversationId
                    }
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
                        : items.Take(k).ToArray());

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
                ResetsRequired = mappedResets
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
