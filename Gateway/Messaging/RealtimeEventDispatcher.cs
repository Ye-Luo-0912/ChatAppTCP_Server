using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Messaging.Realtime;
using ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging;

/// <summary>
/// Realtime 事件分发门面：将 <see cref="RealtimeEvent"/> 路由到注册的
/// <see cref="IRealtimeEventHandler"/>。不支持的事件类型走默认日志路径。
/// <para>
/// 16 个事件类型的实际处理逻辑已抽取到 <c>Gateway/Messaging/Realtime/Handlers/</c>
/// 下的 9 个独立 handler，共享 <see cref="RealtimeEventDeliveryHelper"/>、
/// <see cref="RealtimeEventRejectionSink"/> 与 <see cref="RealtimeTimestampConverter"/>。
/// </para>
/// <para>
/// 构造函数保留为"组合根"：测试场景下可直接 new 出实例，无需配置 DI。
/// 生产路径可在 <c>Program.cs</c> 中改为注册各个 handler 单例并通过
/// <see cref="RealtimeEventHandlerRegistry"/> 注入。
/// </para>
/// </summary>
internal sealed class RealtimeEventDispatcher
{
    private readonly RealtimeEventHandlerRegistry _registry;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<RealtimeEventDispatcher> _logger;

    public RealtimeEventDispatcher(
        UserSessionRegistry userSessions,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageReceiptUpdate> messageReceiptUpdateCodec,
        IPayloadCodec<ConversationChanged> conversationChangedCodec,
        IPayloadCodec<UnreadCountChanged> unreadCountChangedCodec,
        IPayloadCodec<ConversationReadUpdate> conversationReadUpdateCodec,
        IPayloadCodec<MessageRecalledUpdate> messageRecalledUpdateCodec,
        IPayloadCodec<MessageEditedUpdate> messageEditedUpdateCodec,
        IPayloadCodec<ReactionAddedUpdate> reactionAddedUpdateCodec,
        IPayloadCodec<ReactionRemovedUpdate> reactionRemovedUpdateCodec,
        IPayloadCodec<MemberJoinedUpdate> memberJoinedUpdateCodec,
        IPayloadCodec<MemberLeftUpdate> memberLeftUpdateCodec,
        IPayloadCodec<MemberRemovedUpdate> memberRemovedUpdateCodec,
        IPayloadCodec<RoleChangedUpdate> roleChangedUpdateCodec,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<RealtimeEventDispatcher> logger,
        IPayloadCodec<RelationshipListChangedUpdate>? relationshipListChangedCodec = null,
        IPayloadCodec<AttachmentLifecycleUpdate>? attachmentLifecycleCodec = null)
        : this(
            BuildRegistry(
                userSessions,
                metrics,
                timeProvider,
                logger,
                chatMessageCodec,
                messageReceiptUpdateCodec,
                conversationChangedCodec,
                unreadCountChangedCodec,
                conversationReadUpdateCodec,
                messageRecalledUpdateCodec,
                messageEditedUpdateCodec,
                reactionAddedUpdateCodec,
                reactionRemovedUpdateCodec,
                memberJoinedUpdateCodec,
                memberLeftUpdateCodec,
                memberRemovedUpdateCodec,
                roleChangedUpdateCodec,
                relationshipListChangedCodec,
                attachmentLifecycleCodec),
            metrics,
            logger)
    {
    }

    /// <summary>
    /// 生产路径构造函数：直接注入已构建的 <see cref="RealtimeEventHandlerRegistry"/>。
    /// </summary>
    public RealtimeEventDispatcher(
        RealtimeEventHandlerRegistry registry,
        GatewayMetrics metrics,
        ILogger<RealtimeEventDispatcher> logger)
    {
        _registry = registry;
        _metrics = metrics;
        _logger = logger;
    }

    public void Dispatch(RealtimeEvent realtimeEvent)
    {
        ArgumentNullException.ThrowIfNull(realtimeEvent);

        if (_registry.TryGet(realtimeEvent.Type, out var handler))
        {
            handler.Handle(realtimeEvent);
            return;
        }

        // 不支持的事件类型：保留原日志路径与 0 入队指标，避免静默丢弃。
        _metrics.RealtimeEventHandled(queuedDeliveries: 0);
        _logger.RealtimeEventUnsupported(
            realtimeEvent.EventId,
            realtimeEvent.Type.ToString());
    }

    /// <summary>
    /// 组合根：根据传入的 codec 与共享端口构建 9 个 handler 并注册到 registry。
    /// 仅在"组合根构造函数"中调用一次；生产路径应直接注册 handler 单例。
    /// </summary>
    private static RealtimeEventHandlerRegistry BuildRegistry(
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageReceiptUpdate> messageReceiptUpdateCodec,
        IPayloadCodec<ConversationChanged> conversationChangedCodec,
        IPayloadCodec<UnreadCountChanged> unreadCountChangedCodec,
        IPayloadCodec<ConversationReadUpdate> conversationReadUpdateCodec,
        IPayloadCodec<MessageRecalledUpdate> messageRecalledUpdateCodec,
        IPayloadCodec<MessageEditedUpdate> messageEditedUpdateCodec,
        IPayloadCodec<ReactionAddedUpdate> reactionAddedUpdateCodec,
        IPayloadCodec<ReactionRemovedUpdate> reactionRemovedUpdateCodec,
        IPayloadCodec<MemberJoinedUpdate> memberJoinedUpdateCodec,
        IPayloadCodec<MemberLeftUpdate> memberLeftUpdateCodec,
        IPayloadCodec<MemberRemovedUpdate> memberRemovedUpdateCodec,
        IPayloadCodec<RoleChangedUpdate> roleChangedUpdateCodec,
        IPayloadCodec<RelationshipListChangedUpdate>? relationshipListChangedCodec,
        IPayloadCodec<AttachmentLifecycleUpdate>? attachmentLifecycleCodec)
    {
        var delivery = new RealtimeEventDeliveryHelper(userSessions, metrics);
        // 复用 dispatcher 的 logger：原 RejectEvent 也走该 logger，保持日志类别一致。
        var rejection = new RealtimeEventRejectionSink(metrics, logger);
        var timestamp = new RealtimeTimestampConverter(timeProvider);

        IRealtimeEventHandler chatMessage = new ChatMessageDeliveryHandler(
            chatMessageCodec, delivery, rejection, timestamp);
        IRealtimeEventHandler messageReceipt = new MessageReceiptHandler(
            messageReceiptUpdateCodec, delivery, rejection, timestamp);
        IRealtimeEventHandler messageLifecycle = new MessageLifecycleEventHandler(
            messageRecalledUpdateCodec, messageEditedUpdateCodec, delivery, rejection);
        IRealtimeEventHandler reaction = new ReactionEventHandler(
            reactionAddedUpdateCodec, reactionRemovedUpdateCodec, delivery, rejection);
        IRealtimeEventHandler conversationNotification = new ConversationNotificationHandler(
            conversationChangedCodec, unreadCountChangedCodec, conversationReadUpdateCodec,
            delivery, rejection);
        IRealtimeEventHandler member = new ConversationMemberEventHandler(
            memberJoinedUpdateCodec, memberLeftUpdateCodec, memberRemovedUpdateCodec,
            roleChangedUpdateCodec, delivery, rejection);
        IRealtimeEventHandler relationshipList = new RelationshipListHandler(
            relationshipListChangedCodec, delivery, rejection, metrics);
        IRealtimeEventHandler attachmentLifecycle = new AttachmentLifecycleHandler(
            attachmentLifecycleCodec, delivery, rejection, metrics);
        IRealtimeEventHandler sessionRevocation = new SessionRevocationHandler(
            userSessions, metrics, rejection);

        return new RealtimeEventHandlerRegistry(new KeyValuePair<RealtimeEventType, IRealtimeEventHandler>[]
        {
            new(RealtimeEventType.MessageReceived, chatMessage),
            new(RealtimeEventType.MessageReceiptUpdated, messageReceipt),
            new(RealtimeEventType.MessageRecalled, messageLifecycle),
            new(RealtimeEventType.MessageEdited, messageLifecycle),
            new(RealtimeEventType.ReactionAdded, reaction),
            new(RealtimeEventType.ReactionRemoved, reaction),
            new(RealtimeEventType.ConversationListChanged, conversationNotification),
            new(RealtimeEventType.UnreadCountChanged, conversationNotification),
            new(RealtimeEventType.ConversationRead, conversationNotification),
            new(RealtimeEventType.MemberJoined, member),
            new(RealtimeEventType.MemberLeft, member),
            new(RealtimeEventType.MemberRemoved, member),
            new(RealtimeEventType.RoleChanged, member),
            new(RealtimeEventType.FriendRequestListChanged, relationshipList),
            new(RealtimeEventType.FriendListChanged, relationshipList),
            new(RealtimeEventType.BlockedListChanged, relationshipList),
            new(RealtimeEventType.AttachmentLifecycleChanged, attachmentLifecycle),
            new(RealtimeEventType.SessionRevoked, sessionRevocation),
        });
    }
}
