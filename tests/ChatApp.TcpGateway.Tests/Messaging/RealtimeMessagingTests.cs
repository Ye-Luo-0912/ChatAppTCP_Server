using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;
using RealtimeEventConsumerService = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventConsumerService;
using RealtimeEventDispatcher = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventDispatcher;

namespace ChatApp.TcpGateway.Tests.Messaging;

public sealed class RealtimeMessagingTests
{
    [Fact]
    public async Task SessionRevokedEventClosesOnlyMatchingSession()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var revoked = CreateSession(1, metrics);
        await using var retained = CreateSession(2, metrics);
        revoked.Authenticate(42, "revoked-session", deviceIdHash: 1);
        retained.Authenticate(42, "retained-session", deviceIdHash: 2);
        registry.Add(revoked);
        registry.Add(retained);

        var dispatcher = CreateDispatcher(registry, metrics);
        dispatcher.Dispatch(
            new RealtimeEvent
            {
                EventId = "session-revoked-event",
                Type = RealtimeEventType.SessionRevoked,
                TargetUserId = 42,
                SessionId = "revoked-session"
            });

        Assert.False(revoked.IsConnected);
        Assert.Equal(
            SessionCloseReason.SessionRevoked,
            revoked.CloseReason);
        Assert.True(retained.IsConnected);
    }

    [Fact(Timeout = 5_000)]
    public async Task ConsumerAcknowledgesEventAfterDispatch()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        var dispatcher = CreateDispatcher(registry, metrics);
        var messageBus = new SingleEventMessageBus(
            CreateMessageReceivedEvent());
        using var service = new RealtimeEventConsumerService(
            messageBus,
            dispatcher,
            metrics,
            NullLogger<RealtimeEventConsumerService>.Instance);

        await service.StartAsync(CancellationToken.None);
        try
        {
            await messageBus.Acknowledged.Task.WaitAsync(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken);
            Assert.False(messageBus.WasNaked);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    private static RealtimeEventDispatcher CreateDispatcher(
        UserSessionRegistry registry,
        GatewayMetrics metrics) =>
        new(
            registry,
            new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage),
            new JsonPayloadCodec<MessageReceiptUpdate>(
                GatewayJsonSerializerContext.Default.MessageReceiptUpdate),
            new JsonPayloadCodec<ConversationChanged>(
                GatewayJsonSerializerContext.Default.ConversationChanged),
            new JsonPayloadCodec<UnreadCountChanged>(
                GatewayJsonSerializerContext.Default.UnreadCountChanged),
            new JsonPayloadCodec<ConversationReadUpdate>(
                GatewayJsonSerializerContext.Default.ConversationReadUpdate),
            new JsonPayloadCodec<MessageRecalledUpdate>(
                GatewayJsonSerializerContext.Default.MessageRecalledUpdate),
            new JsonPayloadCodec<MessageEditedUpdate>(
                GatewayJsonSerializerContext.Default.MessageEditedUpdate),
            new JsonPayloadCodec<ReactionAddedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionAddedUpdate),
            new JsonPayloadCodec<ReactionRemovedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionRemovedUpdate),
            new JsonPayloadCodec<MemberJoinedUpdate>(
                GatewayJsonSerializerContext.Default.MemberJoinedUpdate),
            new JsonPayloadCodec<MemberLeftUpdate>(
                GatewayJsonSerializerContext.Default.MemberLeftUpdate),
            new JsonPayloadCodec<MemberRemovedUpdate>(
                GatewayJsonSerializerContext.Default.MemberRemovedUpdate),
            new JsonPayloadCodec<RoleChangedUpdate>(
                GatewayJsonSerializerContext.Default.RoleChangedUpdate),
            metrics,
            TimeProvider.System,
            NullLogger<RealtimeEventDispatcher>.Instance);

    private static TcpClientSession CreateSession(
        uint connectionId,
        GatewayMetrics metrics) =>
        new(
            new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp),
            connectionId,
            outboundQueueCapacity: 8,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

    private static RealtimeEvent CreateMessageReceivedEvent() =>
        new()
        {
            EventId = "message-event",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 42,
            ActorUserId = 7,
            MessageId = "message-id",
            SessionId = "sender-session",
            PayloadJson = """
                {
                  "messageId": "message-id",
                  "clientMessageId": "client-message-id",
                  "senderUserId": 7,
                  "senderSessionId": "sender-session",
                  "receiverUserId": 42,
                  "content": "hello",
                  "receivedAtMs": 1784476800000
                }
                """,
            OccurredAtMs = 1784476800000
        };

    private sealed class SingleEventMessageBus(
        RealtimeEvent realtimeEvent) : IRealtimeMessageBus
    {
        public TaskCompletionSource Acknowledged { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public bool WasNaked { get; private set; }

        public Task PublishIncomingMessageAsync(
            ChatApp.Realtime.Abstractions.Messaging.IncomingMessageCommand command,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishMessageReceiptAsync(
            MessageReceiptCommand command,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryPage>
            QueryMessageHistoryAsync(
                ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryQuery query,
                CancellationToken ct = default) =>
            Task.FromResult(
                ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryPage.Failed(
                    query.RequestId,
                    "not_used",
                    "not used"));

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationListPage.Failed(
                    query.RequestId,
                    "not_used",
                    "not used"));

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(
            ConversationMarkReadCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationMarkReadResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
            ConversationSetPrefsCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationSetPrefsResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                GroupConversationResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<MessageRecallResult> RecallMessageAsync(
            MessageRecallCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                MessageRecallResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<MessageEditResult> EditMessageAsync(
            MessageEditCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                MessageEditResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<MessageReactionResult> ReactToMessageAsync(
            MessageReactionCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                MessageReactionResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                SyncBootstrapPage.Failed(
                    query.RequestId,
                    "not_used",
                    "not used"));

        public Task<ChatApp.Realtime.Abstractions.Messaging.History.RealtimeHistoryMessage?>
            TryGetMessageByIdAsync(
                long userId,
                string messageId,
                CancellationToken ct = default) =>
            Task.FromResult<ChatApp.Realtime.Abstractions.Messaging.History.RealtimeHistoryMessage?>(null);

        public Task PublishEventAsync(
            RealtimeEvent evt,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            yield return new RealtimeEventDelivery(
                realtimeEvent,
                ack: _ =>
                {
                    Acknowledged.TrySetResult();
                    return ValueTask.CompletedTask;
                },
                nak: (_, _) =>
                {
                    WasNaked = true;
                    return ValueTask.CompletedTask;
                },
                deliveryCount: 1);

            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task PublishEphemeralTypingAsync(
            ChatApp.Realtime.Integration.Ephemeral.EphemeralTypingEvent evt,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishEphemeralPresenceAsync(
            ChatApp.Realtime.Integration.Ephemeral.EphemeralPresenceEvent evt,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<ChatApp.Realtime.Integration.Ephemeral.EphemeralTypingEvent>
            ConsumeEphemeralTypingAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ChatApp.Realtime.Integration.Ephemeral.EphemeralPresenceEvent>
            ConsumeEphemeralPresenceAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task<ChatApp.Realtime.Integration.Ephemeral.PresenceAuthorizeResponse> AuthorizePresenceAsync(
            ChatApp.Realtime.Integration.Ephemeral.PresenceAuthorizeQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(new ChatApp.Realtime.Integration.Ephemeral.PresenceAuthorizeResponse
            {
                AllowedUserIds = query.TargetUserIds
            });

        public Task ServePresenceAuthorizeAsync(
            Func<ChatApp.Realtime.Integration.Ephemeral.PresenceAuthorizeQuery, CancellationToken,
                ValueTask<ChatApp.Realtime.Integration.Ephemeral.PresenceAuthorizeResponse>> handler,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<TimeSpan> PingAsync(
            CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.FromMilliseconds(1));
    }
}
