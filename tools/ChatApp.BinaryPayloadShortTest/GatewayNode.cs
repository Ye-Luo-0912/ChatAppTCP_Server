using System.Net;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Gateway.Commands.Attachments;
using ChatApp.TcpGateway.Gateway.Commands.Calls;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Commands.Relationships;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Gateway.Serialization;
using ChatApp.TcpGateway.Infrastructure.Authentication;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Infrastructure.Server;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RealtimeHistory =
    ChatApp.Realtime.Abstractions.Messaging.History;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.BinaryPayloadShortTest;

/// <summary>
/// 真实 TcpGatewayService 组装（照抄 tests/ChatApp.TcpGateway.Tests/Networking/
/// BinaryPayloadNegotiationTests.cs 的 handler 图与测试替身），仅把容量/限流参数
/// 调整为适合负载短测的值，其余语义与生产代码一致。
/// </summary>
internal sealed class GatewayNode : IDisposable
{
    private GatewayNode(
        TcpGatewayService service,
        CountingRealtimeMessageBus bus,
        GatewayMetrics metrics)
    {
        Service = service;
        Bus = bus;
        Metrics = metrics;
    }

    public TcpGatewayService Service { get; }

    public CountingRealtimeMessageBus Bus { get; }

    public GatewayMetrics Metrics { get; }

    public void Dispose() => Metrics.Dispose();

    public static GatewayNode Create(int port, bool enableBinaryPayloadFormat)
    {
        var options = new TcpGatewayOptions
        {
            ListenAddress = IPAddress.Loopback.ToString(),
            Port = port,
            ListenBacklog = 32,
            MaxConnections = 64,
            ReceiveBufferSize = 16 * 1024,
            PipePauseWriterThreshold = 256 * 1024,
            PipeResumeWriterThreshold = 128 * 1024,
            OutboundQueueCapacity = 4096,
            MaxOutboundQueuedBytes = 32L * 1024 * 1024,
            AuthenticationTimeout = TimeSpan.FromSeconds(10),
            IdleTimeout = TimeSpan.FromMinutes(30),
            HeartbeatScanInterval = TimeSpan.FromSeconds(1),
            SendTimeout = TimeSpan.FromSeconds(30),
            MaxPacketsPerSecond = 200_000,
            MaxInboundBytesPerSecond = 64L * 1024 * 1024,
            MaxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
            RequireClientHello = true,
            UseActorRuntimeForEphemeralCommands = true,
            InboundTransportMode = InboundTransportMode.Pipelines,
            OutboundSendMode = OutboundSendMode.PersistentSendLoop,
            EnableBinaryPayloadFormat = enableBinaryPayloadFormat,
            GoAwayDrainTimeout = TimeSpan.FromSeconds(5)
        };

        var metrics = new GatewayMetrics();
        var userSessions = new UserSessionRegistry();
        var messageBus = new CountingRealtimeMessageBus();
        var authenticationRequestCodec =
            new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest);
        var authenticationResponseCodec =
            new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse);
        var clientHelloCodec = new JsonPayloadCodec<ClientHello>(
            GatewayJsonSerializerContext.Default.ClientHello);
        var serverHelloCodec = new JsonPayloadCodec<ServerHello>(
            GatewayJsonSerializerContext.Default.ServerHello);
        var protocolErrorCodec = new JsonPayloadCodec<ProtocolErrorFrame>(
            GatewayJsonSerializerContext.Default.ProtocolErrorFrame);
        var resumeResponseCodec = new JsonPayloadCodec<ResumeResponse>(
            GatewayJsonSerializerContext.Default.ResumeResponse);
        var goAwayCodec = new JsonPayloadCodec<GoAway>(
            GatewayJsonSerializerContext.Default.GoAway);
        var chatMessageCodec = new JsonPayloadCodec<ChatMessage>(
            GatewayJsonSerializerContext.Default.ChatMessage);
        var acknowledgementCodec =
            new JsonPayloadCodec<MessageAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageAcknowledgement);
        var receiptRequestCodec =
            new JsonPayloadCodec<MessageReceiptRequest>(
                GatewayJsonSerializerContext.Default.MessageReceiptRequest);
        var receiptAcknowledgementCodec =
            new JsonPayloadCodec<MessageReceiptAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageReceiptAcknowledgement);
        var typingNotifyCodec = new JsonPayloadCodec<TypingNotify>(
            GatewayJsonSerializerContext.Default.TypingNotify);
        var typingUpdateCodec = new JsonPayloadCodec<TypingUpdate>(
            GatewayJsonSerializerContext.Default.TypingUpdate);
        var presenceChangedCodec = new JsonPayloadCodec<PresenceChanged>(
            GatewayJsonSerializerContext.Default.PresenceChanged);

        var integrationOptions = new RealtimeIntegrationOptions
        {
            InstanceId = "binary-shorttest-gateway"
        };
        var globalPresence = new NoopGlobalPresenceStore();
        var presenceWatchers = new PresenceWatcherRegistry();
        var typingFanout = new TypingFanoutCoordinator(TimeProvider.System);
        var pushStore = new InMemoryPushTokenStore();

        var pushHandler = new PushTokenCommandHandler(
            new JsonPayloadCodec<RegisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenRequest),
            new JsonPayloadCodec<RegisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenResponse),
            new JsonPayloadCodec<UnregisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenRequest),
            new JsonPayloadCodec<UnregisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenResponse),
            metrics,
            NullLogger<PushTokenCommandHandler>.Instance,
            pushStore);
        var reactionHandler = new ReactionCommandHandler(
            messageBus,
            new JsonPayloadCodec<AddReactionRequest>(
                GatewayJsonSerializerContext.Default.AddReactionRequest),
            new JsonPayloadCodec<AddReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.AddReactionAcknowledgement),
            new JsonPayloadCodec<RemoveReactionRequest>(
                GatewayJsonSerializerContext.Default.RemoveReactionRequest),
            new JsonPayloadCodec<RemoveReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement),
            metrics,
            TimeProvider.System,
            NullLogger<ReactionCommandHandler>.Instance);
        var messagingHandler = new MessagingCommandHandler(
            messageBus,
            chatMessageCodec,
            acknowledgementCodec,
            receiptRequestCodec,
            receiptAcknowledgementCodec,
            new JsonPayloadCodec<MessageRecallRequest>(
                GatewayJsonSerializerContext.Default.MessageRecallRequest),
            new JsonPayloadCodec<MessageRecallAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageRecallAcknowledgement),
            new JsonPayloadCodec<MessageEditRequest>(
                GatewayJsonSerializerContext.Default.MessageEditRequest),
            new JsonPayloadCodec<MessageEditAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageEditAcknowledgement),
            metrics,
            TimeProvider.System,
            NullLogger<MessagingCommandHandler>.Instance,
            Options.Create(options));
        var historyQueryHandler = new HistoryQueryCommandHandler(
            messageBus,
            new JsonPayloadCodec<MessageHistoryRequest>(
                GatewayJsonSerializerContext.Default.MessageHistoryRequest),
            new JsonPayloadCodec<MessageHistoryResponse>(
                GatewayJsonSerializerContext.Default.MessageHistoryResponse),
            new JsonPayloadCodec<MessageHistoryItem[]>(
                GatewayJsonSerializerContext.Default.MessageHistoryItemArray),
            new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest),
            new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse),
            new JsonPayloadCodec<ChatApp.Realtime.Abstractions.Conversations.ConversationListItem[]>(
                GatewayJsonSerializerContext.Default.ConversationListItemArray),
            new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest),
            new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse),
            metrics,
            NullLogger<HistoryQueryCommandHandler>.Instance);
        var conversationPrefsHandler = new ConversationPrefsCommandHandler(
            messageBus,
            new JsonPayloadCodec<ConversationMarkReadRequest>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadRequest),
            new JsonPayloadCodec<ConversationMarkReadResponse>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadResponse),
            new JsonPayloadCodec<ConversationSetPrefsRequest>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsRequest),
            new JsonPayloadCodec<ConversationSetPrefsResponse>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsResponse),
            metrics,
            NullLogger<ConversationPrefsCommandHandler>.Instance);
        var groupHandler = new GroupCommandHandler(
            messageBus,
            new JsonPayloadCodec<CreateGroupRequest>(
                GatewayJsonSerializerContext.Default.CreateGroupRequest),
            new JsonPayloadCodec<CreateGroupResponse>(
                GatewayJsonSerializerContext.Default.CreateGroupResponse),
            new JsonPayloadCodec<AddGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.AddGroupMembersRequest),
            new JsonPayloadCodec<AddGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.AddGroupMembersResponse),
            new JsonPayloadCodec<RemoveGroupMemberRequest>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberRequest),
            new JsonPayloadCodec<RemoveGroupMemberResponse>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberResponse),
            new JsonPayloadCodec<LeaveGroupRequest>(
                GatewayJsonSerializerContext.Default.LeaveGroupRequest),
            new JsonPayloadCodec<LeaveGroupResponse>(
                GatewayJsonSerializerContext.Default.LeaveGroupResponse),
            new JsonPayloadCodec<ChangeMemberRoleRequest>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleRequest),
            new JsonPayloadCodec<ChangeMemberRoleResponse>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleResponse),
            new JsonPayloadCodec<ListGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.ListGroupMembersRequest),
            new JsonPayloadCodec<ListGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.ListGroupMembersResponse),
            new JsonPayloadCodec<MessageReadReceiptQueryRequest>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryRequest),
            new JsonPayloadCodec<MessageReadReceiptQueryResponse>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryResponse),
            new JsonPayloadCodec<DissolveGroupRequest>(
                GatewayJsonSerializerContext.Default.DissolveGroupRequest),
            new JsonPayloadCodec<DissolveGroupResponse>(
                GatewayJsonSerializerContext.Default.DissolveGroupResponse),
            metrics,
            NullLogger<GroupCommandHandler>.Instance);
        var typingHandler = new TypingCommandHandler(
            typingNotifyCodec,
            typingFanout,
            directConversationAuthorizer: null,
            Options.Create(options),
            NullLogger<TypingCommandHandler>.Instance);
        var presenceHandler = new PresenceCommandHandler(
            Options.Create(options),
            messageBus,
            integrationOptions,
            globalPresence,
            userSessions,
            presenceWatchers,
            NullWatcherGatewayDirectory.Instance,
            new JsonPayloadCodec<PresenceQueryRequest>(
                GatewayJsonSerializerContext.Default.PresenceQueryRequest),
            new JsonPayloadCodec<PresenceUnwatchRequest>(
                GatewayJsonSerializerContext.Default.PresenceUnwatchRequest),
            new JsonPayloadCodec<PresenceSnapshotResponse>(
                GatewayJsonSerializerContext.Default.PresenceSnapshotResponse),
            metrics,
            NullLogger<PresenceCommandHandler>.Instance);
        var attachmentHandler = new AttachmentCommandHandler(
            new StubAttachmentBackend(NullLogger<StubAttachmentBackend>.Instance),
            new JsonPayloadCodec<AttachmentFinalizeRequest>(
                GatewayJsonSerializerContext.Default.AttachmentFinalizeRequest),
            new JsonPayloadCodec<AttachmentFinalizeResponse>(
                GatewayJsonSerializerContext.Default.AttachmentFinalizeResponse),
            new JsonPayloadCodec<AttachmentDownloadAuthorizeRequest>(
                GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeRequest),
            new JsonPayloadCodec<AttachmentDownloadAuthorizeResponse>(
                GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeResponse),
            metrics,
            NullLogger<AttachmentCommandHandler>.Instance);
        var relationshipHandler = new RelationshipCommandHandler(
            new StubRelationshipBackend(NullLogger<StubRelationshipBackend>.Instance),
            new JsonPayloadCodec<RelationshipCommandRequest>(
                GatewayJsonSerializerContext.Default.RelationshipCommandRequest),
            new JsonPayloadCodec<RelationshipCommandResponse>(
                GatewayJsonSerializerContext.Default.RelationshipCommandResponse),
            new JsonPayloadCodec<TcpRelationshipListRequest>(
                GatewayJsonSerializerContext.Default.TcpRelationshipListRequest),
            new JsonPayloadCodec<TcpRelationshipListResponse>(
                GatewayJsonSerializerContext.Default.TcpRelationshipListResponse),
            metrics,
            NullLogger<RelationshipCommandHandler>.Instance);
        var callHandler = new CallCommandHandler(
            new StubCallBackend(NullLogger<StubCallBackend>.Instance),
            new GroupCallSignalRelay(
                Microsoft.Extensions.Options.Options.Create(new GroupCallGrantOptions()),
                TimeProvider.System,
                NullLogger<GroupCallSignalRelay>.Instance),
            new JsonPayloadCodec<TcpCallCommandRequest>(
                GatewayJsonSerializerContext.Default.TcpCallCommandRequest),
            new JsonPayloadCodec<TcpCallCommandResponse>(
                GatewayJsonSerializerContext.Default.TcpCallCommandResponse),
            new JsonPayloadCodec<TcpCallSignal>(
                GatewayJsonSerializerContext.Default.TcpCallSignal),
            userSessions,
            metrics,
            NullLogger<CallCommandHandler>.Instance);

        var commandDispatcher = new CommandDispatcher(
            pushHandler,
            reactionHandler,
            messagingHandler,
            historyQueryHandler,
            conversationPrefsHandler,
            groupHandler,
            typingHandler,
            presenceHandler,
            attachmentHandler,
            relationshipHandler,
            callHandler);

        var service = new TcpGatewayService(
            Options.Create(options),
            new FakeAuthenticator(),
            authenticationRequestCodec,
            authenticationResponseCodec,
            acknowledgementCodec,
            typingUpdateCodec,
            presenceChangedCodec,
            messageBus,
            integrationOptions,
            new NoopDeviceSessionLeaseStore(),
            globalPresence,
            userSessions,
            presenceWatchers,
            typingFanout,
            metrics,
            TimeProvider.System,
            NullLogger<TcpGatewayService>.Instance,
            NullLogger<TcpClientSession>.Instance,
            serverIdentity: new ServerIdentity(
                "00000000000000000000000000000001"),
            resumeTokenStore: null,
            clientHelloCodec: clientHelloCodec,
            serverHelloCodec: serverHelloCodec,
            goAwayCodec: goAwayCodec,
            resumeResponseCodec: resumeResponseCodec,
            protocolErrorFrameCodec: protocolErrorCodec,
            commandDispatcher: commandDispatcher);

        return new GatewayNode(service, messageBus, metrics);
    }

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(
                RealtimeAuthenticationResult.Success(
                    userId: 42,
                    sessionId: "binary-shorttest",
                    userName: "shorttest-user",
                    deviceIdHash,
                    roles: []));
    }

    private sealed class NoopGlobalPresenceStore : IGlobalPresenceStore
    {
        public Task<PresenceTransition> SetOnlineAsync(long userId, string instanceId, CancellationToken ct = default) =>
            Task.FromResult(PresenceTransition.None);

        public Task<PresenceTransition> SetOfflineAsync(long userId, string instanceId, CancellationToken ct = default) =>
            Task.FromResult(PresenceTransition.None);

        public Task RefreshOnlineAsync(long userId, string instanceId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(false);

        public Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
            IReadOnlyList<long> userIds,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyDictionary<long, bool>>(
                userIds.ToDictionary(static id => id, static _ => false));

        public Task RunMaintenanceAsync(CancellationToken ct = default) =>
            Task.CompletedTask;
    }

    /// <summary>
    /// 捕获型 IRealtimeMessageBus（照抄测试的 CapturingRealtimeMessageBus），
    /// 额外计数 PublishIncomingMessageAsync 作为网关侧解析正确性的旁证。
    /// </summary>
    internal sealed class CountingRealtimeMessageBus : IRealtimeMessageBus
    {
        private long _publishedIncomingCount;

        public long PublishedIncomingCount => Interlocked.Read(ref _publishedIncomingCount);

        public IncomingMessageCommand? LastIncomingMessage { get; private set; }

        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command,
            CancellationToken ct = default)
        {
            Interlocked.Increment(ref _publishedIncomingCount);
            LastIncomingMessage = command;
            return Task.CompletedTask;
        }

        public Task PublishMessageReceiptAsync(
            MessageReceiptCommand command,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<RealtimeHistory.MessageHistoryPage> QueryMessageHistoryAsync(
            RealtimeHistory.MessageHistoryQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                RealtimeHistory.MessageHistoryPage.Success(
                    "downstream", [], nextCursor: null, hasMore: false));

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationListPage.Success(
                    query.RequestId, [], nextCursor: null, hasMore: false));

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(
            ConversationMarkReadCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationMarkReadResult.Success(
                    command.RequestId,
                    command.ConversationId,
                    unreadCount: 0,
                    lastReadMessageId: command.ReadMessageId,
                    lastReadAtMs: command.ReadAtMs,
                    changed: true));

        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
            ConversationSetPrefsCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationSetPrefsResult.Success(
                    command.RequestId,
                    command.ConversationId,
                    isPinned: false,
                    isMuted: false,
                    mutedUntilMs: null,
                    changed: false));

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                GroupConversationResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                GroupConversationResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(
            AttachmentFinalizeCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(AttachmentFinalizeResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<AttachmentDownloadAuthorizeResult> AuthorizeAttachmentDownloadAsync(
            AttachmentDownloadAuthorizeCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<RelationshipCommandResult> MutateRelationshipAsync(
            RelationshipCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(RelationshipCommandResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipListResult> QueryRelationshipListAsync(
            RelationshipListQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(RelationshipListResult.Failed(query.RequestId, "x", "x"));

        public Task<MessageRecallResult> RecallMessageAsync(
            MessageRecallCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                MessageRecallResult.Success(
                    command.RequestId,
                    command.MessageId,
                    conversationId: null,
                    recalledAtMs: command.OccurredAtMs));

        public Task<MessageEditResult> EditMessageAsync(
            MessageEditCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                MessageEditResult.Success(
                    command.RequestId,
                    command.MessageId,
                    conversationId: null,
                    content: command.Content,
                    editVersion: 2,
                    editedAtMs: command.OccurredAtMs));

        public Task<MessageReactionResult> ReactToMessageAsync(
            MessageReactionCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                MessageReactionResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                SyncBootstrapPage.Success(
                    "downstream",
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    [],
                    conversationsNextCursor: null,
                    conversationsHasMore: false,
                    catchUps: []));

        public Task<RealtimeHistory.RealtimeHistoryMessage?> TryGetMessageByIdAsync(
            long userId,
            string messageId,
            CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistory.RealtimeHistoryMessage?>(null);

        public Task<CallProcessResult> SendCallCommandAsync(
            CallCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(CallProcessResult.Failed(CallErrorCode.StateStoreUnavailable, "unavailable"));

        public Task PublishEventAsync(
            RealtimeEvent evt,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
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
            ConsumeEphemeralTypingAsync(
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ChatApp.Realtime.Integration.Ephemeral.EphemeralPresenceEvent>
            ConsumeEphemeralPresenceAsync(
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
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

        public Task PublishPushDeliveryAsync(PushDeliveryCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<PushDelivery> ConsumePushDeliveriesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task<TimeSpan> PingAsync(
            CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}
