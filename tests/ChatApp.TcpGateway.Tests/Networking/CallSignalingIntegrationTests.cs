using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
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
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.Shared.Protocol.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RealtimeHistory = ChatApp.Realtime.Abstractions.Messaging.History;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// CALL-E2E-2 通话控制面端到端验证：主叫/被叫两条真实 TCP 连接。
/// <para>
/// 验证：Gate成 producer 把上层 CallCommandRequest 映射为 TCP wire；Realtime 返回成功状态与
/// 需转发的对端信号时，主叫收到 <c>CallCommandResponse</c>，被叫收到 <c>CallSignal</c>。
/// 同时验证请求参数校验失败（空 command id）返回 <c>call_bad_request</c> 且不调用后端。
/// </para>
/// </summary>
public sealed class CallSignalingIntegrationTests
{
    [Fact(Timeout = 15_000)]
    public async Task Invite_ResponseReturned_And_SignalForwardedToCallee_OverTcp()
    {
        var scriptedBackend = new ScriptedCallBackend();
        scriptedBackend.SetResult("call-1", TcpCallState.Ringing, signalToForward:
            new TcpCallSignal
            {
                SignalId = "sig-invite-1",
                CallId = "call-1",
                FromUserId = 42,
                ToUserId = 43,
                Kind = TcpCallCommandType.Invite,
                Sdp = "v=0\r\no=caller 1 1 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 40000 RTP/AVP 0\r\n",
                Revision = 1,
                OccurredAtMs = 1_900_000_000_000L
            });

        await using var harness = await CallHarness.StartAsync(scriptedBackend);

        // 主叫连接（user 42）与被叫连接（user 43）都注册到 UserSessionRegistry。
        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);

        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();

        await harness.AuthenticateAsync(callerStream, "caller-token", 42);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);

        // 主叫发起 invite。
        var requestId = Guid.CreateVersion7().ToString("N");
        await harness.WriteCallCommandAsync(
            callerStream,
            new TcpCallCommandRequest
            {
                RequestId = requestId,
                CommandId = "cmd-invite-1",
                CallId = "call-1",
                Type = TcpCallCommandType.Invite,
                ActorUserId = 42,
                Revision = 1,
                Grant = new TcpCallGrant { CallId = "call-1", CallerUserId = 42, CalleeUserId = 43 },
                Sdp = "v=0\r\no=caller 1 1 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 40000 RTP/AVP 0\r\n",
                ClientOccurredAtMs = 1_900_000_000_000L
            });

        // 主叫收到成功响应。
        var responseFrame = await harness.ReadFrameAsync(callerStream);
        Assert.Equal(PacketCommand.CallCommandResponse, responseFrame.Command);
        var response = CallHarness.DeserializeResponse(responseFrame.Payload);
        Assert.NotNull(response);
        Assert.True(response.Succeeded);
        Assert.Equal(requestId, response.RequestId);
        Assert.Equal("call-1", response.CallId);
        Assert.Equal(TcpCallState.Ringing, response.State);
        Assert.Equal(1L, response.Revision);
        Assert.Null(response.ErrorCode);

        // 被叫收到转发信令。
        var signalFrame = await harness.ReadFrameAsync(calleeStream);
        Assert.Equal(PacketCommand.CallSignal, signalFrame.Command);
        var signal = CallHarness.DeserializeSignal(signalFrame.Payload);
        Assert.NotNull(signal);
        Assert.Equal("sig-invite-1", signal.SignalId);
        Assert.Equal("call-1", signal.CallId);
        Assert.Equal(42L, signal.FromUserId);
        Assert.Equal(43L, signal.ToUserId);
        Assert.Equal(TcpCallCommandType.Invite, signal.Kind);
        Assert.Contains("o=caller", signal.Sdp);

        // 后端收到的参数映射正确：Actor 身份来自会话（可信），而非请求体。
        Assert.Equal("cmd-invite-1", scriptedBackend.LastCommandId);
        Assert.Equal("call-1", scriptedBackend.LastCallId);
        Assert.Equal(TcpCallCommandType.Invite, scriptedBackend.LastType);
        Assert.Equal(42L, scriptedBackend.LastActorUserId);
        Assert.Equal(1L, scriptedBackend.LastRevision);
    }

    [Fact(Timeout = 15_000)]
    public async Task InvalidCommandId_BadRequest_BackendNotCalled()
    {
        var scriptedBackend = new ScriptedCallBackend();
        await using var harness = await CallHarness.StartAsync(scriptedBackend);

        using var caller = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", 42);

        var requestId = Guid.CreateVersion7().ToString("N");
        await harness.WriteCallCommandAsync(
            callerStream,
            new TcpCallCommandRequest
            {
                RequestId = requestId,
                CommandId = "   ",
                CallId = "call-1",
                Type = TcpCallCommandType.Invite,
                ActorUserId = 42,
                Revision = 1
            });

        var responseFrame = await harness.ReadFrameAsync(callerStream);
        Assert.Equal(PacketCommand.CallCommandResponse, responseFrame.Command);
        var response = CallHarness.DeserializeResponse(responseFrame.Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.BadRequest, response.ErrorCode);
        Assert.False(scriptedBackend.Called);
    }

    /// <summary>可编程通话后端：记录收到的命令，返回可配置结果。</summary>
    private sealed class ScriptedCallBackend : ICallBackend
    {
        private CallCommandBackendResult _result = CallCommandBackendResult.Failed(
            "req", "call", TcpCallErrorCode.StateStoreUnavailable, "unavailable");

        public bool Called { get; private set; }
        public string? LastCommandId { get; private set; }
        public string? LastCallId { get; private set; }
        public TcpCallCommandType LastType { get; private set; }
        public long LastActorUserId { get; private set; }
        public long LastRevision { get; private set; }

        public void SetResult(
            string callId,
            TcpCallState state,
            TcpCallSignal? signalToForward = null) =>
            _result = new CallCommandBackendResult(
                RequestId: "req",
                CallId: callId,
                Succeeded: true,
                ErrorCode: null,
                ErrorMessage: null,
                State: state,
                EndReason: TcpCallEndReason.None,
                Revision: 1,
                Replayed: false,
                SignalToForward: signalToForward);

        public Task<CallCommandBackendResult> SendCommandAsync(
            string requestId,
            long actorUserId,
            string actorSessionId,
            string commandId,
            string callId,
            TcpCallCommandType type,
            long revision,
            TcpCallGrant? grant,
            string? sdp,
            long clientOccurredAtMs,
            CancellationToken cancellationToken = default)
        {
            Called = true;
            LastCommandId = commandId;
            LastCallId = callId;
            LastType = type;
            LastActorUserId = actorUserId;
            LastRevision = revision;
            return Task.FromResult(_result with { RequestId = requestId, CallId = callId });
        }
    }

    /// <summary>按 access token 分配不同用户身份的认证器。</summary>
    private sealed class TokenAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default)
        {
            var (userId, sessionId) = accessToken switch
            {
                "caller-token" => (42L, "caller-session"),
                "callee-token" => (43L, "callee-session"),
                _ => (0L, "unknown-session")
            };

            return ValueTask.FromResult(
                RealtimeAuthenticationResult.Success(
                    userId: userId,
                    sessionId: sessionId,
                    userName: accessToken == "caller-token" ? "caller" : "callee",
                    deviceIdHash,
                    roles: []));
        }
    }

    /// <summary>端到端 TCP 测试夹具：装配 TcpGatewayService 与全部 handler。</summary>
    private sealed class CallHarness : IAsyncDisposable
    {
        private readonly TcpGatewayService _service;

        private CallHarness(TcpGatewayService service, int port, CancellationToken token)
        {
            _service = service;
            Port = port;
            Token = token;
        }

        public int Port { get; }
        public CancellationToken Token { get; }

        public static async Task<CallHarness> StartAsync(ScriptedCallBackend backend)
        {
            var port = ReserveLoopbackPort();
            var options = new TcpGatewayOptions
            {
                ListenAddress = IPAddress.Loopback.ToString(),
                Port = port,
                ListenBacklog = 8,
                MaxConnections = 8,
                ReceiveBufferSize = 1024,
                PipePauseWriterThreshold = 32 * 1024,
                PipeResumeWriterThreshold = 16 * 1024,
                OutboundQueueCapacity = 8,
                MaxOutboundQueuedBytes = 128 * 1024,
                AuthenticationTimeout = TimeSpan.FromSeconds(2),
                IdleTimeout = TimeSpan.FromSeconds(10),
                HeartbeatScanInterval = TimeSpan.FromMilliseconds(200),
                SendTimeout = TimeSpan.FromSeconds(2),
                MaxPacketsPerSecond = 40,
                MaxInboundBytesPerSecond = 256 * 1024,
                MaxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
                RequireClientHello = false
            };

            var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var metrics = new GatewayMetrics();
            var userSessions = new UserSessionRegistry();
            var messageBus = new NoopCallMessageBus();
            var pushStore = new InMemoryPushTokenStore();

            var authenticationRequestCodec = new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest);
            var authenticationResponseCodec = new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse);
            var chatMessageCodec = new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage);
            var acknowledgementCodec = new JsonPayloadCodec<MessageAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageAcknowledgement);
            var receiptRequestCodec = new JsonPayloadCodec<MessageReceiptRequest>(
                GatewayJsonSerializerContext.Default.MessageReceiptRequest);
            var receiptAcknowledgementCodec = new JsonPayloadCodec<MessageReceiptAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageReceiptAcknowledgement);
            var historyRequestCodec = new JsonPayloadCodec<MessageHistoryRequest>(
                GatewayJsonSerializerContext.Default.MessageHistoryRequest);
            var historyResponseCodec = new JsonPayloadCodec<MessageHistoryResponse>(
                GatewayJsonSerializerContext.Default.MessageHistoryResponse);
            var historyItemCodec = new JsonPayloadCodec<MessageHistoryItem[]>(
                GatewayJsonSerializerContext.Default.MessageHistoryItemArray);
            var conversationListRequestCodec = new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest);
            var conversationListResponseCodec = new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse);
            var conversationListItemCodec = new JsonPayloadCodec<ConversationListItem[]>(
                GatewayJsonSerializerContext.Default.ConversationListItemArray);
            var conversationMarkReadRequestCodec = new JsonPayloadCodec<ConversationMarkReadRequest>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadRequest);
            var conversationMarkReadResponseCodec = new JsonPayloadCodec<ConversationMarkReadResponse>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadResponse);
            var conversationSetPrefsRequestCodec = new JsonPayloadCodec<ConversationSetPrefsRequest>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsRequest);
            var conversationSetPrefsResponseCodec = new JsonPayloadCodec<ConversationSetPrefsResponse>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsResponse);
            var messageRecallRequestCodec = new JsonPayloadCodec<MessageRecallRequest>(
                GatewayJsonSerializerContext.Default.MessageRecallRequest);
            var messageRecallAcknowledgementCodec = new JsonPayloadCodec<MessageRecallAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageRecallAcknowledgement);
            var messageEditRequestCodec = new JsonPayloadCodec<MessageEditRequest>(
                GatewayJsonSerializerContext.Default.MessageEditRequest);
            var messageEditAcknowledgementCodec = new JsonPayloadCodec<MessageEditAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageEditAcknowledgement);
            var syncBootstrapRequestCodec = new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest);
            var syncBootstrapResponseCodec = new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse);

            var integrationOptions = new RealtimeIntegrationOptions { InstanceId = "call-signaling-test" };
            var globalPresence = new NoopGlobalPresenceStore();
            var presenceWatchers = new PresenceWatcherRegistry();
            var typingFanout = new TypingFanoutCoordinator(TimeProvider.System);

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
                messageRecallRequestCodec,
                messageRecallAcknowledgementCodec,
                messageEditRequestCodec,
                messageEditAcknowledgementCodec,
                metrics,
                TimeProvider.System,
                NullLogger<MessagingCommandHandler>.Instance,
                Options.Create(options));

            var historyQueryHandler = new HistoryQueryCommandHandler(
                messageBus,
                historyRequestCodec,
                historyResponseCodec,
                historyItemCodec,
                conversationListRequestCodec,
                conversationListResponseCodec,
                conversationListItemCodec,
                syncBootstrapRequestCodec,
                syncBootstrapResponseCodec,
                metrics,
                NullLogger<HistoryQueryCommandHandler>.Instance);

            var conversationPrefsHandler = new ConversationPrefsCommandHandler(
                messageBus,
                conversationMarkReadRequestCodec,
                conversationMarkReadResponseCodec,
                conversationSetPrefsRequestCodec,
                conversationSetPrefsResponseCodec,
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
                new JsonPayloadCodec<TypingNotify>(
                    GatewayJsonSerializerContext.Default.TypingNotify),
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
                backend,
                new JsonPayloadCodec<TcpCallCommandRequest>(
                    GatewayJsonSerializerContext.Default.TcpCallCommandRequest),
                new JsonPayloadCodec<TcpCallCommandResponse>(
                    GatewayJsonSerializerContext.Default.TcpCallCommandResponse),
                new JsonPayloadCodec<TcpCallSignal>(
                    GatewayJsonSerializerContext.Default.TcpCallSignal),
                userSessions,
                metrics,
                NullLogger<CallCommandHandler>.Instance);

            var dispatcher = new CommandDispatcher(
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
                new TokenAuthenticator(),
                authenticationRequestCodec,
                authenticationResponseCodec,
                acknowledgementCodec,
                new JsonPayloadCodec<TypingUpdate>(
                    GatewayJsonSerializerContext.Default.TypingUpdate),
                new JsonPayloadCodec<PresenceChanged>(
                    GatewayJsonSerializerContext.Default.PresenceChanged),
                messageBus,
                integrationOptions,
                new NoopLeaseStore(),
                globalPresence,
                userSessions,
                presenceWatchers,
                typingFanout,
                metrics,
                TimeProvider.System,
                NullLogger<TcpGatewayService>.Instance,
                NullLogger<TcpClientSession>.Instance,
                commandDispatcher: dispatcher);

            await service.StartAsync(CancellationToken.None);
            return new CallHarness(service, port, timeout.Token);
        }

        public async Task AuthenticateAsync(Stream stream, string token, long expectedUserId)
        {
            await WriteFrameAsync(
                stream,
                PacketCommand.AuthenticationRequest,
                new JsonPayloadCodec<AuthenticationRequest>(
                    GatewayJsonSerializerContext.Default.AuthenticationRequest),
                new AuthenticationRequest { AccessToken = token, DeviceIdHash = 7 },
                Token);

            var frame = await ReadFrameAsync(stream);
            Assert.Equal(PacketCommand.AuthenticationResponse, frame.Command);
            var response = new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse)
                .Deserialize(new ReadOnlySequence<byte>(frame.Payload));
            Assert.NotNull(response);
            Assert.True(response.Success);
            Assert.Equal(expectedUserId, response.UserId);
        }

        public Task WriteCallCommandAsync(Stream stream, TcpCallCommandRequest request) =>
            WriteFrameAsync(
                stream,
                PacketCommand.CallCommandRequest,
                new JsonPayloadCodec<TcpCallCommandRequest>(
                    GatewayJsonSerializerContext.Default.TcpCallCommandRequest),
                request,
                Token).AsTask();

        public static TcpCallCommandResponse? DeserializeResponse(byte[] payload) =>
            new JsonPayloadCodec<TcpCallCommandResponse>(
                GatewayJsonSerializerContext.Default.TcpCallCommandResponse)
                .Deserialize(new ReadOnlySequence<byte>(payload));

        public static TcpCallSignal? DeserializeSignal(byte[] payload) =>
            new JsonPayloadCodec<TcpCallSignal>(
                GatewayJsonSerializerContext.Default.TcpCallSignal)
                .Deserialize(new ReadOnlySequence<byte>(payload));

        public ValueTask<ReceivedFrame> ReadFrameAsync(Stream stream) =>
            ReadFrameInternalAsync(stream, Token);

        public async ValueTask DisposeAsync()
        {
            await _service.StopAsync(CancellationToken.None);
        }

        private static async ValueTask WriteFrameAsync<T>(
            Stream stream,
            PacketCommand command,
            JsonPayloadCodec<T> codec,
            T value,
            CancellationToken cancellationToken)
        {
            var payload = new ArrayBufferWriter<byte>();
            codec.Serialize(payload, value);
            var frame = new byte[PacketProtocol.HeaderSize + payload.WrittenCount];
            PacketParser.WriteHeader(frame, command, payload.WrittenCount);
            payload.WrittenSpan.CopyTo(frame.AsSpan(PacketProtocol.HeaderSize));
            await stream.WriteAsync(frame, cancellationToken);
        }

        private static async ValueTask<ReceivedFrame> ReadFrameInternalAsync(
            Stream stream,
            CancellationToken cancellationToken)
        {
            var header = new byte[PacketProtocol.HeaderSize];
            await stream.ReadExactlyAsync(header, cancellationToken);
            Assert.Equal(
                PacketProtocol.MagicNumber,
                BinaryPrimitives.ReadUInt32LittleEndian(header));

            var command = (PacketCommand)BinaryPrimitives.ReadUInt16LittleEndian(
                header.AsSpan(PacketProtocol.CommandOffset));
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
                header.AsSpan(PacketProtocol.LengthOffset));
            Assert.InRange(payloadLength, 0, PacketProtocol.MaxPayloadSize);

            var payload = new byte[payloadLength];
            if (payloadLength != 0)
            {
                await stream.ReadExactlyAsync(payload, cancellationToken);
            }

            return new ReceivedFrame(command, payload);
        }

        private static int ReserveLoopbackPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }
    }

    private sealed record ReceivedFrame(PacketCommand Command, byte[] Payload);

    private sealed class NoopLeaseStore : IDeviceSessionLeaseStore
    {
        public ValueTask<TakeOverResult> TakeOverAsync(
            long userId, ulong deviceIdHash, string sessionId, string transportId,
            string leaseOwnerToken, TimeSpan ttl, CancellationToken cancellationToken) =>
            ValueTask.FromResult(TakeOverResult.NoPreviousLease());

        public ValueTask ReleaseIfOwnerAsync(
            long userId, ulong deviceIdHash, string leaseOwnerToken, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> RefreshIfOwnerAsync(
            long userId, ulong deviceIdHash, string leaseOwnerToken, TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask<string?> GetCurrentSessionIdAsync(
            long userId, ulong deviceIdHash, CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>(null);
    }

    private sealed class NoopCallMessageBus : IRealtimeMessageBus
    {
        public Task PublishIncomingMessageAsync(IncomingMessageCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;
        public Task PublishMessageReceiptAsync(MessageReceiptCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;
        public Task<MessageHistoryPage> QueryMessageHistoryAsync(MessageHistoryQuery query, CancellationToken ct = default) =>
            Task.FromResult(MessageHistoryPage.Failed(query.RequestId, "x", "x"));
        public Task<ConversationListPage> QueryConversationListAsync(ConversationListQuery query, CancellationToken ct = default) =>
            Task.FromResult(ConversationListPage.Failed(query.RequestId, "x", "x"));
        public Task<ConversationMarkReadResult> MarkConversationReadAsync(ConversationMarkReadCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationMarkReadResult.Failed(command.RequestId, "x", "x"));
        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(ConversationSetPrefsCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationSetPrefsResult.Failed(command.RequestId, "x", "x"));
        public Task<GroupConversationResult> MutateGroupConversationAsync(GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));
        public Task<GroupConversationResult> QueryReadReceiptsAsync(GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));
        public Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(AttachmentFinalizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentFinalizeResult.Failed(command.RequestId, "x", "x"));
        public Task<AttachmentDownloadAuthorizeResult> AuthorizeAttachmentDownloadAsync(AttachmentDownloadAuthorizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeResult.Failed(command.RequestId, "x", "x"));
        public Task<RelationshipCommandResult> MutateRelationshipAsync(RelationshipCommand command, CancellationToken ct = default) =>
            Task.FromResult(RelationshipCommandResult.Failed(command.RequestId, "x", "x"));
        public Task<RelationshipListResult> QueryRelationshipListAsync(RelationshipListQuery query, CancellationToken ct = default) =>
            Task.FromResult(RelationshipListResult.Failed(query.RequestId, "x", "x"));
        public Task<MessageRecallResult> RecallMessageAsync(MessageRecallCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageRecallResult.Failed(command.RequestId, "x", "x"));
        public Task<MessageEditResult> EditMessageAsync(MessageEditCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageEditResult.Failed(command.RequestId, "x", "x"));
        public Task<MessageReactionResult> ReactToMessageAsync(MessageReactionCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageReactionResult.Failed(command.RequestId, "x", "x"));
        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(SyncBootstrapQuery query, CancellationToken ct = default) =>
            Task.FromResult(SyncBootstrapPage.Failed(query.RequestId, "x", "x"));
        public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(long userId, string messageId, CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistoryMessage?>(null);
        public Task<CallProcessResult> SendCallCommandAsync(CallCommand command, CancellationToken ct = default) =>
            Task.FromResult(CallProcessResult.Failed(CallErrorCode.StateStoreUnavailable, "unavailable"));
        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;
        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }
        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }
        public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;
        public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;
        public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }
        public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }
        public Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(PresenceAuthorizeQuery query, CancellationToken ct = default) =>
            Task.FromResult(new PresenceAuthorizeResponse { AllowedUserIds = query.TargetUserIds });
        public Task ServePresenceAuthorizeAsync(Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler, CancellationToken ct = default) =>
            Task.CompletedTask;
        public Task PublishPushDeliveryAsync(PushDeliveryCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;
        public async IAsyncEnumerable<PushDelivery> ConsumePushDeliveriesAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }
        public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}