using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using ChatApp.Binary.Core;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
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
using ChatApp.Shared.Protocol.Tcp.Binary;
using ChatApp.Shared.Protocol.Tcp.Binary.Schemas;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RealtimeHistory =
    ChatApp.Realtime.Abstractions.Messaging.History;
using TcpCallCommandRequest = ChatApp.Shared.Protocol.Tcp.TcpCallCommandRequest;
using TcpCallCommandResponse = ChatApp.Shared.Protocol.Tcp.TcpCallCommandResponse;
using TcpCallSignal = ChatApp.Shared.Protocol.Tcp.TcpCallSignal;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// Ephemeral（Typing / Presence）混合格式 fanout 回归测试（真 TcpGatewayService + 真 TCP 回环）。
/// <para>
/// 背景：BIN-INTEGRATION-3 e2e 曾发现 5 处 fanout 站点漏做格式分组（旧 3 参
/// <c>OutboundFrameFactory.Create</c> 只产 JSON 帧，却投给 binary 协商会话）。
/// 本类驱动真实上行命令路径，覆盖其中两条此前无测试的站点：
/// <list type="bullet">
/// <item>Typing：TypingNotify → Ephemeral lane → <c>TypingCommandHandler</c> →
/// <c>TypingFanoutCoordinator</c>（时间轮 + emission）→ <c>TypingFanoutHost.FanoutTypingUpdate</c>
/// → 目标用户双会话（JSON + binary）各收到按自己格式编码、可正确解码的 TypingUpdate；</item>
/// <item>Presence：认证上线 → <c>SessionLifecycleCoordinator.OnAuthenticatedAsync</c> →
/// <c>BroadcastPresenceChangedLocal</c> → watcher 双会话（JSON + binary）各收到按自己格式
/// 编码、可正确解码的 PresenceChanged。</item>
/// </list>
/// 核心断言是"按会话协商格式解码成功"：JSON 会话帧以 <c>{</c> 开头且能 JSON 解码；
/// binary 会话帧能被 <see cref="TcpBinaryWireCodec.TryDecode"/> 解码。若 fanout 退回
/// 单一格式，测试必须失败——这正是其目的。
/// 组装方式照抄 <see cref="BinaryPayloadNegotiationTests"/>。
/// </para>
/// </summary>
[Collection("TcpSessionSerial")]
public sealed class EphemeralMixedFormatFanoutTests
{
    private const string JsonFormat = ProtocolPayloadFormat.Json;
    private const string BinaryFormat = BinaryPayloadFormat.Id;

    // 测试用户：42 = sender / watched，43 = target / watcher。
    private const long SenderOrWatchedUserId = 42;
    private const long TargetOrWatcherUserId = 43;

    private static readonly string TokenForSenderOrWatched =
        TokenFor(SenderOrWatchedUserId);
    private static readonly string TokenForTargetOrWatcher =
        TokenFor(TargetOrWatcherUserId);

    // ──────────── 场景 1：Typing 混合格式 fanout ────────────

    /// <summary>
    /// 目标用户 43 同时有 binary 协商与 JSON 协商两个会话；第三个会话（sender 42 的
    /// binary 会话）发送 TypingNotify（按其会话格式 chatapp-bin-v1 编码）。驱动
    /// TypingCommandHandler → TypingFanoutCoordinator → TypingFanoutHost 真实路径后，
    /// 两个目标会话必须各收到按自己协商格式编码、可正确解码且语义一致的 TypingUpdate。
    /// </summary>
    [Fact(Timeout = 20_000)]
    public async Task TypingNotifyFromSenderSession_FansOutTypingUpdateEncodedPerTargetSessionFormat()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(port);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var conversationId = ConversationId.CreateDirect(
                SenderOrWatchedUserId,
                TargetOrWatcherUserId);

            // 目标用户 43 的 JSON 协商会话（DeviceIdHash 互异，避免同设备替换踢下线）。
            using var jsonTarget = await ConnectAsync(port, timeout.Token);
            var jsonTargetStream = jsonTarget.GetStream();
            var jsonTargetHello = await HandshakeAsync(
                jsonTargetStream,
                featureBits: (uint)(GatewayFeature.CommandCapabilities |
                                    GatewayFeature.PresenceAndTyping),
                resumeToken: null,
                timeout.Token);
            Assert.Equal(JsonFormat, jsonTargetHello.PayloadFormat);
            var jsonTargetAuthentication = await AuthenticateAsJsonAsync(
                jsonTargetStream,
                TokenForTargetOrWatcher,
                deviceIdHash: 7,
                timeout.Token);
            Assert.Equal(TargetOrWatcherUserId, jsonTargetAuthentication.UserId);

            // 目标用户 43 的 binary 协商会话。
            using var binaryTarget = await ConnectAsync(port, timeout.Token);
            var binaryTargetStream = binaryTarget.GetStream();
            var binaryTargetAuthentication =
                await PerformBinaryHandshakeAndAuthenticationAsync(
                    binaryTargetStream,
                    TokenForTargetOrWatcher,
                    deviceIdHash: 8,
                    timeout.Token);
            Assert.Equal(TargetOrWatcherUserId, binaryTargetAuthentication.UserId);

            // 第三个会话：sender 用户 42 的 binary 会话，发送 TypingNotify。
            using var binarySender = await ConnectAsync(port, timeout.Token);
            var binarySenderStream = binarySender.GetStream();
            var senderAuthentication = await PerformBinaryHandshakeAndAuthenticationAsync(
                binarySenderStream,
                TokenForSenderOrWatched,
                deviceIdHash: 9,
                timeout.Token);
            Assert.Equal(SenderOrWatchedUserId, senderAuthentication.UserId);

            await WriteBinaryFrameAsync(
                binarySenderStream,
                PacketCommand.TypingNotify,
                new TypingNotify
                {
                    ConversationId = conversationId,
                    IsTyping = true
                },
                timeout.Token);

            // JSON 会话收到 JSON 帧：以 '{' 开头且能按 JSON 解码出一致语义。
            var jsonFrame = await ReadFrameAsync(jsonTargetStream, timeout.Token);
            Assert.Equal(PacketCommand.TypingUpdate, jsonFrame.Command);
            Assert.Equal((byte)'{', jsonFrame.Payload[0]);
            var jsonUpdate = gateway.TypingUpdateCodec.Deserialize(
                new ReadOnlySequence<byte>(jsonFrame.Payload));
            Assert.NotNull(jsonUpdate);
            Assert.Equal(SenderOrWatchedUserId, jsonUpdate.SenderUserId);
            Assert.Equal(conversationId, jsonUpdate.ConversationId);
            Assert.True(jsonUpdate.IsTyping);

            // binary 会话收到二进制帧：能被 TcpBinaryWireCodec 解码出一致语义。
            // （若 fanout 退回 JSON-only，TryDecode 必然失败——回归断言核心。）
            var binaryFrame = await ReadFrameAsync(binaryTargetStream, timeout.Token);
            Assert.Equal(PacketCommand.TypingUpdate, binaryFrame.Command);
            var binaryUpdate = DecodeBinaryPayload<TypingUpdate>(
                PacketCommand.TypingUpdate,
                binaryFrame.Payload);
            Assert.Equal(SenderOrWatchedUserId, binaryUpdate.SenderUserId);
            Assert.Equal(conversationId, binaryUpdate.ConversationId);
            Assert.True(binaryUpdate.IsTyping);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 2：Presence 混合格式 fanout ────────────

    /// <summary>
    /// watcher 用户 43 同时有 JSON 协商与 binary 协商两个会话，先经真实 PresenceQuery
    /// 命令（binary 会话发送）订阅被观察用户 42；随后用户 42 通过真实认证路径上线，
    /// 驱动 SessionLifecycleCoordinator.OnAuthenticatedAsync → BroadcastPresenceChangedLocal。
    /// watcher 的两个会话必须各收到按自己协商格式编码、可正确解码且语义一致的
    /// PresenceChanged(UserId=42, IsOnline=true)。
    /// </summary>
    [Fact(Timeout = 20_000)]
    public async Task PresenceOnlineTransition_FansOutPresenceChangedEncodedPerWatcherSessionFormat()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(port);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

            // watcher 用户 43 的 JSON 协商会话。
            using var jsonWatcher = await ConnectAsync(port, timeout.Token);
            var jsonWatcherStream = jsonWatcher.GetStream();
            var jsonWatcherHello = await HandshakeAsync(
                jsonWatcherStream,
                featureBits: (uint)(GatewayFeature.CommandCapabilities |
                                    GatewayFeature.PresenceAndTyping),
                resumeToken: null,
                timeout.Token);
            Assert.Equal(JsonFormat, jsonWatcherHello.PayloadFormat);
            var jsonWatcherAuthentication = await AuthenticateAsJsonAsync(
                jsonWatcherStream,
                TokenForTargetOrWatcher,
                deviceIdHash: 7,
                timeout.Token);
            Assert.Equal(TargetOrWatcherUserId, jsonWatcherAuthentication.UserId);

            // watcher 用户 43 的 binary 协商会话。
            using var binaryWatcher = await ConnectAsync(port, timeout.Token);
            var binaryWatcherStream = binaryWatcher.GetStream();
            var binaryWatcherAuthentication =
                await PerformBinaryHandshakeAndAuthenticationAsync(
                    binaryWatcherStream,
                    TokenForTargetOrWatcher,
                    deviceIdHash: 8,
                    timeout.Token);
            Assert.Equal(TargetOrWatcherUserId, binaryWatcherAuthentication.UserId);

            // binary watcher 会话发送 PresenceQuery 订阅用户 42（按其会话格式编码）。
            // PresenceSnapshot 响应到达即代表 WatchMany 登记已完成（响应在登记后排队），
            // 同时验证 binary 会话收到的快照同样是 binary 编码（可解码）。
            await WriteBinaryFrameAsync(
                binaryWatcherStream,
                PacketCommand.PresenceQuery,
                new PresenceQueryRequest
                {
                    RequestId = "presence-mixed-format-1",
                    UserIds = [SenderOrWatchedUserId]
                },
                timeout.Token);
            var snapshotFrame = await ReadFrameAsync(binaryWatcherStream, timeout.Token);
            Assert.Equal(PacketCommand.PresenceSnapshot, snapshotFrame.Command);
            var snapshot = DecodeBinaryPayload<PresenceSnapshotResponse>(
                PacketCommand.PresenceSnapshot,
                snapshotFrame.Payload);
            Assert.Equal("presence-mixed-format-1", snapshot.RequestId);
            var snapshotItem = Assert.Single(snapshot.Items);
            Assert.Equal(SenderOrWatchedUserId, snapshotItem.UserId);
            Assert.False(snapshotItem.IsOnline);

            // 被观察用户 42 经真实认证路径上线：
            // AuthenticationRequest → OnAuthenticatedAsync → 全局 0→1 转换 →
            // BroadcastPresenceChangedLocal → watcher(43) 的全部会话。
            using var binaryWatched = await ConnectAsync(port, timeout.Token);
            var binaryWatchedStream = binaryWatched.GetStream();
            var watchedAuthentication = await PerformBinaryHandshakeAndAuthenticationAsync(
                binaryWatchedStream,
                TokenForSenderOrWatched,
                deviceIdHash: 9,
                timeout.Token);
            Assert.Equal(SenderOrWatchedUserId, watchedAuthentication.UserId);

            // JSON watcher 会话收到 JSON 帧：以 '{' 开头且能按 JSON 解码出一致语义。
            var jsonFrame = await ReadFrameAsync(jsonWatcherStream, timeout.Token);
            Assert.Equal(PacketCommand.PresenceChanged, jsonFrame.Command);
            Assert.Equal((byte)'{', jsonFrame.Payload[0]);
            var jsonPresence = gateway.PresenceChangedCodec.Deserialize(
                new ReadOnlySequence<byte>(jsonFrame.Payload));
            Assert.NotNull(jsonPresence);
            Assert.Equal(SenderOrWatchedUserId, jsonPresence.UserId);
            Assert.True(jsonPresence.IsOnline);

            // binary watcher 会话收到二进制帧：能被 TcpBinaryWireCodec 解码出一致语义。
            // （若 fanout 退回 JSON-only，TryDecode 必然失败——回归断言核心。）
            var binaryFrame = await ReadFrameAsync(binaryWatcherStream, timeout.Token);
            Assert.Equal(PacketCommand.PresenceChanged, binaryFrame.Command);
            var binaryPresence = DecodeBinaryPayload<PresenceChanged>(
                PacketCommand.PresenceChanged,
                binaryFrame.Payload);
            Assert.Equal(SenderOrWatchedUserId, binaryPresence.UserId);
            Assert.True(binaryPresence.IsOnline);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 组装：真 TcpGatewayService + 真 handler 图 ────────────

    private sealed record GatewayFixture(
        TcpGatewayService Service,
        CapturingRealtimeMessageBus Bus,
        GatewayMetrics Metrics,
        UserSessionRegistry UserSessions,
        FirstTransitionPresenceStore GlobalPresence,
        JsonPayloadCodec<TypingUpdate> TypingUpdateCodec,
        JsonPayloadCodec<PresenceChanged> PresenceChangedCodec);

    private static GatewayFixture CreateGateway(int port)
    {
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
            AuthenticationTimeout = TimeSpan.FromSeconds(1),
            IdleTimeout = TimeSpan.FromSeconds(5),
            HeartbeatScanInterval = TimeSpan.FromMilliseconds(100),
            SendTimeout = TimeSpan.FromSeconds(1),
            MaxPacketsPerSecond = 100,
            MaxInboundBytesPerSecond = 256 * 1024,
            MaxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
            RequireClientHello = true,
            UseActorRuntimeForEphemeralCommands = true,
            InboundTransportMode = InboundTransportMode.Pipelines,
            OutboundSendMode = OutboundSendMode.PersistentSendLoop,
            EnableBinaryPayloadFormat = true,
            EnableEphemeralPresenceAndTyping = true,
            GoAwayDrainTimeout = TimeSpan.FromSeconds(5)
        };

        var metrics = new GatewayMetrics();
        var userSessions = new UserSessionRegistry();
        var messageBus = new CapturingRealtimeMessageBus();
        var globalPresence = new FirstTransitionPresenceStore();
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
            InstanceId = "ephemeral-mixed-format-gateway"
        };
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
            CallSignalingIntegrationTests.DisabledGroupRelay(),
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
            new TokenMappedAuthenticator(),
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
            clientHelloCodec: clientHelloCodec,
            serverHelloCodec: serverHelloCodec,
            goAwayCodec: goAwayCodec,
            resumeResponseCodec: resumeResponseCodec,
            protocolErrorFrameCodec: protocolErrorCodec,
            commandDispatcher: commandDispatcher);

        return new GatewayFixture(
            service,
            messageBus,
            metrics,
            userSessions,
            globalPresence,
            typingUpdateCodec,
            presenceChangedCodec);
    }

    /// <summary>
    /// AccessToken → UserId 映射认证器：允许同一网关内多用户共存
    ///（42 = sender/watched，43 = target/watcher）。
    /// </summary>
    private sealed class TokenMappedAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(
                accessToken == TokenForSenderOrWatched
                    ? RealtimeAuthenticationResult.Success(
                        SenderOrWatchedUserId,
                        "integration-test-sender",
                        "test-user-42",
                        deviceIdHash,
                        roles: [])
                    : accessToken == TokenForTargetOrWatcher
                        ? RealtimeAuthenticationResult.Success(
                            TargetOrWatcherUserId,
                            "integration-test-target",
                            "test-user-43",
                            deviceIdHash,
                            roles: [])
                        : RealtimeAuthenticationResult.Failure("unknown token"));
    }

    private static string TokenFor(long userId) => $"token-for-user-{userId}";

    /// <summary>
    /// 首次转换 Presence 存储：每用户首次 SetOnline 返回 WentOnline、首次 SetOffline
    /// 返回 WentOffline，其余 None——模拟 Redis 全局在线状态的 0→1 / 1→0 转换，
    /// 使 SessionLifecycleCoordinator 触发本地 Presence 广播。
    /// </summary>
    private sealed class FirstTransitionPresenceStore : IGlobalPresenceStore
    {
        private readonly HashSet<long> _online = [];

        public Task<PresenceTransition> SetOnlineAsync(
            long userId,
            string instanceId,
            CancellationToken ct = default)
        {
            lock (_online)
            {
                return Task.FromResult(
                    _online.Add(userId)
                        ? PresenceTransition.WentOnline
                        : PresenceTransition.None);
            }
        }

        public Task<PresenceTransition> SetOfflineAsync(
            long userId,
            string instanceId,
            CancellationToken ct = default)
        {
            lock (_online)
            {
                return Task.FromResult(
                    _online.Remove(userId)
                        ? PresenceTransition.WentOffline
                        : PresenceTransition.None);
            }
        }

        public Task RefreshOnlineAsync(
            long userId,
            string instanceId,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default)
        {
            lock (_online)
            {
                return Task.FromResult(_online.Contains(userId));
            }
        }

        public Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
            IReadOnlyList<long> userIds,
            CancellationToken ct = default)
        {
            lock (_online)
            {
                return Task.FromResult<IReadOnlyDictionary<long, bool>>(
                    userIds.ToDictionary(
                        static id => id,
                        id => _online.Contains(id)));
            }
        }

        public Task RunMaintenanceAsync(CancellationToken ct = default) =>
            Task.CompletedTask;
    }

    private sealed class CapturingRealtimeMessageBus : IRealtimeMessageBus
    {
        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command,
            CancellationToken ct = default) =>
            Task.CompletedTask;

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

    // ──────────── 客户端原语：帧读写 / 二进制编解码 ────────────

    private static async Task<TcpClient> ConnectAsync(int port, CancellationToken cancellationToken)
    {
        var client = new TcpClient();
        client.NoDelay = true;
        await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
        return client;
    }

    /// <summary>发送 JSON ClientHello 并读取 JSON ServerHello（握手段恒 JSON）。</summary>
    private static async Task<ServerHello> HandshakeAsync(
        Stream stream,
        uint featureBits,
        string? resumeToken,
        CancellationToken cancellationToken)
    {
        await WriteJsonFrameAsync(
            stream,
            PacketCommand.ClientHello,
            new ClientHello
            {
                ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
                FeatureBits = featureBits,
                InstallationId = "ephemeral-mixed-format-test",
                ResumeToken = resumeToken
            },
            cancellationToken);

        var serverHelloFrame = await ReadFrameAsync(stream, cancellationToken);
        Assert.Equal(PacketCommand.ServerHello, serverHelloFrame.Command);
        var serverHello = JsonServerHelloCodec.Deserialize(
            new ReadOnlySequence<byte>(serverHelloFrame.Payload));
        Assert.NotNull(serverHello);
        Assert.Equal(
            PacketProtocol.CurrentProtocolVersion,
            serverHello.ProtocolVersion);
        return serverHello;
    }

    private static readonly JsonPayloadCodec<ServerHello> JsonServerHelloCodec =
        new(GatewayJsonSerializerContext.Default.ServerHello);

    /// <summary>JSON 会话认证，返回 AuthenticationResponse 供断言用户身份。</summary>
    private static async Task<AuthenticationResponse> AuthenticateAsJsonAsync(
        Stream stream,
        string accessToken,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        await WriteJsonFrameAsync(
            stream,
            PacketCommand.AuthenticationRequest,
            new AuthenticationRequest
            {
                AccessToken = accessToken,
                DeviceIdHash = deviceIdHash
            },
            cancellationToken);

        var authenticationFrame = await ReadFrameAsync(stream, cancellationToken);
        Assert.Equal(
            PacketCommand.AuthenticationResponse,
            authenticationFrame.Command);
        var authentication = JsonAuthenticationResponseCodec.Deserialize(
            new ReadOnlySequence<byte>(authenticationFrame.Payload));
        Assert.NotNull(authentication);
        Assert.True(authentication.Success);
        return authentication;
    }

    private static readonly JsonPayloadCodec<AuthenticationResponse> JsonAuthenticationResponseCodec =
        new(GatewayJsonSerializerContext.Default.AuthenticationResponse);

    /// <summary>协商 binary + 二进制认证（可指定 AccessToken 与 DeviceIdHash）。</summary>
    private static async Task<AuthenticationResponse> PerformBinaryHandshakeAndAuthenticationAsync(
        Stream stream,
        string accessToken,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        var serverHello = await HandshakeAsync(
            stream,
            featureBits: (uint)(GatewayFeature.CommandCapabilities |
                                GatewayFeature.BinaryPayload |
                                GatewayFeature.PresenceAndTyping),
            resumeToken: null,
            cancellationToken);
        Assert.Equal(BinaryFormat, serverHello.PayloadFormat);

        await WriteBinaryFrameAsync(
            stream,
            PacketCommand.AuthenticationRequest,
            new AuthenticationRequest
            {
                AccessToken = accessToken,
                DeviceIdHash = deviceIdHash
            },
            cancellationToken);

        var authenticationFrame = await ReadFrameAsync(stream, cancellationToken);
        Assert.Equal(
            PacketCommand.AuthenticationResponse,
            authenticationFrame.Command);
        var authentication = DecodeBinaryPayload<AuthenticationResponse>(
            PacketCommand.AuthenticationResponse,
            authenticationFrame.Payload);
        Assert.True(authentication.Success);
        return authentication;
    }

    private static byte[] EncodeBinaryPayload<T>(PacketCommand command, T value)
        where T : class
    {
        var shared = BinaryPayloadMapper.ToShared(command, value);
        var buffer = new byte[BinaryLimits.Default.MaxMessageBytes];
        var encode = TcpBinaryWireEncoder.TryEncode(
            shared,
            buffer,
            BinaryLimits.Default);
        Assert.Equal(TcpBinaryWireEncodeStatus.Encoded, encode.Status);
        return buffer.AsSpan(0, encode.Written).ToArray();
    }

    private static async ValueTask WriteBinaryFrameAsync<T>(
        Stream stream,
        PacketCommand command,
        T value,
        CancellationToken cancellationToken)
        where T : class
    {
        var payload = EncodeBinaryPayload(command, value);
        await WriteRawFrameAsync(stream, command, payload, cancellationToken);
    }

    private static TLocal DecodeBinaryPayload<TLocal>(PacketCommand command, byte[] payload)
        where TLocal : class
    {
        var decode = TcpBinaryWireCodec.TryDecode(
            command,
            new ReadOnlySequence<byte>(payload),
            BinaryLimits.Default);
        Assert.Equal(TcpBinaryWireStatus.Decoded, decode.Status);
        return BinaryPayloadMapper.ToLocal<TLocal>(command, decode.Value!)!;
    }

    private static readonly JsonPayloadCodec<ClientHello> JsonClientHelloCodec =
        new(GatewayJsonSerializerContext.Default.ClientHello);

    private static readonly JsonPayloadCodec<AuthenticationRequest> JsonAuthenticationRequestCodec =
        new(GatewayJsonSerializerContext.Default.AuthenticationRequest);

    private static async ValueTask WriteJsonFrameAsync<T>(
        Stream stream,
        PacketCommand command,
        T value,
        CancellationToken cancellationToken)
    {
        var writer = new ArrayBufferWriter<byte>();
        var codec = typeof(T) switch
        {
            var t when t == typeof(ClientHello) =>
                (JsonPayloadCodec<T>)(object)JsonClientHelloCodec,
            var t when t == typeof(AuthenticationRequest) =>
                (JsonPayloadCodec<T>)(object)JsonAuthenticationRequestCodec,
            _ => throw new InvalidOperationException(
                $"no cached json codec for {typeof(T).Name}")
        };
        codec.Serialize(writer, value);

        var frame = new byte[
            PacketProtocol.HeaderSize + writer.WrittenCount];
        PacketParser.WriteHeader(
            frame,
            command,
            writer.WrittenCount);
        writer.WrittenSpan.CopyTo(
            frame.AsSpan(PacketProtocol.HeaderSize));

        await stream.WriteAsync(frame, cancellationToken);
    }

    private static async ValueTask WriteRawFrameAsync(
        Stream stream,
        PacketCommand command,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        var frame = new byte[PacketProtocol.HeaderSize + payload.Length];
        PacketParser.WriteHeader(frame, command, payload.Length);
        payload.CopyTo(frame.AsSpan(PacketProtocol.HeaderSize));
        await stream.WriteAsync(frame, cancellationToken);
    }

    private static async ValueTask<ReceivedFrame> ReadFrameAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        await stream.ReadExactlyAsync(header, cancellationToken);

        Assert.Equal(
            PacketProtocol.MagicNumber,
            BinaryPrimitives.ReadUInt32LittleEndian(header));

        var command = (PacketCommand)
            BinaryPrimitives.ReadUInt16LittleEndian(
                header.AsSpan(PacketProtocol.CommandOffset));
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset));
        Assert.InRange(
            payloadLength,
            0,
            PacketProtocol.MaxPayloadSize);

        var payload = new byte[payloadLength];
        if (payloadLength != 0)
        {
            await stream.ReadExactlyAsync(
                payload,
                cancellationToken);
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

    private sealed record ReceivedFrame(
        PacketCommand Command,
        byte[] Payload);
}
