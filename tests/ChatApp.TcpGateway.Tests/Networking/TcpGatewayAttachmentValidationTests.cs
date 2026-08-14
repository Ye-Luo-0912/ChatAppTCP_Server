using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using ChatApp.Realtime.Integration.Configuration;
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
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Commands.Relationships;
using ChatApp.TcpGateway.Gateway.Commands.Calls;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RealtimeHistory = ChatApp.Realtime.Abstractions.Messaging.History;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class TcpGatewayAttachmentValidationTests
{
    [Fact(Timeout = 10_000)]
    public async Task AttachmentOnlyMessageIsAcceptedAndPublished()
    {
        await using var harness = await GatewayHarness.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, harness.Port, timeout.Token);
        await using var stream = client.GetStream();
        await harness.AuthenticateAsync(stream, timeout.Token);

        var attachmentId = new string('a', 32);
        var clientMessageId = Guid.CreateVersion7().ToString("N");
        await WriteFrameAsync(
            stream,
            PacketCommand.ChatMessage,
            harness.ChatMessageCodec,
            new ChatMessage
            {
                MessageId = clientMessageId,
                TargetUserId = 99,
                Content = "",
                AttachmentIds = [attachmentId]
            },
            timeout.Token);

        var ackFrame = await ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.MessageAcknowledgement, ackFrame.Command);
        var ack = harness.AcknowledgementCodec.Deserialize(
            new ReadOnlySequence<byte>(ackFrame.Payload));
        Assert.NotNull(ack);
        Assert.True(ack.Accepted);

        var command = Assert.IsType<IncomingMessageCommand>(harness.MessageBus.LastIncomingMessage);
        Assert.Equal(clientMessageId, command.ClientMessageId);
        Assert.Equal(99, command.ReceiverUserId);
        Assert.Equal(string.Empty, command.Content);
        Assert.Equal([attachmentId], command.AttachmentIds);
    }

    [Fact(Timeout = 10_000)]
    public async Task EmptyContentWithoutAttachmentsClosesAsProtocolViolation()
    {
        await using var harness = await GatewayHarness.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, harness.Port, timeout.Token);
        await using var stream = client.GetStream();
        await harness.AuthenticateAsync(stream, timeout.Token);

        await WriteFrameAsync(
            stream,
            PacketCommand.ChatMessage,
            harness.ChatMessageCodec,
            new ChatMessage
            {
                MessageId = Guid.CreateVersion7().ToString("N"),
                TargetUserId = 99,
                Content = "   ",
                AttachmentIds = null
            },
            timeout.Token);

        var ackFrame = await ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.MessageAcknowledgement, ackFrame.Command);
        var ack = harness.AcknowledgementCodec.Deserialize(
            new ReadOnlySequence<byte>(ackFrame.Payload));
        Assert.NotNull(ack);
        Assert.False(ack.Accepted);
        Assert.Equal("invalid_message", ack.ErrorCode);

        await AssertConnectionClosedAsync(stream, timeout.Token);
        Assert.Null(harness.MessageBus.LastIncomingMessage);
    }

    [Fact(Timeout = 10_000)]
    public async Task TooManyAttachmentIdsClosesAsProtocolViolation()
    {
        await using var harness = await GatewayHarness.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, harness.Port, timeout.Token);
        await using var stream = client.GetStream();
        await harness.AuthenticateAsync(stream, timeout.Token);

        var ids = Enumerable.Range(0, 33)
            .Select(static i => i.ToString("x", System.Globalization.CultureInfo.InvariantCulture).PadLeft(8, '0'))
            .ToArray();

        await WriteFrameAsync(
            stream,
            PacketCommand.ChatMessage,
            harness.ChatMessageCodec,
            new ChatMessage
            {
                MessageId = Guid.CreateVersion7().ToString("N"),
                TargetUserId = 99,
                Content = "x",
                AttachmentIds = ids
            },
            timeout.Token);

        var ackFrame = await ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.MessageAcknowledgement, ackFrame.Command);
        var ack = harness.AcknowledgementCodec.Deserialize(
            new ReadOnlySequence<byte>(ackFrame.Payload));
        Assert.NotNull(ack);
        Assert.False(ack.Accepted);
        Assert.Equal(
            InboundPayloadEarlyValidator.TooManyAttachmentsCode,
            ack.ErrorCode);

        await AssertConnectionClosedAsync(stream, timeout.Token);
        Assert.Null(harness.MessageBus.LastIncomingMessage);
    }

    [Fact(Timeout = 10_000)]
    public async Task OversizedAttachmentIdClosesAsProtocolViolation()
    {
        await using var harness = await GatewayHarness.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, harness.Port, timeout.Token);
        await using var stream = client.GetStream();
        await harness.AuthenticateAsync(stream, timeout.Token);

        await WriteFrameAsync(
            stream,
            PacketCommand.ChatMessage,
            harness.ChatMessageCodec,
            new ChatMessage
            {
                MessageId = Guid.CreateVersion7().ToString("N"),
                TargetUserId = 99,
                Content = null,
                AttachmentIds = [new string('b', 65)]
            },
            timeout.Token);

        var ackFrame = await ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.MessageAcknowledgement, ackFrame.Command);
        var ack = harness.AcknowledgementCodec.Deserialize(
            new ReadOnlySequence<byte>(ackFrame.Payload));
        Assert.NotNull(ack);
        Assert.False(ack.Accepted);
        Assert.Equal(
            InboundPayloadEarlyValidator.InvalidAttachmentIdCode,
            ack.ErrorCode);

        await AssertConnectionClosedAsync(stream, timeout.Token);
        Assert.Null(harness.MessageBus.LastIncomingMessage);
    }

    private static async Task AssertConnectionClosedAsync(NetworkStream stream, CancellationToken ct)
    {
        var buffer = new byte[1];
        try
        {
            var read = await stream.ReadAsync(buffer.AsMemory(0, 1), ct);
            Assert.Equal(0, read);
        }
        catch (IOException)
        {
            // 对端因 ProtocolViolation 关闭连接。
        }
        catch (ObjectDisposedException)
        {
        }
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

    private static async ValueTask<ReceivedFrame> ReadFrameAsync(
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
            await stream.ReadExactlyAsync(payload, cancellationToken);

        return new ReceivedFrame(command, payload);
    }

    private sealed record ReceivedFrame(PacketCommand Command, byte[] Payload);

    private sealed class GatewayHarness : IAsyncDisposable
    {
        private readonly TcpGatewayService _service;

        private GatewayHarness(
            TcpGatewayService service,
            int port,
            CapturingRealtimeMessageBus messageBus,
            JsonPayloadCodec<ChatMessage> chatMessageCodec,
            JsonPayloadCodec<MessageAcknowledgement> acknowledgementCodec,
            JsonPayloadCodec<AuthenticationRequest> authenticationRequestCodec,
            JsonPayloadCodec<AuthenticationResponse> authenticationResponseCodec)
        {
            _service = service;
            Port = port;
            MessageBus = messageBus;
            ChatMessageCodec = chatMessageCodec;
            AcknowledgementCodec = acknowledgementCodec;
            AuthenticationRequestCodec = authenticationRequestCodec;
            AuthenticationResponseCodec = authenticationResponseCodec;
        }

        public int Port { get; }
        public CapturingRealtimeMessageBus MessageBus { get; }
        public JsonPayloadCodec<ChatMessage> ChatMessageCodec { get; }
        public JsonPayloadCodec<MessageAcknowledgement> AcknowledgementCodec { get; }
        private JsonPayloadCodec<AuthenticationRequest> AuthenticationRequestCodec { get; }
        private JsonPayloadCodec<AuthenticationResponse> AuthenticationResponseCodec { get; }

        public static async Task<GatewayHarness> StartAsync()
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
                AuthenticationTimeout = TimeSpan.FromSeconds(1),
                IdleTimeout = TimeSpan.FromSeconds(5),
                HeartbeatScanInterval = TimeSpan.FromMilliseconds(100),
                SendTimeout = TimeSpan.FromSeconds(1),
                MaxPacketsPerSecond = 40,
                MaxInboundBytesPerSecond = 256 * 1024,
                MaxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
                MaxChatAttachments = ChatMessageLimits.MaxAttachments,
                RequireClientHello = false
            };

            var messageBus = new CapturingRealtimeMessageBus();
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
            var conversationListItemCodec = new JsonPayloadCodec<ChatApp.Realtime.Abstractions.Conversations.ConversationListItem[]>(
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

            var metrics = new GatewayMetrics();

            // 共享依赖（同时用于 service 构造与 handler 构造）
            var integrationOptions = new RealtimeIntegrationOptions
            {
                InstanceId = "test-gateway-attach"
            };
            var globalPresence = new NoopGlobalPresenceStore();
            var userSessions = new UserSessionRegistry();
            var presenceWatchers = new PresenceWatcherRegistry();
            var typingFanout = new TypingFanoutCoordinator(TimeProvider.System);
            var pushStore = new InMemoryPushTokenStore();

            // Push handler 专用 codec
            var registerPushRequestCodec = new JsonPayloadCodec<RegisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenRequest);
            var registerPushResponseCodec = new JsonPayloadCodec<RegisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenResponse);
            var unregisterPushRequestCodec = new JsonPayloadCodec<UnregisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenRequest);
            var unregisterPushResponseCodec = new JsonPayloadCodec<UnregisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenResponse);

            // Reaction handler 专用 codec
            var addReactionRequestCodec = new JsonPayloadCodec<AddReactionRequest>(
                GatewayJsonSerializerContext.Default.AddReactionRequest);
            var addReactionAckCodec = new JsonPayloadCodec<AddReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.AddReactionAcknowledgement);
            var removeReactionRequestCodec = new JsonPayloadCodec<RemoveReactionRequest>(
                GatewayJsonSerializerContext.Default.RemoveReactionRequest);
            var removeReactionAckCodec = new JsonPayloadCodec<RemoveReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement);

            // Group handler 专用 codec
            var createGroupRequestCodec = new JsonPayloadCodec<CreateGroupRequest>(
                GatewayJsonSerializerContext.Default.CreateGroupRequest);
            var createGroupResponseCodec = new JsonPayloadCodec<CreateGroupResponse>(
                GatewayJsonSerializerContext.Default.CreateGroupResponse);
            var addGroupMembersRequestCodec = new JsonPayloadCodec<AddGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.AddGroupMembersRequest);
            var addGroupMembersResponseCodec = new JsonPayloadCodec<AddGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.AddGroupMembersResponse);
            var removeGroupMemberRequestCodec = new JsonPayloadCodec<RemoveGroupMemberRequest>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberRequest);
            var removeGroupMemberResponseCodec = new JsonPayloadCodec<RemoveGroupMemberResponse>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberResponse);
            var leaveGroupRequestCodec = new JsonPayloadCodec<LeaveGroupRequest>(
                GatewayJsonSerializerContext.Default.LeaveGroupRequest);
            var leaveGroupResponseCodec = new JsonPayloadCodec<LeaveGroupResponse>(
                GatewayJsonSerializerContext.Default.LeaveGroupResponse);
            var changeMemberRoleRequestCodec = new JsonPayloadCodec<ChangeMemberRoleRequest>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleRequest);
            var changeMemberRoleResponseCodec = new JsonPayloadCodec<ChangeMemberRoleResponse>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleResponse);
            var listGroupMembersRequestCodec = new JsonPayloadCodec<ListGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.ListGroupMembersRequest);
            var listGroupMembersResponseCodec = new JsonPayloadCodec<ListGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.ListGroupMembersResponse);
            var messageReadReceiptQueryRequestCodec = new JsonPayloadCodec<MessageReadReceiptQueryRequest>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryRequest);
            var messageReadReceiptQueryResponseCodec = new JsonPayloadCodec<MessageReadReceiptQueryResponse>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryResponse);
            var dissolveGroupRequestCodec = new JsonPayloadCodec<DissolveGroupRequest>(
                GatewayJsonSerializerContext.Default.DissolveGroupRequest);
            var dissolveGroupResponseCodec = new JsonPayloadCodec<DissolveGroupResponse>(
                GatewayJsonSerializerContext.Default.DissolveGroupResponse);

            // Typing / Presence handler 专用 codec
            var typingNotifyCodec = new JsonPayloadCodec<TypingNotify>(
                GatewayJsonSerializerContext.Default.TypingNotify);
            var presenceQueryRequestCodec = new JsonPayloadCodec<PresenceQueryRequest>(
                GatewayJsonSerializerContext.Default.PresenceQueryRequest);
            var presenceUnwatchRequestCodec = new JsonPayloadCodec<PresenceUnwatchRequest>(
                GatewayJsonSerializerContext.Default.PresenceUnwatchRequest);
            var presenceSnapshotResponseCodec = new JsonPayloadCodec<PresenceSnapshotResponse>(
                GatewayJsonSerializerContext.Default.PresenceSnapshotResponse);

            // TcpGatewayService 直接消费的 codec（DI 注入路径在此测试中以手工构造替代）
            var typingUpdateCodec = new JsonPayloadCodec<TypingUpdate>(
                GatewayJsonSerializerContext.Default.TypingUpdate);
            var presenceChangedCodec = new JsonPayloadCodec<PresenceChanged>(
                GatewayJsonSerializerContext.Default.PresenceChanged);

            // 8 个 handler。本测试只断言 Messaging 路径（ChatMessage 附件校验），
            // 但 CommandDispatcher 构造函数要求全部 8 个 handler 实例。
            var pushHandler = new PushTokenCommandHandler(
                registerPushRequestCodec,
                registerPushResponseCodec,
                unregisterPushRequestCodec,
                unregisterPushResponseCodec,
                metrics,
                NullLogger<PushTokenCommandHandler>.Instance,
                pushStore);
            var reactionHandler = new ReactionCommandHandler(
                messageBus,
                addReactionRequestCodec,
                addReactionAckCodec,
                removeReactionRequestCodec,
                removeReactionAckCodec,
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
                createGroupRequestCodec,
                createGroupResponseCodec,
                addGroupMembersRequestCodec,
                addGroupMembersResponseCodec,
                removeGroupMemberRequestCodec,
                removeGroupMemberResponseCodec,
                leaveGroupRequestCodec,
                leaveGroupResponseCodec,
                changeMemberRoleRequestCodec,
                changeMemberRoleResponseCodec,
                listGroupMembersRequestCodec,
                listGroupMembersResponseCodec,
                messageReadReceiptQueryRequestCodec,
                messageReadReceiptQueryResponseCodec,
                dissolveGroupRequestCodec,
                dissolveGroupResponseCodec,
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
                presenceQueryRequestCodec,
                presenceUnwatchRequestCodec,
                presenceSnapshotResponseCodec,
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
                new FakeAuthenticator(),
                authenticationRequestCodec,
                authenticationResponseCodec,
                acknowledgementCodec,
                typingUpdateCodec,
                presenceChangedCodec,
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
            return new GatewayHarness(
                service,
                port,
                messageBus,
                chatMessageCodec,
                acknowledgementCodec,
                authenticationRequestCodec,
                authenticationResponseCodec);
        }

        public async Task AuthenticateAsync(Stream stream, CancellationToken ct)
        {
            await WriteFrameAsync(
                stream,
                PacketCommand.AuthenticationRequest,
                AuthenticationRequestCodec,
                new AuthenticationRequest
                {
                    AccessToken = "valid-token",
                    DeviceIdHash = 7
                },
                ct);

            var frame = await ReadFrameAsync(stream, ct);
            Assert.Equal(PacketCommand.AuthenticationResponse, frame.Command);
            var response = AuthenticationResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(frame.Payload));
            Assert.NotNull(response);
            Assert.True(response.Success);
        }

        public async ValueTask DisposeAsync()
        {
            await _service.StopAsync(CancellationToken.None);
            _service.Dispose();
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

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(
                RealtimeAuthenticationResult.Success(
                    userId: 42,
                    sessionId: "attachment-validation",
                    userName: "test-user",
                    deviceIdHash,
                    roles: []));
    }

    private sealed class NoopLeaseStore : IDeviceSessionLeaseStore
    {
        public ValueTask<TakeOverResult> TakeOverAsync(
            long userId,
            ulong deviceIdHash,
            string sessionId,
            string transportId,
            string leaseOwnerToken,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(TakeOverResult.NoPreviousLease());

        public ValueTask ReleaseIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string leaseOwnerToken,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> RefreshIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string leaseOwnerToken,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask<string?> GetCurrentSessionIdAsync(
            long userId,
            ulong deviceIdHash,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>(null);
    }

    private sealed class CapturingRealtimeMessageBus : IRealtimeMessageBus
    {
        public IncomingMessageCommand? LastIncomingMessage { get; private set; }

        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command,
            CancellationToken ct = default)
        {
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
                    query.RequestId, [], null, false));

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                ConversationListPage.Success(query.RequestId, [], null, false));

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
                    changed: false));

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
                GroupConversationResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(
                GroupConversationResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

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
                MessageReactionResult.Failed(
                    command.RequestId,
                    "not_used",
                    "not used"));

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(
                SyncBootstrapPage.Success(
                    query.RequestId,
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    [],
                    null,
                    false,
                    []));

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

    public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}
