using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Authentication;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RealtimeEventDispatcher = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventDispatcher;
using RealtimeHistory =
    ChatApp.Realtime.Abstractions.Messaging.History;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class TcpGatewayServiceTests
{
    [Theory(Timeout = 10_000)]
    [InlineData(OutboundSendMode.PersistentSendLoop)]
    [InlineData(OutboundSendMode.OnDemandSendPump)]
    public async Task PublishesIncomingMessageAndDispatchesPersistedEventOverTcp(
        OutboundSendMode outboundSendMode)
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
            MaxPacketsPerSecond = 20,
            MaxInboundBytesPerSecond = 256 * 1024,
            MaxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
            MaxChatAttachments = 32,
            RequireClientHello = false,
            // A/B：同一场景同时验证 PersistentSendLoop（永久 SendLoop Task）
            // 与 OnDemandSendPump（共享 worker 池按需 pump）两条出站路径。
            OutboundSendMode = outboundSendMode
        };

        using var metrics = new GatewayMetrics();
        var userSessions = new UserSessionRegistry();
        var messageBus = new CapturingRealtimeMessageBus();
        var authenticationRequestCodec =
            new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest);
        var authenticationResponseCodec =
            new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse);
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
        var receiptUpdateCodec =
            new JsonPayloadCodec<MessageReceiptUpdate>(
                GatewayJsonSerializerContext.Default.MessageReceiptUpdate);
        var historyRequestCodec =
            new JsonPayloadCodec<MessageHistoryRequest>(
                GatewayJsonSerializerContext.Default.MessageHistoryRequest);
        var historyResponseCodec =
            new JsonPayloadCodec<MessageHistoryResponse>(
                GatewayJsonSerializerContext.Default.MessageHistoryResponse);
        var historyItemCodec =
            new JsonPayloadCodec<MessageHistoryItem[]>(
                GatewayJsonSerializerContext.Default.MessageHistoryItemArray);
        var conversationListRequestCodec =
            new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest);
        var conversationListResponseCodec =
            new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse);
        var conversationListItemCodec =
            new JsonPayloadCodec<ChatApp.TcpGateway.Core.Messaging.Conversations.ConversationListItem[]>(
                GatewayJsonSerializerContext.Default.ConversationListItemArray);
        var conversationMarkReadRequestCodec =
            new JsonPayloadCodec<ConversationMarkReadRequest>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadRequest);
        var conversationMarkReadResponseCodec =
            new JsonPayloadCodec<ConversationMarkReadResponse>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadResponse);
        var conversationSetPrefsRequestCodec =
            new JsonPayloadCodec<ConversationSetPrefsRequest>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsRequest);
        var conversationSetPrefsResponseCodec =
            new JsonPayloadCodec<ConversationSetPrefsResponse>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsResponse);
        var messageRecallRequestCodec =
            new JsonPayloadCodec<MessageRecallRequest>(
                GatewayJsonSerializerContext.Default.MessageRecallRequest);
        var messageRecallAcknowledgementCodec =
            new JsonPayloadCodec<MessageRecallAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageRecallAcknowledgement);
        var messageEditRequestCodec =
            new JsonPayloadCodec<MessageEditRequest>(
                GatewayJsonSerializerContext.Default.MessageEditRequest);
        var messageEditAcknowledgementCodec =
            new JsonPayloadCodec<MessageEditAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageEditAcknowledgement);
        var syncBootstrapRequestCodec =
            new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest);
        var syncBootstrapResponseCodec =
            new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse);
        // 共享依赖（同时用于 service 构造与 handler 构造）
        var integrationOptions = new RealtimeIntegrationOptions
        {
            InstanceId = "test-gateway"
        };
        var globalPresence = new NoopGlobalPresenceStore();
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

        // 8 个 handler。本测试只断言 Messaging / HistoryQuery 路径，但 CommandDispatcher
        // 构造函数要求全部 8 个 handler 实例。
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

        var commandDispatcher = new CommandDispatcher(
            pushHandler,
            reactionHandler,
            messagingHandler,
            historyQueryHandler,
            conversationPrefsHandler,
            groupHandler,
            typingHandler,
            presenceHandler);

        using var service = new TcpGatewayService(
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
            commandDispatcher: commandDispatcher);

        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
            using var client = new TcpClient();
            client.NoDelay = true;
            await client.ConnectAsync(
                IPAddress.Loopback,
                port,
                timeout.Token);
            await using var stream = client.GetStream();

            await AuthenticateAsync(
                stream,
                authenticationRequestCodec,
                authenticationResponseCodec,
                timeout.Token);

            await WriteEmptyFrameAsync(
                stream,
                PacketCommand.Heartbeat,
                timeout.Token);
            var heartbeatFrame = await ReadFrameAsync(
                stream,
                timeout.Token);
            Assert.Equal(
                PacketCommand.HeartbeatAcknowledgement,
                heartbeatFrame.Command);

            var clientMessageId = Guid
                .CreateVersion7()
                .ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.ChatMessage,
                chatMessageCodec,
                new ChatMessage
                {
                    MessageId = clientMessageId,
                    TargetUserId = 42,
                    Content = "hello through JetStream"
                },
                timeout.Token);

            var acknowledgementFrame = await ReadFrameAsync(
                stream,
                timeout.Token);
            Assert.Equal(
                PacketCommand.MessageAcknowledgement,
                acknowledgementFrame.Command);

            var acknowledgement = acknowledgementCodec.Deserialize(
                new ReadOnlySequence<byte>(
                    acknowledgementFrame.Payload));
            Assert.NotNull(acknowledgement);
            Assert.True(acknowledgement.Accepted);
            Assert.Equal(
                clientMessageId,
                acknowledgement.ClientMessageId);

            var command = Assert.IsType<IncomingMessageCommand>(
                messageBus.LastIncomingMessage);
            Assert.Equal(42, command.SenderUserId);
            Assert.Equal(42, command.ReceiverUserId);
            Assert.Equal(
                clientMessageId,
                command.ClientMessageId);
            Assert.Equal(64, command.CommandId.Length);
            Assert.Equal(
                command.CommandId,
                acknowledgement.CommandId);

            var dispatcher = new RealtimeEventDispatcher(
                userSessions,
                chatMessageCodec,
                receiptUpdateCodec,
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
            dispatcher.Dispatch(
                CreateMessageReceivedEvent(command));

            var deliveredFrame = await ReadFrameAsync(
                stream,
                timeout.Token);
            Assert.Equal(
                PacketCommand.ChatMessage,
                deliveredFrame.Command);

            var deliveredMessage = chatMessageCodec.Deserialize(
                new ReadOnlySequence<byte>(
                    deliveredFrame.Payload));
            Assert.NotNull(deliveredMessage);
            Assert.Equal(command.CommandId, deliveredMessage.MessageId);
            Assert.Equal(command.SenderUserId, deliveredMessage.SenderUserId);
            Assert.Equal(command.ReceiverUserId, deliveredMessage.TargetUserId);
            Assert.Equal(command.Content, deliveredMessage.Content);
            await WriteFrameAsync(
                stream,
                PacketCommand.MessageReceipt,
                receiptRequestCodec,
                new MessageReceiptRequest
                {
                    MessageId = command.CommandId,
                    State = MessageReceiptState.Read
                },
                timeout.Token);

            var receiptAcknowledgementFrame = await ReadFrameAsync(
                stream,
                timeout.Token);
            Assert.Equal(
                PacketCommand.MessageReceiptAcknowledgement,
                receiptAcknowledgementFrame.Command);
            var receiptAcknowledgement =
                receiptAcknowledgementCodec.Deserialize(
                    new ReadOnlySequence<byte>(
                        receiptAcknowledgementFrame.Payload));
            Assert.NotNull(receiptAcknowledgement);
            Assert.True(receiptAcknowledgement.Accepted);
            Assert.Equal(command.CommandId, receiptAcknowledgement.MessageId);
            Assert.Equal(
                MessageReceiptState.Read,
                receiptAcknowledgement.State);

            var receiptCommand = Assert.IsType<MessageReceiptCommand>(
                messageBus.LastReceipt);
            Assert.Equal(command.CommandId, receiptCommand.MessageId);
            Assert.Equal(42, receiptCommand.ReceiverUserId);
            Assert.Equal(
                MessageReceiptType.Read,
                receiptCommand.ReceiptType);
            Assert.Equal(
                receiptCommand.CommandId,
                receiptAcknowledgement.CommandId);

            dispatcher.Dispatch(
                CreateReceiptUpdatedEvent(
                    command,
                    receiptCommand));

            var receiptUpdateFrame = await ReadFrameAsync(
                stream,
                timeout.Token);
            Assert.Equal(
                PacketCommand.MessageReceiptUpdated,
                receiptUpdateFrame.Command);
            var receiptUpdate = receiptUpdateCodec.Deserialize(
                new ReadOnlySequence<byte>(
                    receiptUpdateFrame.Payload));
            Assert.NotNull(receiptUpdate);
            Assert.Equal(command.CommandId, receiptUpdate.MessageId);
            Assert.Equal(42, receiptUpdate.ReceiverUserId);
            Assert.Equal(MessageReceiptState.Read, receiptUpdate.State);
            var historyRequestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.MessageHistoryRequest,
                historyRequestCodec,
                new MessageHistoryRequest
                {
                    RequestId = historyRequestId,
                    Limit = 20
                },
                timeout.Token);

            var historyFrame = await ReadFrameAsync(
                stream,
                timeout.Token);
            Assert.Equal(
                PacketCommand.MessageHistoryPage,
                historyFrame.Command);
            var historyPage = historyResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(historyFrame.Payload));
            Assert.NotNull(historyPage);
            Assert.True(historyPage.Succeeded);
            Assert.Equal(historyRequestId, historyPage.RequestId);
            var historyItem = Assert.Single(historyPage.Items);
            Assert.Equal(command.CommandId, historyItem.MessageId);
            Assert.NotNull(historyItem.DeliveredAtMs);
            Assert.NotNull(historyItem.ReadAtMs);

            var historyQuery = Assert.IsType<RealtimeHistory.MessageHistoryQuery>(
                messageBus.LastHistoryQuery);
            Assert.Equal(42, historyQuery.UserId);
            Assert.Equal(20, historyQuery.Limit);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    private static async Task AuthenticateAsync(
        Stream stream,
        JsonPayloadCodec<AuthenticationRequest> requestCodec,
        JsonPayloadCodec<AuthenticationResponse> responseCodec,
        CancellationToken cancellationToken)
    {
        await WriteFrameAsync(
            stream,
            PacketCommand.AuthenticationRequest,
            requestCodec,
            new AuthenticationRequest
            {
                AccessToken = "valid-token",
                DeviceIdHash = 7
            },
            cancellationToken);

        var authenticationFrame = await ReadFrameAsync(
            stream,
            cancellationToken);
        Assert.Equal(
            PacketCommand.AuthenticationResponse,
            authenticationFrame.Command);

        var response = responseCodec.Deserialize(
            new ReadOnlySequence<byte>(
                authenticationFrame.Payload));
        Assert.NotNull(response);
        Assert.True(response.Success);
        Assert.Equal(42, response.UserId);
        Assert.Equal("integration-test", response.SessionId);
    }

    private static RealtimeEvent CreateMessageReceivedEvent(
        IncomingMessageCommand command) =>
        new()
        {
            EventId = command.CommandId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = command.ReceiverUserId,
            ActorUserId = command.SenderUserId,
            MessageId = command.CommandId,
            SessionId = "upstream-persist-session",
            PayloadJson = $$"""
                {
                  "messageId": "{{command.CommandId}}",
                  "clientMessageId": "{{command.ClientMessageId}}",
                  "senderUserId": {{command.SenderUserId}},
                  "senderSessionId": "{{command.SenderSessionId}}",
                  "receiverUserId": {{command.ReceiverUserId}},
                  "content": "{{command.Content}}",
                  "receivedAtMs": {{command.ReceivedAtMs}}
                }
                """,
            OccurredAtMs = command.ReceivedAtMs
        };

    private static RealtimeEvent CreateReceiptUpdatedEvent(
        IncomingMessageCommand message,
        MessageReceiptCommand receipt) =>
        new()
        {
            EventId = receipt.CommandId,
            Type = RealtimeEventType.MessageReceiptUpdated,
            TargetUserId = message.SenderUserId,
            ActorUserId = receipt.ReceiverUserId,
            MessageId = message.CommandId,
            SessionId = receipt.ReceiverSessionId,
            PayloadJson = $$"""
                {
                  "messageId": "{{message.CommandId}}",
                  "receiverUserId": {{receipt.ReceiverUserId}},
                  "receiptType": {{(byte)receipt.ReceiptType}},
                  "occurredAtMs": {{receipt.OccurredAtMs}}
                }
                """,
            OccurredAtMs = receipt.OccurredAtMs
        };
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

    private static async ValueTask WriteFrameAsync<T>(
        Stream stream,
        PacketCommand command,
        JsonPayloadCodec<T> codec,
        T value,
        CancellationToken cancellationToken)
    {
        var payload = new ArrayBufferWriter<byte>();
        codec.Serialize(payload, value);

        var frame = new byte[
            PacketProtocol.HeaderSize + payload.WrittenCount];
        PacketParser.WriteHeader(
            frame,
            command,
            payload.WrittenCount);
        payload.WrittenSpan.CopyTo(
            frame.AsSpan(PacketProtocol.HeaderSize));

        await stream.WriteAsync(frame, cancellationToken);
    }

    private static async ValueTask WriteEmptyFrameAsync(
        Stream stream,
        PacketCommand command,
        CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(header, command, payloadLength: 0);
        await stream.WriteAsync(header, cancellationToken);
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

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(
                RealtimeAuthenticationResult.Success(
                    userId: 42,
                    sessionId: "integration-test",
                    userName: "test-user",
                    deviceIdHash,
                    roles: []));
    }

    private sealed class CapturingRealtimeMessageBus : IRealtimeMessageBus
    {
        public IncomingMessageCommand? LastIncomingMessage { get; private set; }
        public MessageReceiptCommand? LastReceipt { get; private set; }
        public RealtimeHistory.MessageHistoryQuery? LastHistoryQuery { get; private set; }
        public ConversationListQuery? LastConversationListQuery { get; private set; }
        public ConversationMarkReadCommand? LastConversationMarkRead { get; private set; }
        public SyncBootstrapQuery? LastSyncBootstrapQuery { get; private set; }

        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command,
            CancellationToken ct = default)
        {
            LastIncomingMessage = command;
            return Task.CompletedTask;
        }

        public Task PublishMessageReceiptAsync(
            MessageReceiptCommand command,
            CancellationToken ct = default)
        {
            LastReceipt = command;
            return Task.CompletedTask;
        }

        public Task<RealtimeHistory.MessageHistoryPage> QueryMessageHistoryAsync(
            RealtimeHistory.MessageHistoryQuery query,
            CancellationToken ct = default)
        {
            LastHistoryQuery = query;
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            return Task.FromResult(
                RealtimeHistory.MessageHistoryPage.Success(
                    query.RequestId,
                    [
                        new RealtimeHistory.RealtimeHistoryMessage
                        {
                            MessageId = LastIncomingMessage!.CommandId,
                            ClientMessageId = LastIncomingMessage.ClientMessageId,
                            SenderUserId = LastIncomingMessage.SenderUserId,
                            ReceiverUserId = LastIncomingMessage.ReceiverUserId,
                            Content = LastIncomingMessage.Content,
                            ReceivedAtMs = LastIncomingMessage.ReceivedAtMs,
                            DeliveredAtMs = now,
                            ReadAtMs = now
                        }
                    ],
                    nextCursor: null,
                    hasMore: false));
        }

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query,
            CancellationToken ct = default)
        {
            LastConversationListQuery = query;
            return Task.FromResult(
                ConversationListPage.Success(
                    query.RequestId,
                    [],
                    nextCursor: null,
                    hasMore: false));
        }

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(
            ConversationMarkReadCommand command,
            CancellationToken ct = default)
        {
            LastConversationMarkRead = command;
            return Task.FromResult(
                ConversationMarkReadResult.Success(
                    command.RequestId,
                    command.ConversationId,
                    unreadCount: 0,
                    lastReadMessageId: command.ReadMessageId,
                    lastReadAtMs: command.ReadAtMs,
                    changed: true));
        }

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
            CancellationToken ct = default)
        {
            LastSyncBootstrapQuery = query;
            return Task.FromResult(
                SyncBootstrapPage.Success(
                    query.RequestId,
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    [],
                    conversationsNextCursor: null,
                    conversationsHasMore: false,
                    catchUps: []));
        }

        public Task<RealtimeHistory.RealtimeHistoryMessage?> TryGetMessageByIdAsync(
            long userId,
            string messageId,
            CancellationToken ct = default)
        {
            var message = LastIncomingMessage;
            if (message is null ||
                !string.Equals(message.CommandId, messageId, StringComparison.Ordinal) ||
                (message.SenderUserId != userId && message.ReceiverUserId != userId))
            {
                return Task.FromResult<RealtimeHistory.RealtimeHistoryMessage?>(null);
            }

            return Task.FromResult<RealtimeHistory.RealtimeHistoryMessage?>(
                new RealtimeHistory.RealtimeHistoryMessage
                {
                    MessageId = message.CommandId,
                    ClientMessageId = message.ClientMessageId,
                    SenderUserId = message.SenderUserId,
                    ReceiverUserId = message.ReceiverUserId,
                    Content = message.Content,
                    ReceivedAtMs = message.ReceivedAtMs
                });
        }

        public Task PublishEventAsync(
            RealtimeEvent evt,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
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
            Task.FromResult(TimeSpan.Zero);
    }

    private sealed record ReceivedFrame(
        PacketCommand Command,
        byte[] Payload);
}
