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
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Relationships;
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

/// <summary>
/// 验证 CommandDispatcher 路径：Push / Reaction / 群已读回执 / 群解散命令通过 handler 处理，
/// 而非 TcpGatewayService 内联 switch。通过 TCP 端到端验证响应正确。
/// </summary>
public sealed class CommandDispatcherIntegrationTests
{
    [Fact(Timeout = 15_000)]
    public async Task DispatcherRoutesCommandsToHandlersOverTcp()
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

        var metrics = new GatewayMetrics();
        var userSessions = new UserSessionRegistry();
        var messageBus = new ReactionCapturingMessageBus();
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

        // Handler 专用 codec（与 service 内联 codec 分开，证明 handler 自带 codec 不依赖 service 字段）
        var registerPushRequestCodec = new JsonPayloadCodec<RegisterPushTokenRequest>(
            GatewayJsonSerializerContext.Default.RegisterPushTokenRequest);
        var registerPushResponseCodec = new JsonPayloadCodec<RegisterPushTokenResponse>(
            GatewayJsonSerializerContext.Default.RegisterPushTokenResponse);
        var unregisterPushRequestCodec = new JsonPayloadCodec<UnregisterPushTokenRequest>(
            GatewayJsonSerializerContext.Default.UnregisterPushTokenRequest);
        var unregisterPushResponseCodec = new JsonPayloadCodec<UnregisterPushTokenResponse>(
            GatewayJsonSerializerContext.Default.UnregisterPushTokenResponse);
        var addReactionRequestCodec = new JsonPayloadCodec<AddReactionRequest>(
            GatewayJsonSerializerContext.Default.AddReactionRequest);
        var addReactionAckCodec = new JsonPayloadCodec<AddReactionAcknowledgement>(
            GatewayJsonSerializerContext.Default.AddReactionAcknowledgement);
        var removeReactionRequestCodec = new JsonPayloadCodec<RemoveReactionRequest>(
            GatewayJsonSerializerContext.Default.RemoveReactionRequest);
        var removeReactionAckCodec = new JsonPayloadCodec<RemoveReactionAcknowledgement>(
            GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement);

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

        // 其余 6 个 handler：复用已有 codec 与依赖。本测试只断言 Push/Reaction 路径，
        // 但 CommandDispatcher 构造函数要求全部 8 个 handler 实例。
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

        var typingNotifyCodec = new JsonPayloadCodec<TypingNotify>(
            GatewayJsonSerializerContext.Default.TypingNotify);
        var typingFanout = new TypingFanoutCoordinator(TimeProvider.System);
        var typingHandler = new TypingCommandHandler(
            typingNotifyCodec,
            typingFanout,
            directConversationAuthorizer: null,
            Options.Create(options),
            NullLogger<TypingCommandHandler>.Instance);

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

        var integrationOptions = new RealtimeIntegrationOptions { InstanceId = "dispatcher-test" };
        var globalPresence = new NoopGlobalPresenceStore();
        var presenceWatchers = new PresenceWatcherRegistry();
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
            new JsonPayloadCodec<RelationshipListRequest>(
                GatewayJsonSerializerContext.Default.RelationshipListRequest),
            new JsonPayloadCodec<RelationshipListResponse>(
                GatewayJsonSerializerContext.Default.RelationshipListResponse),
            metrics,
            NullLogger<RelationshipCommandHandler>.Instance);

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
            relationshipHandler);

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
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
            await using var stream = client.GetStream();

            await AuthenticateAsync(stream, authenticationRequestCodec, authenticationResponseCodec, timeout.Token);

            // Push: 注册推送令牌。认证会话 DeviceIdHash=7，应成功。
            var registerRequestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.RegisterPushTokenRequest,
                registerPushRequestCodec,
                new RegisterPushTokenRequest
                {
                    RequestId = registerRequestId,
                    Platform = PushPlatform.Fcm,
                    Token = "fcm-dispatcher-test-token"
                },
                timeout.Token);

            var registerFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.RegisterPushTokenResponse, registerFrame.Command);
            var registerResponse = registerPushResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(registerFrame.Payload));
            Assert.NotNull(registerResponse);
            Assert.True(registerResponse.Succeeded);
            Assert.Equal(registerRequestId, registerResponse.RequestId);
            Assert.Equal(1, registerResponse.ActiveTokenCount);

            // 验证 store 状态确实被 handler 写入
            var tokens = await pushStore.ListAsync(42, timeout.Token);
            var token = Assert.Single(tokens);
            Assert.Equal("fcm-dispatcher-test-token", token.Token);

            // Reaction: 添加反应。fake bus 返回 Failed，验证 handler 仍正常返回 ack。
            var reactionRequestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.AddReactionRequest,
                addReactionRequestCodec,
                new AddReactionRequest
                {
                    RequestId = reactionRequestId,
                    MessageId = "msg-dispatcher-test",
                    Emoji = "👍"
                },
                timeout.Token);

            var reactionFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.AddReactionAck, reactionFrame.Command);
            var reactionAck = addReactionAckCodec.Deserialize(
                new ReadOnlySequence<byte>(reactionFrame.Payload));
            Assert.NotNull(reactionAck);
            Assert.Equal(reactionRequestId, reactionAck.RequestId);
            // CapturingMessageBus 返回 Failed，handler 应原样透传
            Assert.False(reactionAck.Succeeded);
            Assert.NotNull(messageBus.LastReactionCommand);
            Assert.Equal("msg-dispatcher-test", messageBus.LastReactionCommand!.MessageId);
            Assert.Equal("👍", messageBus.LastReactionCommand.Emoji);

            // 群已读回执：查询已读回执。fake bus 返回成功（小群 + 2 位 reader），验证 handler 映射。
            var receiptRequestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.MessageReadReceiptQueryRequest,
                messageReadReceiptQueryRequestCodec,
                new MessageReadReceiptQueryRequest
                {
                    RequestId = receiptRequestId,
                    ConversationId = "grp-dispatcher-test",
                    MessageId = "msg-read-receipt-1",
                    PageSize = 20
                },
                timeout.Token);

            var receiptFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.MessageReadReceiptQueryResponse, receiptFrame.Command);
            var receiptResponse = messageReadReceiptQueryResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(receiptFrame.Payload));
            Assert.NotNull(receiptResponse);
            Assert.True(receiptResponse.Succeeded);
            Assert.Equal(receiptRequestId, receiptResponse.RequestId);
            Assert.Equal("grp-dispatcher-test", receiptResponse.ConversationId);
            Assert.True(receiptResponse.IsSmallGroup);
            Assert.Equal(2, receiptResponse.ReadCount);
            Assert.Equal(10, receiptResponse.TotalMemberCount);
            Assert.NotNull(receiptResponse.Readers);
            Assert.Equal(2, receiptResponse.Readers!.Count);
            Assert.Equal(100, receiptResponse.Readers[0].UserId);
            Assert.Equal(200, receiptResponse.Readers[1].UserId);
            Assert.Equal(200L, receiptResponse.NextCursor);
            Assert.False(receiptResponse.HasMore);
            // 验证 handler 正确透传请求参数到 Realtime bus
            Assert.NotNull(messageBus.LastReadReceiptCommand);
            Assert.Equal("grp-dispatcher-test", messageBus.LastReadReceiptCommand!.ConversationId);
            Assert.Equal("msg-read-receipt-1", messageBus.LastReadReceiptCommand!.MessageId);
            Assert.Equal(42, messageBus.LastReadReceiptCommand!.ActorUserId);
            Assert.Equal(20, messageBus.LastReadReceiptCommand!.PageSize);

            // 群解散：fake bus 返回成功，验证 Operation=Dissolve 透传与响应映射。
            var dissolveRequestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.DissolveGroupRequest,
                dissolveGroupRequestCodec,
                new DissolveGroupRequest
                {
                    RequestId = dissolveRequestId,
                    ConversationId = "grp-dispatcher-test"
                },
                timeout.Token);

            var dissolveFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.DissolveGroupResponse, dissolveFrame.Command);
            var dissolveResponse = dissolveGroupResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(dissolveFrame.Payload));
            Assert.NotNull(dissolveResponse);
            Assert.True(dissolveResponse.Succeeded);
            Assert.Equal(dissolveRequestId, dissolveResponse.RequestId);
            Assert.Equal("grp-dispatcher-test", dissolveResponse.ConversationId);
            // 验证 handler 正确透传请求参数到 Realtime bus（仅 Owner 判定在 Realtime 侧）
            Assert.NotNull(messageBus.LastMutateCommand);
            Assert.Equal(GroupConversationOperation.Dissolve, messageBus.LastMutateCommand!.Operation);
            Assert.Equal("grp-dispatcher-test", messageBus.LastMutateCommand!.ConversationId);
            Assert.Equal(42, messageBus.LastMutateCommand!.ActorUserId);

            // 群解散参数无效：空 ConversationId → invalid_request 失败响应。
            var badDissolveRequestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.DissolveGroupRequest,
                dissolveGroupRequestCodec,
                new DissolveGroupRequest
                {
                    RequestId = badDissolveRequestId,
                    ConversationId = ""
                },
                timeout.Token);

            var badDissolveFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.DissolveGroupResponse, badDissolveFrame.Command);
            var badDissolveResponse = dissolveGroupResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(badDissolveFrame.Payload));
            Assert.NotNull(badDissolveResponse);
            Assert.False(badDissolveResponse.Succeeded);
            Assert.Equal("invalid_request", badDissolveResponse.ErrorCode);
            Assert.Equal(badDissolveRequestId, badDissolveResponse.RequestId);
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

        var frame = await ReadFrameAsync(stream, cancellationToken);
        Assert.Equal(PacketCommand.AuthenticationResponse, frame.Command);
        var response = responseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        Assert.NotNull(response);
        Assert.True(response.Success);
        Assert.Equal(42, response.UserId);
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
        {
            await stream.ReadExactlyAsync(payload, cancellationToken);
        }

        return new ReceivedFrame(command, payload);
    }

    private sealed record ReceivedFrame(PacketCommand Command, byte[] Payload);

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(
                RealtimeAuthenticationResult.Success(
                    userId: 42,
                    sessionId: "dispatcher-test",
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

    private sealed class ReactionCapturingMessageBus : IRealtimeMessageBus
    {
        public MessageReactionCommand? LastReactionCommand { get; private set; }

        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishMessageReceiptAsync(
            MessageReceiptCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<MessageHistoryPage> QueryMessageHistoryAsync(
            MessageHistoryQuery query, CancellationToken ct = default) =>
            Task.FromResult(MessageHistoryPage.Failed(query.RequestId, "x", "x"));

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query, CancellationToken ct = default) =>
            Task.FromResult(ConversationListPage.Failed(query.RequestId, "x", "x"));

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(
            ConversationMarkReadCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationMarkReadResult.Failed(command.RequestId, "x", "x"));

        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
            ConversationSetPrefsCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationSetPrefsResult.Failed(command.RequestId, "x", "x"));

        public GroupConversationCommand? LastMutateCommand { get; private set; }

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command, CancellationToken ct = default)
        {
            LastMutateCommand = command;
            return Task.FromResult(
                command.Operation == GroupConversationOperation.Dissolve
                    ? GroupConversationResult.Success(command.RequestId, command.ConversationId!)
                    : GroupConversationResult.Failed(command.RequestId, "x", "x"));
        }

        public GroupConversationCommand? LastReadReceiptCommand { get; private set; }

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command, CancellationToken ct = default)
        {
            LastReadReceiptCommand = command;
            return Task.FromResult(GroupConversationResult.SuccessReadReceipt(
                command.RequestId,
                command.ConversationId!,
                readCount: 2,
                totalMemberCount: 10,
                isSmallGroup: true,
                readers: new[]
                {
                    new MessageReader { UserId = 100, ReadAtMs = 1700000000000L },
                    new MessageReader { UserId = 200, ReadAtMs = 1700000000001L }
                },
                nextCursor: 200,
                hasMore: false));
        }

        public Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(
            AttachmentFinalizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentFinalizeResult.Failed(command.RequestId, "x", "x"));

        public Task<AttachmentDownloadAuthorizeResult> AuthorizeAttachmentDownloadAsync(
            AttachmentDownloadAuthorizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipCommandResult> MutateRelationshipAsync(
            RelationshipCommand command, CancellationToken ct = default) =>
            Task.FromResult(RelationshipCommandResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipListResult> QueryRelationshipListAsync(
            RelationshipListQuery query, CancellationToken ct = default) =>
            Task.FromResult(RelationshipListResult.Failed(query.RequestId, "x", "x"));

        public Task<MessageRecallResult> RecallMessageAsync(
            MessageRecallCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageRecallResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageEditResult> EditMessageAsync(
            MessageEditCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageEditResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageReactionResult> ReactToMessageAsync(
            MessageReactionCommand command, CancellationToken ct = default)
        {
            LastReactionCommand = command;
            return Task.FromResult(MessageReactionResult.Failed(command.RequestId, "not_used", "not used"));
        }

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query, CancellationToken ct = default) =>
            Task.FromResult(SyncBootstrapPage.Failed(query.RequestId, "x", "x"));

        public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
            long userId, string messageId, CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistoryMessage?>(null);

        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default) =>
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

        public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(
            PresenceAuthorizeQuery query, CancellationToken ct = default) =>
            Task.FromResult(new PresenceAuthorizeResponse { AllowedUserIds = query.TargetUserIds });

        public Task ServePresenceAuthorizeAsync(
            Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishPushDeliveryAsync(PushDeliveryCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<PushDelivery> ConsumePushDeliveriesAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}
