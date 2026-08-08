using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
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
/// P1-3：附件下载授权命令端到端验证。客户端发送 AttachmentDownloadAuthorizeRequest，
/// 经 CommandDispatcher → AttachmentCommandHandler → 后端（成功桩）签发下载 URL，
/// 返回 AttachmentDownloadAuthorizeResponse 验证序列化 + handler + backend 全链路。
/// </summary>
public sealed class AttachmentDownloadAuthorizeTests
{
    [Fact(Timeout = 15_000)]
    public async Task DownloadAuthorizeCommand_RoundTrips()
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
        var messageBus = new NoopRealtimeMessageBus();

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

        var integrationOptions = new RealtimeIntegrationOptions { InstanceId = "download-authorize-test" };
        var globalPresence = new NoopGlobalPresenceStore();
        var presenceWatchers = new PresenceWatcherRegistry();
        var typingFanout = new TypingFanoutCoordinator(TimeProvider.System);
        var pushStore = new InMemoryPushTokenStore();

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
        var typingUpdateCodec = new JsonPayloadCodec<TypingUpdate>(
            GatewayJsonSerializerContext.Default.TypingUpdate);
        var presenceChangedCodec = new JsonPayloadCodec<PresenceChanged>(
            GatewayJsonSerializerContext.Default.PresenceChanged);
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

        var downloadAuthorizeRequestCodec = new JsonPayloadCodec<AttachmentDownloadAuthorizeRequest>(
            GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeRequest);
        var downloadAuthorizeResponseCodec = new JsonPayloadCodec<AttachmentDownloadAuthorizeResponse>(
            GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeResponse);
        var attachmentHandler = new AttachmentCommandHandler(
            new SuccessAttachmentBackend(),
            new JsonPayloadCodec<AttachmentFinalizeRequest>(
                GatewayJsonSerializerContext.Default.AttachmentFinalizeRequest),
            new JsonPayloadCodec<AttachmentFinalizeResponse>(
                GatewayJsonSerializerContext.Default.AttachmentFinalizeResponse),
            downloadAuthorizeRequestCodec,
            downloadAuthorizeResponseCodec,
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

            var requestId = Guid.CreateVersion7().ToString("N");
            await WriteFrameAsync(
                stream,
                PacketCommand.AttachmentDownloadAuthorizeRequest,
                downloadAuthorizeRequestCodec,
                new AttachmentDownloadAuthorizeRequest
                {
                    RequestId = requestId,
                    AttachmentId = "attach-download-1",
                    ConversationId = "conv-1"
                },
                timeout.Token);

            var frame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.AttachmentDownloadAuthorizeResponse, frame.Command);
            var response = downloadAuthorizeResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(frame.Payload));
            Assert.NotNull(response);
            Assert.True(response.Succeeded);
            Assert.Equal(requestId, response.RequestId);
            Assert.Equal("attach-download-1", response.AttachmentId);
            Assert.Equal("https://cdn.example.com/attachments/attach-download-1?signature=abc", response.DownloadUrl);
            Assert.Equal("token-abc", response.DownloadToken);
            Assert.NotNull(response.ExpiresAtMs);
            Assert.Null(response.ErrorCode);
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

    /// <summary>成功签发下载 URL 的附件后端桩。</summary>
    private sealed class SuccessAttachmentBackend : IAttachmentBackend
    {
        public Task<AttachmentFinalizeBackendResult> FinalizeUploadAsync(
            string requestId,
            long actorUserId,
            string attachmentId,
            long sizeBytes,
            string? contentHash,
            string? actorSessionId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(AttachmentFinalizeBackendResult.Success(requestId, attachmentId, 4));

        public Task<AttachmentDownloadAuthorizeBackendResult> AuthorizeDownloadAsync(
            string requestId,
            long actorUserId,
            string attachmentId,
            string? conversationId,
            string? actorSessionId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeBackendResult.Success(
                requestId,
                attachmentId,
                "https://cdn.example.com/attachments/" + attachmentId + "?signature=abc",
                "token-abc",
                expiresAtMs: 1_800_000_000_000L));
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
                    sessionId: "download-authorize-test",
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

    private sealed class NoopRealtimeMessageBus : IRealtimeMessageBus
    {
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

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

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
            MessageReactionCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageReactionResult.Failed(command.RequestId, "x", "x"));

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query, CancellationToken ct = default) =>
            Task.FromResult(SyncBootstrapPage.Failed(query.RequestId, "x", "x"));

        public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
            long userId, string messageId, CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistoryMessage?>(null);

        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default) =>
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

        public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
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
