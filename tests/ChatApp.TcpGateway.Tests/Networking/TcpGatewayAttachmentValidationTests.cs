using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Configuration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Networking;
using ChatApp.TcpGateway.Networking.Sessions;
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
                MaxChatAttachments = ChatMessageLimits.MaxAttachments
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
            var conversationListRequestCodec = new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest);
            var conversationListResponseCodec = new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse);
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
            var addReactionRequestCodec = new JsonPayloadCodec<AddReactionRequest>(
                GatewayJsonSerializerContext.Default.AddReactionRequest);
            var addReactionAcknowledgementCodec = new JsonPayloadCodec<AddReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.AddReactionAcknowledgement);
            var removeReactionRequestCodec = new JsonPayloadCodec<RemoveReactionRequest>(
                GatewayJsonSerializerContext.Default.RemoveReactionRequest);
            var removeReactionAcknowledgementCodec = new JsonPayloadCodec<RemoveReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement);
            var syncBootstrapRequestCodec = new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest);
            var syncBootstrapResponseCodec = new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse);

            var metrics = new GatewayMetrics();
            var service = new TcpGatewayService(
                Options.Create(options),
                new FakeAuthenticator(),
                authenticationRequestCodec,
                authenticationResponseCodec,
                chatMessageCodec,
                acknowledgementCodec,
                receiptRequestCodec,
                receiptAcknowledgementCodec,
                historyRequestCodec,
                historyResponseCodec,
                conversationListRequestCodec,
                conversationListResponseCodec,
                conversationMarkReadRequestCodec,
                conversationMarkReadResponseCodec,
                conversationSetPrefsRequestCodec,
                conversationSetPrefsResponseCodec,
                messageRecallRequestCodec,
                messageRecallAcknowledgementCodec,
                messageEditRequestCodec,
                messageEditAcknowledgementCodec,
                addReactionRequestCodec,
                addReactionAcknowledgementCodec,
                removeReactionRequestCodec,
                removeReactionAcknowledgementCodec,
                syncBootstrapRequestCodec,
                syncBootstrapResponseCodec,
                messageBus,
                new ChatApp.Realtime.Integration.Configuration.RealtimeIntegrationOptions
                {
                    InstanceId = "test-gateway-attach"
                },
                new NoopLeaseStore(),
                new NoopGlobalPresenceStore(),
                new UserSessionRegistry(),
                new PresenceWatcherRegistry(),
                new TypingFanoutCoordinator(TimeProvider.System),
                metrics,
                TimeProvider.System,
                NullLogger<TcpGatewayService>.Instance,
                NullLogger<TcpClientSession>.Instance);

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
        public ValueTask<string?> TakeOverAsync(
            long userId,
            ulong deviceIdHash,
            string sessionId,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>(null);

        public ValueTask ReleaseIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string sessionId,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
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

        public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}
