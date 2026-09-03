using System.Buffers;
using System.Text.Json;
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
using ChatApp.TcpGateway.Gateway.Diagnostics;
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
using RealtimeEventDispatcher = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventDispatcher;
using RealtimeHistory =
    ChatApp.Realtime.Abstractions.Messaging.History;
using TcpCallCommandRequest = ChatApp.Shared.Protocol.Tcp.TcpCallCommandRequest;
using TcpCallCommandResponse = ChatApp.Shared.Protocol.Tcp.TcpCallCommandResponse;
using TcpCallSignal = ChatApp.Shared.Protocol.Tcp.TcpCallSignal;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// BIN-INTEGRATION-3 端到端集成测试：真 TcpGatewayService + 真 TCP 回环 + 真 codec。
/// <para>
/// 覆盖连接级 chatapp-bin-v1 协商（ServerHello.PayloadFormat）、二进制会话上的
/// 认证/心跳/业务往返、JSON fallback、Resume 保持 JSON、混合格式 fanout、
/// 畸形二进制 payload fail-closed、未知命令拒绝与停机 GoAway。
/// 握手段（ClientHello/ServerHello）恒 JSON；客户端帧用共享包
/// <see cref="TcpBinaryWireEncoder"/>/<see cref="TcpBinaryWireCodec"/> + 网关
/// <see cref="BinaryPayloadMapper"/>（internal，经 Gateway 程序集
/// InternalsVisibleTo("ChatApp.TcpGateway.Tests") 可见）编解码。
/// </para>
/// </summary>
[Collection("TcpSessionSerial")]
public sealed class BinaryPayloadNegotiationTests
{
    private const string JsonFormat = ProtocolPayloadFormat.Json;
    private const string BinaryFormat = BinaryPayloadFormat.Id;

    // ──────────── 场景 1：协商成功 + 二进制认证 + 心跳 ────────────

    [Fact(Timeout = 15_000)]
    public async Task NegotiatesBinaryFormat_ThenAuthenticatesAndHeartbeatsOverBinary()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            // ClientHello 恒 JSON；仅声明 CommandCapabilities | BinaryPayload，
            // ServerHello.FeatureBits 应精确等于二者交集。
            var serverHello = await HandshakeAsync(
                stream,
                featureBits: (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload),
                resumeToken: null,
                timeout.Token);
            Assert.Equal(BinaryFormat, serverHello.PayloadFormat);
            Assert.Equal(
                (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload),
                serverHello.FeatureBits);

            // 二进制会话上认证必须走 chatapp-bin-v1。
            await WriteBinaryFrameAsync(
                stream,
                PacketCommand.AuthenticationRequest,
                new AuthenticationRequest
                {
                    AccessToken = "valid-token",
                    DeviceIdHash = 7
                },
                timeout.Token);

            var authenticationFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(
                PacketCommand.AuthenticationResponse,
                authenticationFrame.Command);
            var authentication = DecodeBinaryPayload<AuthenticationResponse>(
                PacketCommand.AuthenticationResponse,
                authenticationFrame.Payload);
            Assert.True(authentication.Success);
            Assert.Equal(42, authentication.UserId);
            Assert.Equal("integration-test", authentication.SessionId);

            // 会话可用：心跳照常应答。
            await WriteEmptyFramesAsync(
                stream,
                PacketCommand.Heartbeat,
                count: 2,
                timeout.Token);
            for (var index = 0; index < 2; index++)
            {
                var heartbeatFrame = await ReadFrameAsync(stream, timeout.Token);
                Assert.Equal(
                    PacketCommand.HeartbeatAcknowledgement,
                    heartbeatFrame.Command);
            }
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 2：JSON fallback ────────────

    [Fact(Timeout = 15_000)]
    public async Task FallsBackToJson_WhenClientDoesNotAdvertiseBinaryPayload()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            var serverHello = await HandshakeAsync(
                stream,
                featureBits: (uint)GatewayFeature.CommandCapabilities,
                resumeToken: null,
                timeout.Token);
            Assert.Equal(JsonFormat, serverHello.PayloadFormat);
            Assert.Equal(
                (uint)GatewayFeature.CommandCapabilities,
                serverHello.FeatureBits);

            // JSON 会话照常认证，心跳作为"会话可用"探针。
            await AuthenticateAsJsonAsync(stream, deviceIdHash: 7, timeout.Token);
            await WriteEmptyFramesAsync(
                stream,
                PacketCommand.Heartbeat,
                count: 1,
                timeout.Token);

            var heartbeatFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(
                PacketCommand.HeartbeatAcknowledgement,
                heartbeatFrame.Command);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task FallsBackToJson_WhenOptionDisabled_EvenIfClientAdvertisesBinaryPayload()
    {
        var port = ReserveLoopbackPort();
        // EnableBinaryPayloadFormat 默认 false。
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: false);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            var serverHello = await HandshakeAsync(
                stream,
                featureBits: (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload),
                resumeToken: null,
                timeout.Token);
            Assert.Equal(JsonFormat, serverHello.PayloadFormat);
            Assert.Equal(
                (uint)GatewayFeature.CommandCapabilities,
                serverHello.FeatureBits);

            await AuthenticateAsJsonAsync(stream, deviceIdHash: 7, timeout.Token);
            await WriteEmptyFramesAsync(
                stream,
                PacketCommand.Heartbeat,
                count: 1,
                timeout.Token);
            var heartbeatFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(
                PacketCommand.HeartbeatAcknowledgement,
                heartbeatFrame.Command);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 3：Resume 保持 JSON ────────────

    [Fact(Timeout = 15_000)]
    public async Task KeepsJsonFormat_WhenClientHelloCarriesResumeToken()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true,
            resumeTokenStore: new RejectingResumeTokenStore());
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            await WriteJsonFrameAsync(
                stream,
                PacketCommand.ClientHello,
                new ClientHello
                {
                    ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
                    FeatureBits = (uint)(GatewayFeature.CommandCapabilities |
                                         GatewayFeature.SessionResume |
                                         GatewayFeature.BinaryPayload),
                    InstallationId = "binary-negotiation-test",
                    ResumeToken = "forged-resume-token"
                },
                timeout.Token);

            // 无效 token：Resume 失败 → Error(ResumeFailed) → 仍回 JSON ServerHello。
            var errorFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.Error, errorFrame.Command);
            var error = gateway.ProtocolErrorCodec.Deserialize(
                new ReadOnlySequence<byte>(errorFrame.Payload));
            Assert.NotNull(error);
            Assert.Equal(ProtocolErrorCode.ResumeFailed, error.Code);

            var serverHello = await ReadServerHelloAsync(stream, timeout.Token);
            Assert.Equal(JsonFormat, serverHello.PayloadFormat);
            Assert.True(
                GatewayFeatureSet.ContainsAll(
                    serverHello.FeatureBits,
                    GatewayFeature.SessionResume),
                $"unexpected feature bits {serverHello.FeatureBits}");

            // 失败的 Resume 之后会话按 JSON 完成握手，完整认证可用。
            await AuthenticateAsJsonAsync(stream, deviceIdHash: 7, timeout.Token);
            await WriteEmptyFramesAsync(
                stream,
                PacketCommand.Heartbeat,
                count: 1,
                timeout.Token);
            var heartbeatFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(
                PacketCommand.HeartbeatAcknowledgement,
                heartbeatFrame.Command);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 4：二进制业务往返 ────────────

    [Fact(Timeout = 15_000)]
    public async Task BinaryChatMessageRoundTrips_AcknowledgementAndGatewaySideParse()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            await PerformBinaryHandshakeAndAuthenticationAsync(
                stream,
                timeout.Token);

            var clientMessageId = Guid.CreateVersion7().ToString("N");
            await WriteBinaryFrameAsync(
                stream,
                PacketCommand.ChatMessage,
                new ChatMessage
                {
                    MessageId = clientMessageId,
                    ClientMessageId = clientMessageId,
                    TargetUserId = 42,
                    Content = "hello over chatapp-bin-v1"
                },
                timeout.Token);

            var acknowledgementFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(
                PacketCommand.MessageAcknowledgement,
                acknowledgementFrame.Command);
            var acknowledgement = DecodeBinaryPayload<MessageAcknowledgement>(
                PacketCommand.MessageAcknowledgement,
                acknowledgementFrame.Payload);
            Assert.True(acknowledgement.Accepted);
            Assert.Equal(clientMessageId, acknowledgement.ClientMessageId);
            Assert.False(string.IsNullOrEmpty(acknowledgement.CommandId));

            // 网关侧解析字段：映射层把二进制 payload 还原成本地 ChatMessage。
            var command = Assert.IsType<IncomingMessageCommand>(
                gateway.Bus.LastIncomingMessage);
            Assert.Equal(clientMessageId, command.ClientMessageId);
            Assert.Equal("hello over chatapp-bin-v1", command.Content);
            Assert.Equal(42, command.SenderUserId);
            Assert.Equal(42, command.ReceiverUserId);
            Assert.Equal(command.CommandId, acknowledgement.CommandId);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 5：混合格式 fanout ────────────

    [Fact(Timeout = 20_000)]
    public async Task MixedFormatFanout_DeliversJsonToJsonSessionAndBinaryToBinarySession()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

            // 同一用户 42 的两个会话：一个 JSON 协商、一个 binary 协商。
            // DeviceIdHash 必须不同，避免同设备替换把先到的会话踢下线。
            using var jsonClient = await ConnectAsync(port, timeout.Token);
            var jsonStream = jsonClient.GetStream();
            var jsonServerHello = await HandshakeAsync(
                jsonStream,
                featureBits: (uint)GatewayFeature.CommandCapabilities,
                resumeToken: null,
                timeout.Token);
            Assert.Equal(JsonFormat, jsonServerHello.PayloadFormat);
            await AuthenticateAsJsonAsync(jsonStream, deviceIdHash: 7, timeout.Token);

            using var binaryClient = await ConnectAsync(port, timeout.Token);
            var binaryStream = binaryClient.GetStream();
            await PerformBinaryHandshakeAndAuthenticationAsync(
                binaryStream,
                timeout.Token,
                deviceIdHash: 8);

            // 注入一条 Realtime MessageReceived 事件，目标是用户 42。
            var dispatcher = CreateRealtimeEventDispatcher(gateway);
            const string fanoutContent = "fanout-hello";
            await dispatcher.DispatchAsync(
                new RealtimeEvent
                {
                    EventId = "fanout-event-1",
                    Type = RealtimeEventType.MessageReceived,
                    TargetUserId = 42,
                    ActorUserId = 42,
                    MessageId = "fanout-message-1",
                    SessionId = "upstream-persist-session",
                    PayloadJson = $$"""
                        {
                          "messageId": "fanout-message-1",
                          "clientMessageId": "fanout-client-1",
                          "senderUserId": 42,
                          "senderSessionId": "upstream-persist-session",
                          "receiverUserId": 42,
                          "content": "{{fanoutContent}}",
                          "receivedAtMs": 1000
                        }
                        """,
                    OccurredAtMs = 1000
                },
                TestContext.Current.CancellationToken);

            // JSON 会话收到 JSON 帧。
            var jsonFrame = await ReadFrameAsync(jsonStream, timeout.Token);
            Assert.Equal(PacketCommand.ChatMessage, jsonFrame.Command);
            Assert.Equal(
                (byte)'{',
                jsonFrame.Payload[0]);
            var jsonMessage = gateway.ChatMessageCodec.Deserialize(
                new ReadOnlySequence<byte>(jsonFrame.Payload));
            Assert.NotNull(jsonMessage);
            Assert.Equal(fanoutContent, jsonMessage.Content);
            Assert.Equal(42, jsonMessage.SenderUserId);

            // binary 会话收到二进制帧，解码出语义一致的字段。
            var binaryFrame = await ReadFrameAsync(binaryStream, timeout.Token);
            Assert.Equal(PacketCommand.ChatMessage, binaryFrame.Command);
            Assert.NotEqual(
                (byte)'{',
                binaryFrame.Payload[0]);
            var binaryMessage = DecodeBinaryPayload<ChatMessage>(
                PacketCommand.ChatMessage,
                binaryFrame.Payload);
            Assert.Equal(fanoutContent, binaryMessage.Content);
            Assert.Equal(42, binaryMessage.SenderUserId);
            Assert.Equal(42, binaryMessage.TargetUserId);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 6：fail-closed ────────────

    [Fact(Timeout = 15_000)]
    public async Task MalformedBinaryPayload_ClosesConnection()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            await HandshakeAsync(
                stream,
                featureBits: (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload),
                resumeToken: null,
                timeout.Token);

            // 畸形二进制 payload（非法 tag/varint）：Inline lane 解码抛
            // BinaryPayloadDecodeException → 协议错误关连接。
            await WriteRawFrameAsync(
                stream,
                PacketCommand.AuthenticationRequest,
                [0xFF, 0xFF, 0xFF, 0xFF],
                timeout.Token);

            await WaitForServerCloseAsync(stream, timeout.Token);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task UnknownCommandValue_RejectedWithProtocolErrorThenClosed()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            await HandshakeAsync(
                stream,
                featureBits: (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload),
                resumeToken: null,
                timeout.Token);

            // 未登记的命令值：包头解析即拒绝（catalog 未定义）。
            // RejectInvalidPacket 入队 Error(closeAfterSend) 后同步 Close()——
            // Socket 关闭与 SendLoop 排空存在竞争，Error 帧可能被丢弃；
            // fail-closed 的硬保证是"连接被关闭"，Error 帧到达时必须可按会话格式解码。
            await WriteRawFrameAsync(
                stream,
                (PacketCommand)999,
                [],
                timeout.Token);

            try
            {
                var errorFrame = await ReadFrameAsync(stream, timeout.Token);
                Assert.Equal(PacketCommand.Error, errorFrame.Command);
                var error = DecodeBinaryPayload<ProtocolErrorFrame>(
                    PacketCommand.Error,
                    errorFrame.Payload);
                Assert.Equal(ProtocolErrorCode.ProtocolViolation, error.Code);
                Assert.True(error.Fatal);
            }
            catch (Exception exception)
                when (exception is IOException or SocketException)
            {
                // Error 帧被关闭竞争丢弃：直接观察到连接关闭，同样满足 fail-closed。
            }

            await WaitForServerCloseAsync(stream, timeout.Token);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 场景 7：GoAway 二进制 ────────────

    [Fact(Timeout = 20_000)]
    public async Task StopAsync_DeliversBinaryEncodedGoAwayToBinarySession()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true,
            goAwayDrainTimeout: TimeSpan.FromMilliseconds(600));
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var client = await ConnectAsync(port, timeout.Token);
            var stream = client.GetStream();

            await PerformBinaryHandshakeAndAuthenticationAsync(
                stream,
                timeout.Token);

            var stopTask = service.StopAsync(CancellationToken.None);

            var goAwayFrame = await ReadFrameAsync(stream, timeout.Token);
            Assert.Equal(PacketCommand.GoAway, goAwayFrame.Command);
            var goAway = DecodeBinaryPayload<GoAway>(
                PacketCommand.GoAway,
                goAwayFrame.Payload);
            Assert.Equal("shutdown", goAway.Reason);
            Assert.Equal(600, goAway.RetryAfterMs);

            await stopTask;
            // drain 结束后服务端关闭会话。
            await WaitForServerCloseAsync(stream, timeout.Token);
        }
        finally
        {
            // 幂等兜底：断言失败时也保证服务停机。
            await service.StopAsync(CancellationToken.None);
        }
    }

    // ──────────── 组装：真 TcpGatewayService + 真 handler 图 ────────────

    private sealed record GatewayFixture(
        TcpGatewayService Service,
        CapturingRealtimeMessageBus Bus,
        GatewayMetrics Metrics,
        UserSessionRegistry UserSessions,
        JsonPayloadCodec<ChatMessage> ChatMessageCodec,
        JsonPayloadCodec<ProtocolErrorFrame> ProtocolErrorCodec);

    private static GatewayFixture CreateGateway(
        int port,
        bool enableBinaryPayloadFormat,
        TimeSpan? goAwayDrainTimeout = null,
        IResumeTokenStore? resumeTokenStore = null)
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
            EnableBinaryPayloadFormat = enableBinaryPayloadFormat,
            GoAwayDrainTimeout = goAwayDrainTimeout ?? TimeSpan.FromSeconds(5)
        };

        var metrics = new GatewayMetrics();
        var userSessions = new UserSessionRegistry();
        var messageBus = new CapturingRealtimeMessageBus();
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
            InstanceId = "binary-negotiation-gateway"
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
            resumeTokenStore: resumeTokenStore,
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
            chatMessageCodec,
            protocolErrorCodec);
    }

    private static RealtimeEventDispatcher CreateRealtimeEventDispatcher(
        GatewayFixture gateway) =>
        new(
            gateway.UserSessions,
            gateway.ChatMessageCodec,
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
            gateway.Metrics,
            TimeProvider.System,
            NullLogger<RealtimeEventDispatcher>.Instance);

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
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

    /// <summary>TryClaim 恒失败的 ResumeTokenStore：模拟无效/伪造 token。</summary>
    private sealed class RejectingResumeTokenStore : IResumeTokenStore
    {
        public Task<string> IssueAsync(
            ResumeContext context,
            TimeSpan ttl,
            CancellationToken ct = default) =>
            Task.FromResult("issued-token");

        public Task<ResumeContext?> TryValidateAsync(
            string resumeToken,
            CancellationToken ct = default) =>
            Task.FromResult<ResumeContext?>(null);

        public Task RevokeAsync(string resumeToken, CancellationToken ct = default) =>
            Task.CompletedTask;
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

    // ──────────── 客户端原语：帧读写 / 二进制编解码 / 等待关闭 ────────────

    // ──────────── 场景 8：超限命令 payload 两格式统一早投拒绝（对称契约） ────────────

    // ChatMessage 的命令级 payload 上限是 64KiB（CommandCatalog），帧校验先于解码：
    // 超限帧在两条格式下都必须得到相同的对称契约——按会话格式的 rejected ack
    // （payload_too_large）后关闭连接。

    [Fact(Timeout = 15_000)]
    public async Task OversizedChatMessageFrame_SymmetricEarlyReject_OnBothFormats()
    {
        var port = ReserveLoopbackPort();
        var gateway = CreateGateway(
            port,
            enableBinaryPayloadFormat: true);
        using var metrics = gateway.Metrics;
        using var service = gateway.Service;
        await service.StartAsync(CancellationToken.None);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var oversized = new string('x', 64 * 1024 + 1024);

            // 二进制会话：早投 rejected ack 后连接关闭。
            using var binaryClient = await ConnectAsync(port, timeout.Token);
            var binaryStream = binaryClient.GetStream();
            await PerformBinaryHandshakeAndAuthenticationAsync(binaryStream, timeout.Token);

            var binaryMessageId = Guid.CreateVersion7().ToString("N");
            await WriteRelaxedBinaryFrameAsync(
                binaryStream,
                PacketCommand.ChatMessage,
                new ChatMessage
                {
                    MessageId = binaryMessageId,
                    ClientMessageId = binaryMessageId,
                    TargetUserId = 42,
                    Content = oversized
                },
                timeout.Token);

            // 早投拒绝存在既知的 close/send 竞争（RejectOversizedPayload TryQueue 后同步关闭）：
            // ack 帧可能到达，也可能直接观察到关闭。两者都符合契约。
            await ReadUntilClosedAsync(
                binaryStream,
                PacketCommand.MessageAcknowledgement,
                expectAccepted: false,
                timeout.Token);

            // JSON 会话：同一超限帧得到同样的对称契约。
            using var jsonClient = await ConnectAsync(port, timeout.Token);
            var jsonStream = jsonClient.GetStream();
            var jsonServerHello = await HandshakeAsync(
                jsonStream,
                featureBits: (uint)GatewayFeature.CommandCapabilities,
                resumeToken: null,
                timeout.Token);
            Assert.Equal(JsonFormat, jsonServerHello.PayloadFormat);
            await AuthenticateAsJsonAsync(jsonStream, deviceIdHash: 7, timeout.Token);

            await WriteJsonFrameAsync(
                jsonStream,
                PacketCommand.ChatMessage,
                new ChatMessage
                {
                    MessageId = Guid.CreateVersion7().ToString("N"),
                    ClientMessageId = Guid.CreateVersion7().ToString("N"),
                    TargetUserId = 42,
                    Content = oversized
                },
                timeout.Token);

            await ReadUntilClosedAsync(
                jsonStream,
                PacketCommand.MessageAcknowledgement,
                expectAccepted: false,
                json: true,
                cancellationToken: timeout.Token);

            // 边界内（<64KiB 命令上限）的正常正文：两条格式都正常 accepted。
            var withinBudget = new string('x', 60 * 1024);
            using var withinClient = await ConnectAsync(port, timeout.Token);
            var withinStream = withinClient.GetStream();
            await PerformBinaryHandshakeAndAuthenticationAsync(withinStream, timeout.Token);
            var withinId = Guid.CreateVersion7().ToString("N");
            await WriteRelaxedBinaryFrameAsync(
                withinStream,
                PacketCommand.ChatMessage,
                new ChatMessage
                {
                    MessageId = withinId,
                    ClientMessageId = withinId,
                    TargetUserId = 42,
                    Content = withinBudget
                },
                timeout.Token);
            var withinFrame = await ReadFrameAsync(withinStream, timeout.Token);
            Assert.Equal(PacketCommand.MessageAcknowledgement, withinFrame.Command);
            var withinAck = DecodeBinaryPayload<MessageAcknowledgement>(
                PacketCommand.MessageAcknowledgement,
                withinFrame.Payload);
            Assert.True(withinAck.Accepted);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>断言对端已关闭连接：读到 0 字节或 Socket/IO 异常，超时视为失败。</summary>
    private static async Task AssertConnectionClosedAsync(Stream stream, CancellationToken cancellationToken)
    {
        try
        {
            var buffer = new byte[16];
            var read = await stream.ReadAsync(buffer, cancellationToken);
            Assert.True(read == 0, $"预期连接关闭，实际读到 {read} 字节");
        }
        catch (Exception ex) when (
            ex is SocketException ||
            ex is IOException ||
            ex is OperationCanceledException)
        {
            // 连接被强制关闭/中止：符合"关闭"预期。
        }
    }

    /// <summary>
    /// 读取直到连接关闭。早投拒绝存在既知的 close/send 竞争：
    /// 若 ack/错误帧到达则按 <paramref name="expectAccepted"/> 断言后继续等关闭；
    /// 若直接观察到关闭（帧被 close 竞争丢弃）同样符合契约。
    /// </summary>
    private static async Task ReadUntilClosedAsync(
        Stream stream,
        PacketCommand expectedCommand,
        bool expectAccepted,
        CancellationToken cancellationToken,
        bool json = false)
    {
        try
        {
            while (true)
            {
                var frame = await ReadFrameAsync(stream, cancellationToken);
                if (frame.Command == expectedCommand)
                {
                    if (json)
                    {
                        var ack = JsonSerializer.Deserialize(
                            frame.Payload,
                            GatewayJsonSerializerContext.Default.MessageAcknowledgement);
                        Assert.NotNull(ack);
                        Assert.Equal(expectAccepted, ack!.Accepted);
                    }
                    else
                    {
                        var decoded = DecodeBinaryPayload<MessageAcknowledgement>(
                            PacketCommand.MessageAcknowledgement,
                            frame.Payload);
                        Assert.Equal(expectAccepted, decoded.Accepted);
                    }
                }
            }
        }
        catch (Exception ex) when (
            ex is IOException ||
            ex is SocketException ||
            ex is OperationCanceledException)
        {
            // 连接关闭：契约的终态。
        }
    }

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
                InstallationId = "binary-negotiation-test",
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

    private static async Task<ServerHello> ReadServerHelloAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        var serverHelloFrame = await ReadFrameAsync(stream, cancellationToken);
        Assert.Equal(PacketCommand.ServerHello, serverHelloFrame.Command);
        return JsonServerHelloCodec.Deserialize(
            new ReadOnlySequence<byte>(serverHelloFrame.Payload))!;
    }

    private static readonly JsonPayloadCodec<ServerHello> JsonServerHelloCodec =
        new(GatewayJsonSerializerContext.Default.ServerHello);

    private static readonly JsonPayloadCodec<ClientHello> JsonClientHelloCodec =
        new(GatewayJsonSerializerContext.Default.ClientHello);

    private static readonly JsonPayloadCodec<AuthenticationRequest> JsonAuthenticationRequestCodec =
        new(GatewayJsonSerializerContext.Default.AuthenticationRequest);

    private static readonly JsonPayloadCodec<AuthenticationResponse> JsonAuthenticationResponseCodec =
        new(GatewayJsonSerializerContext.Default.AuthenticationResponse);

    /// <summary>JSON 会话认证 + 发一个心跳作为"会话可用"探针。</summary>
    private static async Task AuthenticateAsJsonAsync(
        Stream stream,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        await WriteJsonFrameAsync(
            stream,
            PacketCommand.AuthenticationRequest,
            new AuthenticationRequest
            {
                AccessToken = "valid-token",
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
        Assert.Equal(42, authentication.UserId);
    }

    /// <summary>协商 binary + 二进制认证（可指定 DeviceIdHash）。</summary>
    private static async Task PerformBinaryHandshakeAndAuthenticationAsync(
        Stream stream,
        CancellationToken cancellationToken,
        ulong deviceIdHash = 7)
    {
        var serverHello = await HandshakeAsync(
            stream,
            featureBits: (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload),
            resumeToken: null,
            cancellationToken);
        Assert.Equal(BinaryFormat, serverHello.PayloadFormat);

        await WriteBinaryFrameAsync(
            stream,
            PacketCommand.AuthenticationRequest,
            new AuthenticationRequest
            {
                AccessToken = "valid-token",
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
        Assert.Equal(42, authentication.UserId);
    }

    private static byte[] EncodeBinaryPayload<T>(PacketCommand command, T value)
        where T : class
    {
        var shared = BinaryPayloadMapper.ToShared(command, value);
        // 模拟"宽松预算的生产者"（如遗留客户端/直连工具）：字符串域放宽到帧预算，
        // 用于验证服务端对 >64KiB 正文的拒绝语义（rejected ack + 连接保持），
        // 而非依赖共享库默认 64KiB 限制在客户端编码阶段先失败。
        var relaxedLimits = new BinaryLimits(
            maxMessageBytes: 80 * 1024,
            maxFieldBytes: 80 * 1024,
            maxStringBytes: 80 * 1024,
            maxByteArrayBytes: 64 * 1024,
            maxFields: 256);
        var buffer = new byte[relaxedLimits.MaxMessageBytes];
        var encode = TcpBinaryWireEncoder.TryEncode(
            shared,
            buffer,
            relaxedLimits);
        Assert.Equal(TcpBinaryWireEncodeStatus.Encoded, encode.Status);
        return buffer.AsSpan(0, encode.Written).ToArray();
    }

    /// <summary>
    /// 以"宽松预算生产者"（字符串域放宽到帧预算）编码并发送二进制帧，
    /// 用于验证服务端对超出命令级上限帧的早投拒绝语义。
    /// </summary>
    private static async ValueTask WriteRelaxedBinaryFrameAsync<T>(
        Stream stream,
        PacketCommand command,
        T value,
        CancellationToken cancellationToken)
        where T : class
    {
        var shared = BinaryPayloadMapper.ToShared(command, value);
        var relaxedLimits = new BinaryLimits(
            maxMessageBytes: 80 * 1024,
            maxFieldBytes: 80 * 1024,
            maxStringBytes: 80 * 1024,
            maxByteArrayBytes: 64 * 1024,
            maxFields: 256);
        var buffer = new byte[relaxedLimits.MaxMessageBytes];
        var encode = TcpBinaryWireEncoder.TryEncode(shared, buffer, relaxedLimits);
        Assert.Equal(TcpBinaryWireEncodeStatus.Encoded, encode.Status);
        await WriteRawFrameAsync(
            stream,
            command,
            buffer.AsSpan(0, encode.Written).ToArray(),
            cancellationToken);
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

    private static async ValueTask WriteJsonFrameAsync<T>(
        Stream stream,
        PacketCommand command,
        T value,
        CancellationToken cancellationToken)
    {
        var writer = new ArrayBufferWriter<byte>();
        JsonPayloadCodecFor<T>().Serialize(writer, value);

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

    private static JsonPayloadCodec<T> JsonPayloadCodecFor<T>() => typeof(T) switch
    {
        var t when t == typeof(ClientHello) =>
            (JsonPayloadCodec<T>)(object)JsonClientHelloCodec,
        var t when t == typeof(AuthenticationRequest) =>
            (JsonPayloadCodec<T>)(object)JsonAuthenticationRequestCodec,
        var t when t == typeof(ChatMessage) =>
            (JsonPayloadCodec<T>)(object)new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage),
        _ => throw new InvalidOperationException(
            $"no cached json codec for {typeof(T).Name}")
    };

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

    private static async ValueTask WriteEmptyFramesAsync(
        Stream stream,
        PacketCommand command,
        int count,
        CancellationToken cancellationToken)
    {
        var frames = new byte[PacketProtocol.HeaderSize * count];
        for (var index = 0; index < count; index++)
        {
            PacketParser.WriteHeader(
                frames.AsSpan(
                    index * PacketProtocol.HeaderSize,
                    PacketProtocol.HeaderSize),
                command,
                payloadLength: 0);
        }

        await stream.WriteAsync(frames, cancellationToken);
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

    /// <summary>
    /// 等待服务端关闭连接：读返回 0（FIN）或抛 IO/Socket 异常（RST）均视为已关闭；
    /// 取消（超时未关闭）视为失败。
    /// </summary>
    private static async Task WaitForServerCloseAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[1];
        try
        {
            var read = await stream.ReadAsync(buffer, cancellationToken);
            Assert.Equal(0, read);
        }
        catch (Exception exception)
            when (exception is IOException or SocketException)
        {
            // RST / 管道中断同样视为服务端已关闭。
        }
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
