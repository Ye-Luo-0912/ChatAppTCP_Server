using System.Buffers;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// <see cref="SessionControlHandler"/> 独立单测：验证连接状态机命令（ClientHello/AuthenticationRequest）
/// 的协议校验、版本协商、认证成功/失败/超时路径——不启动真实 TCP 监听器。
/// </summary>
public sealed class SessionControlHandlerTests
{
    private static readonly RealtimeIntegrationOptions IntegrationOptions = new()
    {
        InstanceId = "sch-test"
    };

    [Fact]
    public async Task TryHandleAsync_NonControlCommand_ReturnsFalse()
    {
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var handled = await handler.TryHandleAsync(
            PacketCommand.Heartbeat,
            ReadOnlySequence<byte>.Empty,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(handled);
        Assert.True(session.IsConnected); // 未被关闭
    }

    [Fact]
    public async Task AuthenticationRequest_BeforeHandshake_ClosesProtocolViolation()
    {
        // RequireClientHello=true（默认）：未握手时发送 AuthenticationRequest → 致命协议违例
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var payload = SerializeAuthRequest("token", null);

        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task AuthenticationRequest_AfterHandshake_WithEmptyToken_FailsAuth()
    {
        // 握手完成后，空 AccessToken → 认证失败（不关闭连接，由 SendAuthenticationFailure 处理）
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        var payload = SerializeAuthRequest("", null);

        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        // 空 token → 认证失败，SendAuthenticationFailure 以 closeAfterSend=AuthenticationRejected 关闭
        Assert.False(session.IsAuthenticated);
    }

    [Fact]
    public async Task AuthenticationRequest_AfterHandshake_WithNullPayload_FailsAuth()
    {
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        // 空 payload → codec.Deserialize 返回 null → 视为空 token
        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            ReadOnlySequence<byte>.Empty,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsAuthenticated);
    }

    [Fact]
    public async Task AuthenticationRequest_WhenAlreadyAuthenticated_ClosesProtocolViolation()
    {
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(1001, "sess", 0xAA, "dev");

        var payload = SerializeAuthRequest("token", null);

        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task AuthenticationRequest_WithSuccessfulAuth_PromotesAdmissionAndAuthenticates()
    {
        var authenticator = new FakeAuthenticator
        {
            Result = RealtimeAuthenticationResult.Success(
                userId: 2001,
                sessionId: "auth-sess",
                userName: "user",
                deviceIdHash: 0xBB,
                deviceId: "dev-2")
        };
        var (handler, _) = CreateHandler(authenticator);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        var payload = SerializeAuthRequest("valid-token", 0xBB);

        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.True(session.IsAuthenticated);
        Assert.Equal(2001L, session.UserId);
        Assert.Equal("auth-sess", session.SessionId);
        Assert.Equal(AdmissionState.Promoted, session.AdmissionState);
    }

    [Fact]
    public async Task AuthenticationRequest_WithFailedAuth_DoesNotAuthenticate()
    {
        var authenticator = new FakeAuthenticator
        {
            Result = RealtimeAuthenticationResult.Failure("bad token")
        };
        var (handler, _) = CreateHandler(authenticator);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        var payload = SerializeAuthRequest("bad-token", null);

        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsAuthenticated);
        Assert.Equal(AdmissionState.Unauthenticated, session.AdmissionState);
    }

    [Fact]
    public async Task AuthenticationRequest_WhenAuthTimesOut_ClosesAuthenticationTimedOut()
    {
        var authenticator = new FakeAuthenticator
        {
            // 模拟认证超时：当 linked CTS 取消时抛出 OperationCanceledException
            OnAuthenticate = (_, _, ct) =>
            {
                ct.WaitHandle.WaitOne(TimeSpan.FromSeconds(5));
                ct.ThrowIfCancellationRequested();
                return ValueTask.FromResult(RealtimeAuthenticationResult.Failure("timeout"));
            }
        };
        var (handler, _) = CreateHandler(authenticator, authenticationTimeout: TimeSpan.FromMilliseconds(100));
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        var payload = SerializeAuthRequest("slow-token", null);

        await handler.TryHandleAsync(
            PacketCommand.AuthenticationRequest,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.AuthenticationTimedOut, session.CloseReason);
    }

    [Fact]
    public async Task ClientHello_WhenAlreadyAuthenticated_ClosesProtocolViolation()
    {
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.Authenticate(1001, "sess", 0xAA, "dev");

        var payload = SerializeClientHello(protocolVersion: 1, featureBits: 0);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task ClientHello_WhenHandshakeAlreadyCompleted_ClosesProtocolViolation()
    {
        var (handler, _) = CreateHandler();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        var payload = SerializeClientHello(protocolVersion: 1, featureBits: 0);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task ClientHello_WithoutInjectedCodecs_SilentlySkips()
    {
        // 测试场景：codec/identity 未注入时静默跳过握手（v1 兼容回退）
        var handler = CreateHandlerWithoutHandshake();
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var payload = SerializeClientHello(protocolVersion: 1, featureBits: 0);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        // 静默跳过：连接保持打开，握手未完成
        Assert.True(session.IsConnected);
        Assert.False(session.HasCompletedHandshake);
    }

    [Fact]
    public async Task ClientHello_WithVersionTooHigh_ClosesProtocolViolation()
    {
        var (handler, _) = CreateHandler(serverProtocolVersion: 1);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // 客户端版本 5 > 服务端版本 1
        var payload = SerializeClientHello(protocolVersion: 5, featureBits: 0);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task ClientHello_WithVersionBelowMinimum_ClosesProtocolViolation()
    {
        var (handler, _) = CreateHandler(
            serverProtocolVersion: 2,
            minimumClientProtocolVersion: 2);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // 客户端版本 1 < 最低版本 2
        var payload = SerializeClientHello(protocolVersion: 1, featureBits: 0);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task ClientHello_WithValidVersion_CompletesHandshake()
    {
        var (handler, _) = CreateHandler(serverProtocolVersion: 1);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var payload = SerializeClientHello(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.True(session.IsConnected);
        Assert.True(session.HasCompletedHandshake);
        Assert.Equal(1, session.NegotiatedProtocolVersion);
        // CommandCapabilities 应进入协商结果
        Assert.True((session.NegotiatedFeatureBits & (uint)GatewayFeature.CommandCapabilities) != 0);
    }

    [Fact]
    public async Task ClientHello_NegotiatesFeatureIntersection()
    {
        // 客户端请求 CommandCapabilities|SessionResume，服务端支持两者 → 均协商通过
        var (handler, _) = CreateHandler(serverProtocolVersion: 1);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var payload = SerializeClientHello(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities |
                         (uint)GatewayFeature.SessionResume);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.True(session.HasCompletedHandshake);
        Assert.True((session.NegotiatedFeatureBits & (uint)GatewayFeature.SessionResume) != 0);
    }

    [Fact]
    public async Task ClientHello_DisableResume_RemovesSessionResumeFromNegotiatedBits()
    {
        var (handler, _) = CreateHandler(
            serverProtocolVersion: 1,
            enableResume: false);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var payload = SerializeClientHello(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities |
                         (uint)GatewayFeature.SessionResume);

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        Assert.True(session.HasCompletedHandshake);
        // EnableResume=false 时 SessionResume 位被剥离
        Assert.False((session.NegotiatedFeatureBits & (uint)GatewayFeature.SessionResume) != 0);
    }

    [Fact]
    public async Task ClientHello_WithInvalidPayload_ThrowsJsonException()
    {
        // 无效 JSON payload → codec.Deserialize 抛 JsonException，由调用方（SessionRuntime）捕获并关闭连接。
        // SessionControlHandler 不内联捕获 JsonException，保持与 Inline lane 一致的"调用方处理"边界。
        var (handler, _) = CreateHandler(serverProtocolVersion: 1);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var invalidJson = new ReadOnlySequence<byte>(
            System.Text.Encoding.UTF8.GetBytes("{invalid"));

        await Assert.ThrowsAsync<JsonException>(() =>
            handler.TryHandleAsync(
                PacketCommand.ClientHello,
                invalidJson,
                session,
                "127.0.0.1",
                CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task ClientHello_WithResumeTokenNotNegotiated_SendsProtocolError()
    {
        // 严格能力模式：客户端未协商 SessionResume 但携带 ResumeToken → FeatureNotNegotiated 错误
        // 但不关闭连接（非致命），继续发送 ServerHello
        var (handler, _) = CreateHandler(
            serverProtocolVersion: 1,
            enableResume: true);
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        var payload = SerializeClientHello(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities, // 未包含 SessionResume
            resumeToken: "some-token");

        await handler.TryHandleAsync(
            PacketCommand.ClientHello,
            payload,
            session,
            "127.0.0.1",
            CancellationToken.None);

        // FeatureNotNegotiated 非致命，连接保持打开，握手继续完成
        Assert.True(session.IsConnected);
        Assert.True(session.HasCompletedHandshake);
    }

    private static (SessionControlHandler Handler, TcpListenerHost Listener) CreateHandler(
        FakeAuthenticator? authenticator = null,
        TimeSpan? authenticationTimeout = null,
        ushort serverProtocolVersion = 1,
        ushort minimumClientProtocolVersion = 1,
        bool enableResume = true)
    {
        authenticator ??= new FakeAuthenticator();

        var options = new TcpGatewayOptions
        {
            RequireClientHello = true,
            AuthenticationTimeout = authenticationTimeout ?? TimeSpan.FromSeconds(5),
            EnableResume = enableResume,
            EnableEphemeralPresenceAndTyping = false,
            MinimumClientProtocolVersion = minimumClientProtocolVersion,
            IdleTimeout = TimeSpan.FromSeconds(90)
        };

        using var metrics = new GatewayMetrics();
        var listenerHost = CreateListenerHost(options, metrics);
        var coordinator = CreateCoordinator(metrics, options);

        var handler = new SessionControlHandler(
            options,
            authenticator,
            new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest),
            new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse),
            new JsonPayloadCodec<ClientHello>(
                GatewayJsonSerializerContext.Default.ClientHello),
            new JsonPayloadCodec<ServerHello>(
                GatewayJsonSerializerContext.Default.ServerHello),
            new JsonPayloadCodec<ResumeResponse>(
                GatewayJsonSerializerContext.Default.ResumeResponse),
            new JsonPayloadCodec<ProtocolErrorFrame>(
                GatewayJsonSerializerContext.Default.ProtocolErrorFrame),
            new TestServerIdentity(serverProtocolVersion),
            listenerHost,
            coordinator,
            metrics,
            NullLogger.Instance);

        return (handler, listenerHost);
    }

    private static SessionControlHandler CreateHandlerWithoutHandshake()
    {
        using var metrics = new GatewayMetrics();
        var options = new TcpGatewayOptions
        {
            RequireClientHello = false,
            AuthenticationTimeout = TimeSpan.FromSeconds(5),
            EnableResume = false,
            EnableEphemeralPresenceAndTyping = false
        };
        var listenerHost = CreateListenerHost(options, metrics);
        var coordinator = CreateCoordinator(metrics, options);

        return new SessionControlHandler(
            options,
            new FakeAuthenticator(),
            new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest),
            new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse),
            clientHelloCodec: null,
            serverHelloCodec: null,
            resumeResponseCodec: null,
            protocolErrorFrameCodec: null,
            serverIdentity: null,
            listenerHost,
            coordinator,
            metrics,
            NullLogger.Instance);
    }

    private static TcpListenerHost CreateListenerHost(TcpGatewayOptions options, GatewayMetrics metrics)
    {
        return new TcpListenerHost(
            options,
            metrics,
            NullLogger.Instance,
            goAwayCodec: null,
            getSessions: () => Array.Empty<TcpClientSession>(),
            onConnectionAccepted: (_, _, _, _) =>
                ValueTask.FromResult<Task?>(Task.CompletedTask));
    }

    private static SessionLifecycleCoordinator CreateCoordinator(
        GatewayMetrics metrics,
        TcpGatewayOptions options)
    {
        return new SessionLifecycleCoordinator(
            new FakeDeviceSessionLeaseStore(),
            new NoopGlobalPresenceStore(),
            resumeTokenStore: null,
            new UserSessionRegistry(),
            new PresenceWatcherRegistry(),
            new TcpGatewayServiceCompositionTests.EmptyMessageBus(),
            IntegrationOptions,
            options,
            metrics,
            TimeProvider.System,
            NullLogger.Instance,
            new JsonPayloadCodec<PresenceChanged>(
                GatewayJsonSerializerContext.Default.PresenceChanged));
    }

    private static TcpClientSession CreateSession(GatewayMetrics metrics, uint connectionId = 1)
    {
        var socket = new Socket(
            AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);

        return new TcpClientSession(
            socket: socket,
            connectionId: connectionId,
            outboundQueueCapacity: 16,
            maxOutboundQueuedBytes: 4096,
            sendTimeout: TimeSpan.FromSeconds(5),
            timeProvider: TimeProvider.System,
            metrics: metrics,
            logger: NullLogger<TcpClientSession>.Instance,
            globalOutboundBudget: null,
            authenticationTimeout: default,
            deadlineWheel: null,
            idleTimeout: default,
            outboundPump: null);
    }

    private static ReadOnlySequence<byte> SerializeAuthRequest(
        string accessToken,
        ulong? deviceIdHash)
    {
        var request = new AuthenticationRequest
        {
            AccessToken = accessToken,
            DeviceIdHash = deviceIdHash
        };
        return Serialize(request, GatewayJsonSerializerContext.Default.AuthenticationRequest);
    }

    private static ReadOnlySequence<byte> SerializeClientHello(
        ushort protocolVersion,
        uint featureBits,
        string? resumeToken = null)
    {
        var hello = new ClientHello
        {
            ProtocolVersion = protocolVersion,
            FeatureBits = featureBits,
            ResumeToken = resumeToken
        };
        return Serialize(hello, GatewayJsonSerializerContext.Default.ClientHello);
    }

    private static ReadOnlySequence<byte> Serialize<T>(
        T value,
        JsonTypeInfo<T> typeInfo)
    {
        var writer = new ArrayBufferWriter<byte>(256);
        var codec = new JsonPayloadCodec<T>(typeInfo);
        codec.Serialize(writer, value);
        return new ReadOnlySequence<byte>(writer.WrittenMemory.ToArray());
    }

    private sealed class TestServerIdentity : IServerIdentity
    {
        public TestServerIdentity(ushort protocolVersion)
        {
            ProtocolVersion = protocolVersion;
        }

        public string ServerDeviceId { get; } = "test-server-device-id";
        public ushort ProtocolVersion { get; }
        public uint FeatureBits { get; } =
            (uint)GatewayFeatureSet.Implemented;
    }

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {
        public RealtimeAuthenticationResult Result { get; set; } =
            RealtimeAuthenticationResult.Failure("not configured");

        public Func<string, ulong?, CancellationToken, ValueTask<RealtimeAuthenticationResult>>?
            OnAuthenticate { get; set; }

        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default)
        {
            return OnAuthenticate?.Invoke(accessToken, deviceIdHash, cancellationToken)
                ?? ValueTask.FromResult(Result);
        }
    }

    private sealed class FakeDeviceSessionLeaseStore : IDeviceSessionLeaseStore
    {
        public ValueTask<TakeOverResult> TakeOverAsync(
            long userId, ulong deviceIdHash, string sessionId,
            string transportId, string leaseOwnerToken, TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(TakeOverResult.NoPreviousLease());

        public ValueTask ReleaseIfOwnerAsync(
            long userId, ulong deviceIdHash, string leaseOwnerToken,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> RefreshIfOwnerAsync(
            long userId, ulong deviceIdHash, string leaseOwnerToken,
            TimeSpan ttl, CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask<string?> GetCurrentSessionIdAsync(
            long userId, ulong deviceIdHash,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>(null);
    }
}
