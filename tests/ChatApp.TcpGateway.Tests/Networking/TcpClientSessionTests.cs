using System.Net.Sockets;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// <see cref="TcpClientSession"/> 独立单测：验证状态机、限流、admission 跟踪与
/// 连接标识——不依赖真实网络 I/O，覆盖 <see cref="TcpGatewayServiceTests"/> 未细化的 per-session 行为。
/// </summary>
public sealed class TcpClientSessionTests
{
    [Fact]
    public async Task NewSession_IsConnected_AndNotAuthenticated()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        Assert.True(session.IsConnected);
        Assert.False(session.IsAuthenticated);
        Assert.False(session.HasCompletedHandshake);
        Assert.Equal(AdmissionState.Unauthenticated, session.AdmissionState);
        Assert.Equal(SessionCloseReason.None, session.CloseReason);
    }

    [Fact]
    public async Task Authenticate_SetsIdentity_AndMarksAuthenticated()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.Authenticate(
            userId: 1001,
            sessionId: "sess-1001",
            deviceIdHash: 0xAA,
            deviceId: "dev-1");

        Assert.True(session.IsAuthenticated);
        Assert.Equal(1001L, session.UserId);
        Assert.Equal("sess-1001", session.SessionId);
        Assert.Equal((ulong?)0xAA, session.DeviceIdHash);
        Assert.Equal("dev-1", session.DeviceId);
    }

    [Fact]
    public async Task Authenticate_WithNullSessionId_GeneratesDefaultId()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics, connectionId: 42u);

        session.Authenticate(userId: 1, sessionId: null, deviceIdHash: null);

        Assert.Equal("tcp-42", session.SessionId);
    }

    [Fact]
    public async Task MarkAdmissionPromoted_TransitionsUnauthenticatedToPromoted()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        Assert.Equal(AdmissionState.Unauthenticated, session.AdmissionState);
        Assert.False(session.AdmissionPromoted);

        session.MarkAdmissionPromoted();

        Assert.Equal(AdmissionState.Promoted, session.AdmissionState);
        Assert.True(session.AdmissionPromoted);
    }

    [Fact]
    public async Task MarkAdmissionPromoted_IsIdempotent_AfterPromoted()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.MarkAdmissionPromoted();
        session.MarkAdmissionPromoted(); // 重复调用 no-op

        Assert.Equal(AdmissionState.Promoted, session.AdmissionState);
    }

    [Fact]
    public async Task TryReleaseAdmission_ReturnsTrue_WhenPromotedToReleased()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.MarkAdmissionPromoted();
        var released = session.TryReleaseAdmission();

        Assert.True(released);
        Assert.Equal(AdmissionState.Released, session.AdmissionState);
    }

    [Fact]
    public async Task TryReleaseAdmission_ReturnsFalse_WhenAlreadyReleased()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.MarkAdmissionPromoted();
        Assert.True(session.TryReleaseAdmission());
        // 重复释放不重复递减已认证计数
        Assert.False(session.TryReleaseAdmission());
        Assert.Equal(AdmissionState.Released, session.AdmissionState);
    }

    [Fact]
    public async Task TryReleaseAdmission_ReturnsFalse_WhenUnauthenticated()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // Unauthenticated → Released 不允许（必须先 Promoted）
        Assert.False(session.TryReleaseAdmission());
        Assert.Equal(AdmissionState.Unauthenticated, session.AdmissionState);
    }

    [Fact]
    public async Task CompleteHandshake_SetsNegotiatedVersionAndFeatures()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.CompleteHandshake(
            protocolVersion: 2,
            featureBits: (uint)GatewayFeature.CommandCapabilities | (uint)GatewayFeature.SessionResume);

        Assert.True(session.HasCompletedHandshake);
        Assert.Equal(2, session.NegotiatedProtocolVersion);
        Assert.True((session.NegotiatedFeatureBits & (uint)GatewayFeature.CommandCapabilities) != 0);
        Assert.True((session.NegotiatedFeatureBits & (uint)GatewayFeature.SessionResume) != 0);
    }

    [Fact]
    public async Task AllowsFeature_ReturnsTrue_WhenCommandCapabilitiesNotNegotiated()
    {
        // v1 兼容：未协商 CommandCapabilities 时，所有命令放行
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);

        Assert.True(session.AllowsFeature(GatewayFeature.SessionResume));
        Assert.True(session.AllowsFeature(GatewayFeature.PresenceAndTyping));
    }

    [Fact]
    public async Task AllowsFeature_ReturnsTrue_WhenFeatureNegotiated()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.CompleteHandshake(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities | (uint)GatewayFeature.SessionResume);

        Assert.True(session.AllowsFeature(GatewayFeature.SessionResume));
    }

    [Fact]
    public async Task AllowsFeature_ReturnsFalse_WhenFeatureNotNegotiated()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.CompleteHandshake(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities); // 未包含 SessionResume

        Assert.False(session.AllowsFeature(GatewayFeature.SessionResume));
        Assert.True(session.AllowsFeature(GatewayFeature.CommandCapabilities));
    }

    [Fact]
    public async Task Close_SetsCloseReason_AndMarksDisconnected()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.Close(SessionCloseReason.ProtocolViolation);

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task Close_IsIdempotent_KeepsFirstReason()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.Close(SessionCloseReason.ProtocolViolation);
        session.Close(SessionCloseReason.RemoteClosed); // 第二次 no-op

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task ConnectionLeaseId_IsUniqueAndFormatted()
    {
        using var metrics = new GatewayMetrics();
        await using var session1 = CreateSession(metrics, connectionId: 1);
        await using var session2 = CreateSession(metrics, connectionId: 2);

        var lease1 = session1.ConnectionLeaseId;
        var lease2 = session2.ConnectionLeaseId;

        Assert.NotEqual(lease1, lease2);
        Assert.Equal(32, lease1.Length); // GUID "N" 格式 = 32 hex chars
        Assert.Equal(32, lease2.Length);
        // 同一 session 多次访问返回缓存值
        Assert.Same(lease1, session1.ConnectionLeaseId);
    }

    [Fact]
    public async Task LeaseOwnerToken_IsUniqueAndFormatted()
    {
        using var metrics = new GatewayMetrics();
        await using var session1 = CreateSession(metrics, connectionId: 1);
        await using var session2 = CreateSession(metrics, connectionId: 2);

        var token1 = session1.LeaseOwnerToken;
        var token2 = session2.LeaseOwnerToken;

        Assert.NotEqual(token1, token2);
        Assert.Equal(32, token1.Length);
        // ConnectionLeaseId 与 LeaseOwnerToken 必须不同（最小权限原则）
        Assert.NotEqual(session1.ConnectionLeaseId, session1.LeaseOwnerToken);
    }

    [Fact]
    public async Task RecordInboundTraffic_FirstCall_InitializesFullBucket()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // 首次调用初始化满桶，单帧应放行
        var allowed = session.RecordInboundTraffic(
            maximumPacketsPerSecond: 10,
            maximumBytesPerSecond: 1024,
            frameByteCount: 100,
            packetCost: 1);

        Assert.True(allowed);
    }

    [Fact]
    public async Task RecordInboundTraffic_ReturnsFalse_WhenPacketBudgetExhausted()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // 桶容量 10 包，连续 11 个包应在第 11 个被拒
        for (var i = 0; i < 10; i++)
        {
            Assert.True(session.RecordInboundTraffic(
                maximumPacketsPerSecond: 10,
                maximumBytesPerSecond: 1024 * 1024,
                frameByteCount: 10,
                packetCost: 1));
        }

        Assert.False(session.RecordInboundTraffic(
            maximumPacketsPerSecond: 10,
            maximumBytesPerSecond: 1024 * 1024,
            frameByteCount: 10,
            packetCost: 1));
    }

    [Fact]
    public async Task RecordInboundTraffic_ReturnsFalse_WhenByteBudgetExhausted()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // 字节桶容量 1000，每帧 200 字节 → 5 帧后第 6 帧被拒
        for (var i = 0; i < 5; i++)
        {
            Assert.True(session.RecordInboundTraffic(
                maximumPacketsPerSecond: 1000,
                maximumBytesPerSecond: 1000,
                frameByteCount: 200,
                packetCost: 1));
        }

        Assert.False(session.RecordInboundTraffic(
            maximumPacketsPerSecond: 1000,
            maximumBytesPerSecond: 1000,
            frameByteCount: 200,
            packetCost: 1));
    }

    [Fact]
    public async Task RecordInboundTraffic_PacketCostConsumesMoreTokens()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        // 桶容量 10 包，packetCost=5 → 2 帧后第 3 帧被拒
        Assert.True(session.RecordInboundTraffic(
            maximumPacketsPerSecond: 10,
            maximumBytesPerSecond: 1024 * 1024,
            frameByteCount: 10,
            packetCost: 5));
        Assert.True(session.RecordInboundTraffic(
            maximumPacketsPerSecond: 10,
            maximumBytesPerSecond: 1024 * 1024,
            frameByteCount: 10,
            packetCost: 5));
        Assert.False(session.RecordInboundTraffic(
            maximumPacketsPerSecond: 10,
            maximumBytesPerSecond: 1024 * 1024,
            frameByteCount: 10,
            packetCost: 5));
    }

    [Fact]
    public async Task RecordInboundTraffic_RejectsNegativeFrameBytes()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            session.RecordInboundTraffic(
                maximumPacketsPerSecond: 10,
                maximumBytesPerSecond: 1024,
                frameByteCount: -1,
                packetCost: 1));
    }

    [Fact]
    public async Task DisposeAsync_ClosesSession_AndReleasesResources()
    {
        using var metrics = new GatewayMetrics();
        var session = CreateSession(metrics);

        await session.DisposeAsync();

        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ApplicationStopping, session.CloseReason);
    }

    [Fact]
    public async Task LifetimeToken_CancelledOnClose()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        Assert.False(session.LifetimeToken.IsCancellationRequested);

        session.Close(SessionCloseReason.RemoteClosed);

        Assert.True(session.LifetimeToken.IsCancellationRequested);
    }

    [Fact]
    public async Task ConnectionAge_AndLastInboundAge_ArePositive()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        Assert.True(session.ConnectionAge >= TimeSpan.Zero);
        Assert.True(session.LastInboundAge >= TimeSpan.Zero);
    }

    [Fact]
    public async Task CurrentResumeToken_CanBeSetAndRead()
    {
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(metrics);

        session.CurrentResumeToken = "token-abc";
        Assert.Equal("token-abc", session.CurrentResumeToken);
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
}
