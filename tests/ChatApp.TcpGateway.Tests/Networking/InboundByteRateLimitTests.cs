using System.Net;
using System.Net.Sockets;
using ChatApp.TcpGateway.Diagnostics;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class InboundByteRateLimitTests
{
    [Fact]
    public async Task RecordInboundTrafficRejectsWhenByteBudgetExceeded()
    {
        await using var harness = await SessionHarness.CreateAsync();

        Assert.True(
            harness.Session.RecordInboundTraffic(
                maximumPacketsPerSecond: 100,
                maximumBytesPerSecond: 1_000,
                frameByteCount: 600));
        Assert.False(
            harness.Session.RecordInboundTraffic(
                maximumPacketsPerSecond: 100,
                maximumBytesPerSecond: 1_000,
                frameByteCount: 500));
    }

    [Fact]
    public async Task RecordInboundTrafficRejectsWhenPacketBudgetExceeded()
    {
        await using var harness = await SessionHarness.CreateAsync();

        Assert.True(
            harness.Session.RecordInboundTraffic(
                maximumPacketsPerSecond: 2,
                maximumBytesPerSecond: 1_000_000,
                frameByteCount: 10));
        Assert.True(
            harness.Session.RecordInboundTraffic(
                maximumPacketsPerSecond: 2,
                maximumBytesPerSecond: 1_000_000,
                frameByteCount: 10));
        Assert.False(
            harness.Session.RecordInboundTraffic(
                maximumPacketsPerSecond: 2,
                maximumBytesPerSecond: 1_000_000,
                frameByteCount: 10));
    }

    private sealed class SessionHarness : IAsyncDisposable
    {
        private readonly Socket _listener;
        private readonly Socket _client;
        private readonly Socket _accepted;
        private readonly GatewayMetrics _metrics;

        private SessionHarness(
            Socket listener,
            Socket client,
            Socket accepted,
            GatewayMetrics metrics,
            TcpClientSession session)
        {
            _listener = listener;
            _client = client;
            _accepted = accepted;
            _metrics = metrics;
            Session = session;
        }

        public TcpClientSession Session { get; }

        public static async Task<SessionHarness> CreateAsync()
        {
            var listener = new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp);
            listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            listener.Listen(1);

            var client = new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp);
            await client.ConnectAsync(listener.LocalEndPoint!);
            var accepted = await listener.AcceptAsync();

            var metrics = new GatewayMetrics();
            var session = new TcpClientSession(
                accepted,
                connectionId: 1,
                outboundQueueCapacity: 8,
                maxOutboundQueuedBytes: 64 * 1024,
                sendTimeout: TimeSpan.FromSeconds(1),
                timeProvider: TimeProvider.System,
                metrics: metrics,
                logger: NullLogger<TcpClientSession>.Instance);

            return new SessionHarness(listener, client, accepted, metrics, session);
        }

        public async ValueTask DisposeAsync()
        {
            await Session.DisposeAsync();
            _metrics.Dispose();
            _accepted.Dispose();
            _client.Dispose();
            _listener.Dispose();
        }
    }
}
