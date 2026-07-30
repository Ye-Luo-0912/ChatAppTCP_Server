using System.Net.Sockets;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class FrameAssemblyTimeoutTrackerTests
{
    [Fact]
    public async Task OnAssemblyStartedRegistersAndReturnsState()
    {
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker();
        await using var session = CreateSession(metrics);

        var state = tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(5));

        Assert.NotNull(state);
        Assert.Equal(1, tracker.ActiveAssemblyCount);
    }

    [Fact]
    public async Task OnAssemblyCompletedRemovesEntryWhenStateMatches()
    {
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker();
        await using var session = CreateSession(metrics);

        var state = tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(5));
        tracker.OnAssemblyCompleted(session, state);

        Assert.Equal(0, tracker.ActiveAssemblyCount);
    }

    [Fact]
    public async Task OnAssemblyCompletedWithStaleStateDoesNotRemoveEntry()
    {
        // ABA protection: if OnAssemblyStarted was called again (phase change),
        // the old state reference must not remove the new registration.
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker();
        await using var session = CreateSession(metrics);

        var oldState = tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(5));
        // Phase change: OnAssemblyStarted creates a new state object.
        var newState = tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(10));

        Assert.NotSame(oldState, newState);
        Assert.Equal(1, tracker.ActiveAssemblyCount);

        // Completing with the OLD state must NOT remove the entry (newState is active).
        tracker.OnAssemblyCompleted(session, oldState);
        Assert.Equal(1, tracker.ActiveAssemblyCount);

        // Completing with the NEW state removes the entry.
        tracker.OnAssemblyCompleted(session, newState);
        Assert.Equal(0, tracker.ActiveAssemblyCount);
    }

    [Fact]
    public async Task OnSessionClosedRemovesEntry()
    {
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker();
        await using var session = CreateSession(metrics);

        tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(5));
        Assert.Equal(1, tracker.ActiveAssemblyCount);

        tracker.OnSessionClosed(session);
        Assert.Equal(0, tracker.ActiveAssemblyCount);
    }

    [Fact]
    public async Task OnAssemblyCompletedAfterOnSessionClosedIsNoOp()
    {
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker();
        await using var session = CreateSession(metrics);

        var state = tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(5));
        tracker.OnSessionClosed(session);

        // Completing after close should not throw.
        tracker.OnAssemblyCompleted(session, state);
        Assert.Equal(0, tracker.ActiveAssemblyCount);
    }

    [Fact]
    public async Task ScanLoopClosesSessionOnTimeout()
    {
        // Integration test: verify the scan loop detects timeout and closes the session.
        // Using real TimeProvider.System with a short timeout (200ms) and scan interval (50ms).
        // Sleep 600ms to ensure at least one scan tick fires after the timeout.
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker(
            TimeProvider.System,
            TimeSpan.FromMilliseconds(50));
        await using var session = CreateSession(metrics);

        await tracker.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            tracker.OnAssemblyStarted(session, TimeSpan.FromMilliseconds(200));
            Assert.True(session.IsConnected);

            // Wait for timeout + at least one scan tick.
            await Task.Delay(600, TestContext.Current.CancellationToken);

            Assert.False(session.IsConnected);
            // Scan loop Close triggers OnSessionClosed via TcpClientSession.Close,
            // but the tracker's scan loop doesn't call OnSessionClosed directly —
            // it calls session.Close which calls _frameAssemblyTracker.OnSessionClosed.
            // So the entry should be removed.
            Assert.Equal(0, tracker.ActiveAssemblyCount);
        }
        finally
        {
            await tracker.StopAsync();
        }
    }

    [Fact]
    public async Task ScanLoopDoesNotCloseSessionBeforeTimeout()
    {
        using var metrics = new GatewayMetrics();
        await using var tracker = new FrameAssemblyTimeoutTracker(
            TimeProvider.System,
            TimeSpan.FromMilliseconds(50));
        await using var session = CreateSession(metrics);

        await tracker.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            tracker.OnAssemblyStarted(session, TimeSpan.FromSeconds(5));

            // Wait for a few scan ticks — well within the 5s timeout.
            await Task.Delay(150, TestContext.Current.CancellationToken);

            Assert.True(session.IsConnected);
            Assert.Equal(1, tracker.ActiveAssemblyCount);
        }
        finally
        {
            await tracker.StopAsync();
        }
    }

    [Fact]
    public async Task StartAsyncIsIdempotent()
    {
        await using var tracker = new FrameAssemblyTimeoutTracker();
        var ct = TestContext.Current.CancellationToken;

        await tracker.StartAsync(ct);
        await tracker.StartAsync(ct); // second call is no-op

        await tracker.StopAsync();
    }

    [Fact]
    public async Task StopAsyncIsIdempotent()
    {
        await using var tracker = new FrameAssemblyTimeoutTracker();
        await tracker.StartAsync(TestContext.Current.CancellationToken);

        await tracker.StopAsync();
        await tracker.StopAsync(); // second call is no-op
    }

    private static TcpClientSession CreateSession(GatewayMetrics metrics)
    {
        return new TcpClientSession(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId: 1,
            outboundQueueCapacity: 4,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);
    }
}
