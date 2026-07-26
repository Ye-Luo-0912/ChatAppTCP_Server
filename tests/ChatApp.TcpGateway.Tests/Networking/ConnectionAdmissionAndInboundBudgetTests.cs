using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class ConnectionAdmissionTrackerTests
{
    [Fact]
    public void Release_RemovesZeroCountIpEntry()
    {
        var tracker = new ConnectionAdmissionTracker(
            maxUnauthenticatedConnections: 100,
            maxConnectionsPerIp: 10,
            maxAuthAttemptsPerIp: 20,
            authRateWindow: TimeSpan.FromMinutes(1));

        Assert.Equal(AdmissionResult.Admitted, tracker.TryAdmit("1.2.3.4"));
        Assert.Equal(1, tracker.TrackedIpCount);

        tracker.Release("1.2.3.4", wasAuthenticated: false);
        Assert.Equal(0, tracker.TrackedIpCount);
        Assert.Equal(0, tracker.CurrentConnectionsForIp("1.2.3.4"));
    }

    [Fact]
    public void SweepExpiredEntries_RemovesEmptyAuthFailureBuckets()
    {
        var window = TimeSpan.FromMilliseconds(50);
        var tracker = new ConnectionAdmissionTracker(
            maxUnauthenticatedConnections: 100,
            maxConnectionsPerIp: 10,
            maxAuthAttemptsPerIp: 20,
            authRateWindow: window);

        tracker.RecordAuthenticationFailure("9.9.9.9");
        Assert.Equal(1, tracker.TrackedAuthFailureBucketCount);

        Thread.Sleep(80);
        tracker.SweepExpiredEntries(DateTimeOffset.UtcNow);

        Assert.Equal(0, tracker.TrackedAuthFailureBucketCount);
    }
}

public sealed class GlobalInboundBudgetTests
{
    [Fact]
    public void TryReserve_RejectsWhenOverLimit_AndReleaseFreesCapacity()
    {
        var budget = new GlobalInboundBudget(100);
        Assert.True(budget.TryReserve(60));
        Assert.False(budget.TryReserve(50));
        budget.Release(60);
        Assert.True(budget.TryReserve(50));
    }

    [Fact]
    public void SessionInboundPipeLease_ReleaseAll_ReturnsRemaining()
    {
        var budget = new GlobalInboundBudget(1_000);
        var lease = new SessionInboundPipeLease(budget);
        Assert.True(lease.TryReserve(200));
        Assert.True(lease.TryReserve(100));
        lease.Release(50);
        Assert.Equal(250, budget.CurrentBytes);
        lease.ReleaseAll();
        Assert.Equal(0, budget.CurrentBytes);
    }
}
