using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class HeartbeatRefreshCadenceTests
{
    [Fact]
    public void NinetySecondRefreshRunsEveryThirdThirtySecondCycle()
    {
        var every = HeartbeatCoordinator.GetRefreshEveryCycles(
            TimeSpan.FromSeconds(90),
            TimeSpan.FromSeconds(30));

        Assert.Equal(3, every);
        Assert.True(HeartbeatCoordinator.IsRefreshCycleDue(1, 30, every));
        Assert.True(HeartbeatCoordinator.IsRefreshCycleDue(30, 30, every));
        Assert.False(HeartbeatCoordinator.IsRefreshCycleDue(31, 30, every));
        Assert.False(HeartbeatCoordinator.IsRefreshCycleDue(61, 30, every));
        Assert.True(HeartbeatCoordinator.IsRefreshCycleDue(91, 30, every));
    }

    [Fact]
    public void RefreshIntervalRoundsUpToWholeScanCycles()
    {
        Assert.Equal(
            4,
            HeartbeatCoordinator.GetRefreshEveryCycles(
                TimeSpan.FromSeconds(100),
                TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void OptionsRejectCadenceThatCannotSurviveTwoMissedPresenceRefreshes()
    {
        var safe = new TcpGatewayOptions
        {
            GlobalPresenceRefreshInterval = TimeSpan.FromSeconds(90)
        };
        var unsafeOptions = new TcpGatewayOptions
        {
            GlobalPresenceRefreshInterval = TimeSpan.FromSeconds(100)
        };

        Assert.True(safe.IsValid());
        Assert.False(unsafeOptions.IsValid());
    }
}
