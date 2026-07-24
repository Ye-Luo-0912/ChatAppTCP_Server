using System.Net.Sockets;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class UserSessionRegistryTests
{
    [Fact]
    public async Task TakeOverSameDevice_ReturnsOtherSessionsWithSameDeviceHash()
    {
        using var metrics = new GatewayMetrics();
        await using var first = CreateSession(1, metrics);
        await using var second = CreateSession(2, metrics);
        await using var otherDevice = CreateSession(3, metrics);
        var registry = new UserSessionRegistry();

        first.Authenticate(42, "session-a", deviceIdHash: 7);
        second.Authenticate(42, "session-b", deviceIdHash: 7);
        otherDevice.Authenticate(42, "session-c", deviceIdHash: 9);
        registry.Add(first);
        registry.Add(second);
        registry.Add(otherDevice);

        var victims = registry.TakeOverSameDevice(second);
        Assert.Single(victims);
        Assert.Same(first, victims[0]);
    }

    [Fact]
    public async Task ReusesSnapshotUntilMembershipChanges()
    {
        using var metrics = new GatewayMetrics();
        await using var first = CreateSession(1, metrics);
        await using var second = CreateSession(2, metrics);
        var registry = new UserSessionRegistry();

        first.Authenticate(42, "first", deviceIdHash: 1);
        registry.Add(first);

        var firstSnapshot = registry.GetSnapshot(42);
        Assert.Single(firstSnapshot);
        Assert.Same(firstSnapshot, registry.GetSnapshot(42));

        second.Authenticate(42, "second", deviceIdHash: 2);
        registry.Add(second);

        var secondSnapshot = registry.GetSnapshot(42);
        Assert.Equal(2, secondSnapshot.Length);
        Assert.NotSame(firstSnapshot, secondSnapshot);
        Assert.Same(secondSnapshot, registry.GetSnapshot(42));

        registry.Remove(first);
        Assert.Single(registry.GetSnapshot(42));

        registry.Remove(second);
        Assert.Empty(registry.GetSnapshot(42));
    }

    [Fact]
    public async Task Add_ReturnsTrueOnlyForFirstOnline_RemoveReturnsTrueOnlyForLastOffline()
    {
        using var metrics = new GatewayMetrics();
        await using var first = CreateSession(1, metrics);
        await using var second = CreateSession(2, metrics);
        var registry = new UserSessionRegistry();

        first.Authenticate(42, "first", deviceIdHash: 1);
        second.Authenticate(42, "second", deviceIdHash: 2);

        Assert.True(registry.Add(first));
        Assert.False(registry.Add(second));
        Assert.False(registry.Remove(first));
        Assert.True(registry.Remove(second));
        Assert.Empty(registry.GetSnapshot(42));
    }

    private static TcpClientSession CreateSession(
        uint connectionId,
        GatewayMetrics metrics) =>
        new(
            new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp),
            connectionId,
            outboundQueueCapacity: 8,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);
}
