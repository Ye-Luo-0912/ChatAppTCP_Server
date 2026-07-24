using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class TypingFanoutCoordinatorTests
{
    [Fact]
    public void TryAccept_RateLimitsDuplicateTypingTrue()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var coordinator = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(500),
            ttl: TimeSpan.FromSeconds(4));

        Assert.True(coordinator.TryAccept(1, "dm:1:2", isTyping: true, out var expire1));
        Assert.NotNull(expire1);

        Assert.False(coordinator.TryAccept(1, "dm:1:2", isTyping: true, out var expire2));
        Assert.NotNull(expire2);
        Assert.Equal(expire1, expire2);

        clock.Advance(TimeSpan.FromMilliseconds(600));
        Assert.True(coordinator.TryAccept(1, "dm:1:2", isTyping: true, out var expire3));
        Assert.True(expire3 > expire1);
    }

    [Fact]
    public void TryTakeExpired_OnlyWhenExpireMatches()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var coordinator = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(4));

        Assert.True(coordinator.TryAccept(7, "dm:7:8", isTyping: true, out var expireAt));
        Assert.False(coordinator.TryTakeExpired(7, "dm:7:8", expireAt!.Value.AddSeconds(1)));
        Assert.True(coordinator.TryTakeExpired(7, "dm:7:8", expireAt.Value));
        Assert.False(coordinator.TryTakeExpired(7, "dm:7:8", expireAt.Value));
    }

    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utc = start;

        public override DateTimeOffset GetUtcNow() => _utc;

        public void Advance(TimeSpan delta) => _utc += delta;
    }
}
