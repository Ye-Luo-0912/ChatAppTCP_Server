using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class TypingFanoutCoordinatorTests
{
    [Fact]
    public void TryAccept_EmitsTrue_ThenRateLimitsDuplicate_ThenEmitsAfterInterval()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var coordinator = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(500),
            ttl: TimeSpan.FromSeconds(4),
            tickInterval: TimeSpan.FromMilliseconds(500));

        // t=0: 首次 typing=true 发射。
        Assert.True(coordinator.TryAccept(1, 2, "dm:1:2", isTyping: true));
        AssertSingleEmission(coordinator, expectedIsTyping: true);

        // t=0.3: 限频窗口内重复 typing=true，仅刷新过期/版本，不发射。
        clock.Advance(TimeSpan.FromMilliseconds(300));
        Assert.False(coordinator.TryAccept(1, 2, "dm:1:2", isTyping: true));
        Assert.Empty(coordinator.DrainPending());

        // t=0.6: 超过限频窗口，再次发射。
        clock.Advance(TimeSpan.FromMilliseconds(300));
        Assert.True(coordinator.TryAccept(1, 2, "dm:1:2", isTyping: true));
        AssertSingleEmission(coordinator, expectedIsTyping: true);
    }

    [Fact]
    public void TryAccept_ExplicitFalse_StopsAndEmitsFalse()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var coordinator = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(4),
            tickInterval: TimeSpan.FromMilliseconds(500));

        Assert.True(coordinator.TryAccept(7, 8, "dm:7:8", isTyping: true));
        AssertSingleEmission(coordinator, expectedIsTyping: true);

        // 显式停止：立即发射 false，无需等待过期。
        Assert.True(coordinator.TryAccept(7, 8, "dm:7:8", isTyping: false));
        AssertSingleEmission(coordinator, expectedIsTyping: false);

        // 无活跃 typing 时再发 false 不产生发射。
        Assert.False(coordinator.TryAccept(7, 8, "dm:7:8", isTyping: false));
        Assert.Empty(coordinator.DrainPending());
    }

    [Fact]
    public void PumpExpired_EmitsFalse_AfterTtl()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var coordinator = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(4),
            tickInterval: TimeSpan.FromMilliseconds(500));

        Assert.True(coordinator.TryAccept(7, 8, "dm:7:8", isTyping: true));
        AssertSingleEmission(coordinator, expectedIsTyping: true);

        // 到 TTL 但未跨 tick 边界（currentTick == expireTick）：仅扫描已完成 tick，不触碰到期桶。
        clock.Advance(TimeSpan.FromSeconds(4));
        coordinator.PumpExpired();
        Assert.Empty(coordinator.DrainPending());

        // 跨过下一个 tick：扫描到期桶，发射 false。
        clock.Advance(TimeSpan.FromMilliseconds(500));
        coordinator.PumpExpired();
        AssertSingleEmission(coordinator, expectedIsTyping: false);
    }

    [Fact]
    public void PumpExpired_RefreshExtendsExpiry_StrandingRegression()
    {
        // 核心回归：限频刷新只更新版本号与 ExpireAt，旧到期条目因版本不匹配失效，
        // 新到期条目须在刷新后的 ExpireAt 才发射 false。
        // 旧实现（每状态 Task.Delay）刷新后无任务负责 typing=false；本测试锁定时间轮修正。
        // 同时锁定 bucket leftover 搁置修正：刷新条目与原条目落同一 bucket 时，
        // 扫描当前 tick 会把未到期的新条目挂回已扫描桶而搁置一圈；改用 tick<currentTick 后不再发生。
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var coordinator = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(500),
            ttl: TimeSpan.FromSeconds(4),
            tickInterval: TimeSpan.FromMilliseconds(500));

        // t=0: typing=true，ExpireAt=4.0s，expireTick=8，bucket=8（bucketCount=10）。
        Assert.True(coordinator.TryAccept(1, 2, "dm:1:2", isTyping: true));
        AssertSingleEmission(coordinator, expectedIsTyping: true);

        // t=0.3: 限频刷新，ExpireAt=4.3s，expireTick=8（4.3/0.5=8.6→8），同 bucket=8，版本号 2。
        clock.Advance(TimeSpan.FromMilliseconds(300));
        Assert.False(coordinator.TryAccept(1, 2, "dm:1:2", isTyping: true));
        Assert.Empty(coordinator.DrainPending());

        // t=4.0（原 ExpireAt）：v1 版本不匹配被跳过；v2 尚未到期。
        clock.Advance(TimeSpan.FromSeconds(3.7));
        coordinator.PumpExpired();
        Assert.Empty(coordinator.DrainPending());

        // t=4.5（跨过刷新后 ExpireAt=4.3 的 tick 边界）：扫描 bucket=8，v2 到期发射 false。
        clock.Advance(TimeSpan.FromMilliseconds(500));
        coordinator.PumpExpired();
        AssertSingleEmission(coordinator, expectedIsTyping: false);
    }

    private static void AssertSingleEmission(TypingFanoutCoordinator coordinator, bool expectedIsTyping)
    {
        var batch = coordinator.DrainPending();
        var emission = Assert.Single(batch);
        Assert.Equal(expectedIsTyping, emission.IsTyping);
    }

    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utc = start;

        public override DateTimeOffset GetUtcNow() => _utc;

        public void Advance(TimeSpan delta) => _utc += delta;
    }
}
