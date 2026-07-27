using ChatApp.TcpGateway.Gateway.Networking.Executor;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// <see cref="DeadlineWheel"/> 单元测试。
/// 验证注册/取消/触发语义、活跃计数、单调时钟与回调异常隔离。
/// </summary>
public sealed class DeadlineWheelTests
{
    [Fact]
    public async Task RegisterThrowsOnNonPositiveDelay()
    {
        await using var wheel = new DeadlineWheel();
        Assert.Throws<ArgumentOutOfRangeException>(
            () => wheel.Register(TimeSpan.Zero, () => { }));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => wheel.Register(TimeSpan.FromMilliseconds(-1), () => { }));
    }

    [Fact]
    public async Task RegisterThrowsOnNullCallback()
    {
        await using var wheel = new DeadlineWheel();
        Assert.Throws<ArgumentNullException>(
            () => wheel.Register(TimeSpan.FromMilliseconds(1), null!));
    }

    [Fact]
    public async Task CancelIsNoOpForDefaultRegistration()
    {
        await using var wheel = new DeadlineWheel();
        // Id=0 表示无效注册，Cancel 必须静默忽略。
        wheel.Cancel(default);
        Assert.Equal(0, wheel.ActiveDeadlineCount);
    }

    [Fact]
    public async Task ActiveDeadlineCountReflectsRegisterAndCancel()
    {
        await using var wheel = new DeadlineWheel();
        var reg1 = wheel.Register(TimeSpan.FromHours(1), () => { });
        var reg2 = wheel.Register(TimeSpan.FromHours(1), () => { });

        Assert.Equal(2, wheel.ActiveDeadlineCount);

        wheel.Cancel(reg1);
        Assert.Equal(1, wheel.ActiveDeadlineCount);

        // 重复 Cancel 同一注册：幂等，不重复递减。
        wheel.Cancel(reg1);
        Assert.Equal(1, wheel.ActiveDeadlineCount);

        wheel.Cancel(reg2);
        Assert.Equal(0, wheel.ActiveDeadlineCount);
    }

    [Fact]
    public async Task PumpExpiredFiresExpiredDeadlinesAndDecrementsCount()
    {
        // 使用极短 tickInterval 使 PumpExpired 能立即扫到过期条目。
        // tickInterval 必须 > 0，1ms 是 PeriodicTimer 的下限附近但 PumpExpired 直接读 GetTimestamp。
        await using var wheel = new DeadlineWheel(
            tickInterval: TimeSpan.FromMilliseconds(1));
        var fired = 0;

        var reg = wheel.Register(
            TimeSpan.FromMilliseconds(2),
            () => Interlocked.Increment(ref fired));

        // 等待 deadline 物理过期。
        Thread.Sleep(20);
        wheel.PumpExpired();

        Assert.Equal(1, fired);
        Assert.Equal(0, wheel.ActiveDeadlineCount);

        // 已触发的注册再 Cancel：静默忽略，不影响计数。
        wheel.Cancel(reg);
        Assert.Equal(0, wheel.ActiveDeadlineCount);
    }

    [Fact]
    public async Task CancelledDeadlineDoesNotFire()
    {
        await using var wheel = new DeadlineWheel(
            tickInterval: TimeSpan.FromMilliseconds(1));
        var fired = 0;

        var reg = wheel.Register(
            TimeSpan.FromMilliseconds(2),
            () => Interlocked.Increment(ref fired));

        wheel.Cancel(reg);
        Assert.Equal(0, wheel.ActiveDeadlineCount);

        Thread.Sleep(20);
        wheel.PumpExpired();

        Assert.Equal(0, fired);
    }

    [Fact]
    public async Task CallbackExceptionDoesNotBreakWheelOrOtherCallbacks()
    {
        await using var wheel = new DeadlineWheel(
            tickInterval: TimeSpan.FromMilliseconds(1));
        var secondFired = 0;

        wheel.Register(
            TimeSpan.FromMilliseconds(1),
            () => throw new InvalidOperationException("boom"));
        var reg2 = wheel.Register(
            TimeSpan.FromMilliseconds(1),
            () => Interlocked.Increment(ref secondFired));

        Thread.Sleep(20);
        wheel.PumpExpired();

        // 第一个回调抛异常不应阻断后续回调与计数维护。
        Assert.Equal(1, secondFired);
        Assert.Equal(0, wheel.ActiveDeadlineCount);

        // 已触发的 reg2 Cancel 不影响计数。
        wheel.Cancel(reg2);
        Assert.Equal(0, wheel.ActiveDeadlineCount);
    }

    [Fact]
    public async Task StartAsyncIsIdempotent()
    {
        await using var wheel = new DeadlineWheel();
        using var cts = new CancellationTokenSource();

        var task1 = wheel.StartAsync(cts.Token);
        var task2 = wheel.StartAsync(cts.Token);

        Assert.Same(task1, task2);

        cts.Cancel();
    }

    [Fact]
    public async Task DisposeAsyncWaitsForRunTaskCompletion()
    {
        var wheel = new DeadlineWheel(
            tickInterval: TimeSpan.FromMilliseconds(2));
        using var cts = new CancellationTokenSource();

        _ = wheel.StartAsync(cts.Token);
        var fired = 0;
        wheel.Register(
            TimeSpan.FromMilliseconds(5),
            () => Interlocked.Increment(ref fired));

        // Dispose 取消 RunTask 并等待退出，同时清理未触发 deadline 的桶与计数器。
        cts.Cancel();
        await wheel.DisposeAsync();

        // Dispose 后无活跃 deadline（桶与计数器已清理）。
        Assert.Equal(0, wheel.ActiveDeadlineCount);
    }

    [Fact]
    public async Task PumpExpiredNoOpsWhenNoTimeHasPassed()
    {
        await using var wheel = new DeadlineWheel(
            tickInterval: TimeSpan.FromMilliseconds(1));
        var fired = 0;

        wheel.Register(
            TimeSpan.FromHours(1),
            () => Interlocked.Increment(ref fired));

        // 立即 PumpExpired：无过期（currentTick <= _lastSweptTick + 1）。
        wheel.PumpExpired();

        Assert.Equal(0, fired);
        Assert.Equal(1, wheel.ActiveDeadlineCount);
    }
}
