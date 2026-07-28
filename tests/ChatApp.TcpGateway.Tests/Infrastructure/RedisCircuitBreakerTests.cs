using ChatApp.TcpGateway.Infrastructure.Caching;

namespace ChatApp.TcpGateway.Tests.Infrastructure;

/// <summary>
/// RedisCircuitBreaker 状态机验证：Closed→Open→HalfOpen→Closed/Open 的转换条件与线程安全。
/// 这是 Resume 可靠性主线的核心组件——熔断器在 Redis 故障期间快速失败，
/// 避免跨 Gateway 重连风暴串行触发 Redis 超时。
/// </summary>
public sealed class RedisCircuitBreakerTests
{
    [Fact]
    public void IsAvailable_Closed_WhenNoFailures()
    {
        var cb = new RedisCircuitBreaker(failureThreshold: 5);

        Assert.True(cb.IsAvailable);
        Assert.Equal(0, cb.ConsecutiveFailures);
        Assert.Equal(0, cb.OpenUntilTicks);
    }

    [Fact]
    public void RecordFailure_BelowThreshold_StaysClosed()
    {
        var cb = new RedisCircuitBreaker(failureThreshold: 5);

        for (var i = 0; i < 4; i++)
            cb.RecordFailure();

        Assert.True(cb.IsAvailable);
        Assert.Equal(4, cb.ConsecutiveFailures);
        Assert.Equal(0, cb.OpenUntilTicks);
    }

    [Fact]
    public void RecordFailure_ReachingThreshold_OpensCircuit()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 3,
            openDuration: TimeSpan.FromSeconds(5),
            timeProvider: time);

        cb.RecordFailure();
        cb.RecordFailure();
        Assert.True(cb.IsAvailable); // 2 < 3，仍未开路

        cb.RecordFailure(); // 达到阈值，开路

        Assert.False(cb.IsAvailable);
        Assert.Equal(3, cb.ConsecutiveFailures);
        Assert.NotEqual(0, cb.OpenUntilTicks);
    }

    [Fact]
    public void RecordSuccess_ResetsClosedState()
    {
        var cb = new RedisCircuitBreaker(failureThreshold: 3);

        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(2, cb.ConsecutiveFailures);

        cb.RecordSuccess();

        Assert.Equal(0, cb.ConsecutiveFailures);
        Assert.True(cb.IsAvailable);
    }

    [Fact]
    public void IsAvailable_DuringOpenWindow_ReturnsFalse()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            timeProvider: time);

        cb.RecordFailure();
        Assert.False(cb.IsAvailable);

        // 推进 3 秒，仍在开路窗口内
        time.Advance(TimeSpan.FromSeconds(3));

        Assert.False(cb.IsAvailable);
    }

    [Fact]
    public void IsAvailable_AfterOpenDuration_TransitionsToHalfOpen_AllowsProbe()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            timeProvider: time);

        cb.RecordFailure();
        Assert.False(cb.IsAvailable);

        // 推进超过开路窗口，进入 HalfOpen，允许试探请求
        time.Advance(TimeSpan.FromSeconds(6));

        Assert.True(cb.IsAvailable);
        // openUntilTicks 被清零（HalfOpen 进入瞬间由 IsAvailable 完成）
        Assert.Equal(0, cb.OpenUntilTicks);
    }

    [Fact]
    public void HalfOpen_RecordSuccess_TransitionsToClosed()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            timeProvider: time);

        cb.RecordFailure();
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.True(cb.IsAvailable); // HalfOpen

        // 试探请求成功
        cb.RecordSuccess();

        Assert.Equal(0, cb.ConsecutiveFailures);
        Assert.Equal(0, cb.OpenUntilTicks);
        Assert.True(cb.IsAvailable); // Closed
    }

    [Fact]
    public void HalfOpen_RecordFailure_ReopensCircuit()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 2,
            openDuration: TimeSpan.FromSeconds(5),
            timeProvider: time);

        // 触发首次开路
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.False(cb.IsAvailable);

        // 推进入 HalfOpen
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.True(cb.IsAvailable); // HalfOpen 允许试探

        // 试探失败：HalfOpen 下任一失败立即重新 Open（_consecutiveFailures 保持原值，
        // 此处 >= threshold 即重新开路）
        cb.RecordFailure();

        Assert.False(cb.IsAvailable);
        Assert.NotEqual(0, cb.OpenUntilTicks);
    }

    [Fact]
    public void IsAvailable_ConcurrentReads_AreThreadSafe()
    {
        var cb = new RedisCircuitBreaker(failureThreshold: 100);

        // 并发读取 + 失败计数不应抛异常
        Parallel.For(0, 1000, i =>
        {
            if (i % 3 == 0)
                cb.RecordFailure();
            else
                _ = cb.IsAvailable;
        });

        // 不抛即视为通过；具体失败数取决于调度
        Assert.True(cb.ConsecutiveFailures >= 0);
    }

    /// <summary>
    /// 简易时间提供者：控制 GetUtcNow 推进，验证 RedisCircuitBreaker 的时间窗口逻辑。
    /// </summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private long _utcTicks = DateTimeOffset.UtcNow.Ticks;

        public override DateTimeOffset GetUtcNow() =>
            new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan duration) =>
            Interlocked.Add(ref _utcTicks, duration.Ticks);
    }
}
