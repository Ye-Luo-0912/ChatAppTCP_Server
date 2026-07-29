using ChatApp.TcpGateway.Infrastructure.Caching;

namespace ChatApp.TcpGateway.Tests.Infrastructure;

/// <summary>
/// RedisCircuitBreaker 状态机验证：Closed→Open→HalfOpen→Closed/Open 的转换条件与线程安全。
/// R-1 修复后：HalfOpen 状态下仅允许一个调用者获取 Probe Lease，其余快速失败。
/// </summary>
public sealed class RedisCircuitBreakerTests
{
    [Fact]
    public void IsAvailable_Closed_WhenNoFailures()
    {
        var cb = new RedisCircuitBreaker(failureThreshold: 5);

        Assert.True(cb.IsAvailable);
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
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
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
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
        Assert.Equal(CircuitBreakerState.Open, cb.State);
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
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
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
        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // 推进 3 秒，仍在开路窗口内
        time.Advance(TimeSpan.FromSeconds(3));

        Assert.False(cb.IsAvailable);
        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    [Fact]
    public void IsAvailable_AfterOpenDuration_TransitionsToHalfOpen_AllowsProbe()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            probeTimeout: TimeSpan.FromSeconds(10),
            timeProvider: time);

        cb.RecordFailure();
        Assert.False(cb.IsAvailable);
        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // 推进超过开路窗口，进入 HalfOpen，允许试探请求
        time.Advance(TimeSpan.FromSeconds(6));

        Assert.True(cb.IsAvailable); // 获得 Probe Lease
        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);
        // OpenUntilTicks 现在是 Probe Lease 超时（now + probeTimeout），非 0。
        Assert.NotEqual(0, cb.OpenUntilTicks);
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
        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);

        // 试探请求成功
        cb.RecordSuccess();

        Assert.Equal(0, cb.ConsecutiveFailures);
        Assert.Equal(0, cb.OpenUntilTicks);
        Assert.True(cb.IsAvailable); // Closed
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
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
        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // 推进入 HalfOpen
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.True(cb.IsAvailable); // HalfOpen 允许试探
        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);

        // 试探失败：HalfOpen 下任一失败立即重新 Open
        cb.RecordFailure();

        Assert.False(cb.IsAvailable);
        Assert.Equal(CircuitBreakerState.Open, cb.State);
        Assert.NotEqual(0, cb.OpenUntilTicks);
    }

    /// <summary>
    /// R-1 核心验证：Open 窗口超时后，并发调用者中只有一个获得 Probe Lease。
    /// 其余调用者在探针完成前快速失败（返回 false）。
    /// </summary>
    [Fact]
    public void IsAvailable_AfterOpenDuration_OnlyOneConcurrentCallerGetsProbeLease()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            probeTimeout: TimeSpan.FromSeconds(30),
            timeProvider: time);

        // 触发开路
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // 推进超过开路窗口
        time.Advance(TimeSpan.FromSeconds(6));

        // 并发调用 IsAvailable，只有一个应获得 Probe Lease
        var granted = 0;
        var denied = 0;

        Parallel.For(0, 50, _ =>
        {
            if (cb.IsAvailable)
                Interlocked.Increment(ref granted);
            else
                Interlocked.Increment(ref denied);
        });

        Assert.Equal(1, granted); // 仅一个调用者获得 Probe Lease
        Assert.Equal(49, denied); // 其余快速失败
        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);
    }

    /// <summary>
    /// R-1 验证：Probe Lease 超时后，新调用者可获取新 Lease。
    /// 防止探针调用者异常退出导致永久阻塞。
    /// </summary>
    [Fact]
    public void IsAvailable_ProbeLeaseTimeout_AllowsNewProbe()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            probeTimeout: TimeSpan.FromSeconds(10),
            timeProvider: time);

        cb.RecordFailure();
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.True(cb.IsAvailable); // 获得 Probe Lease
        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);

        // 第二次调用在 Lease 内：快速失败
        Assert.False(cb.IsAvailable);

        // 推进超过 Probe Lease 超时
        time.Advance(TimeSpan.FromSeconds(11));

        // 新调用者应获取新 Lease
        Assert.True(cb.IsAvailable);
        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);
    }

    /// <summary>
    /// R-1 验证：探针持有 Lease 期间，其他调用者快速失败（非 Closed）。
    /// </summary>
    [Fact]
    public void IsAvailable_DuringHalfOpenProbe_OtherCallersFailFast()
    {
        var time = new ManualTimeProvider();
        var cb = new RedisCircuitBreaker(
            failureThreshold: 1,
            openDuration: TimeSpan.FromSeconds(5),
            probeTimeout: TimeSpan.FromSeconds(30),
            timeProvider: time);

        cb.RecordFailure();
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.True(cb.IsAvailable); // 第一个获得 Lease

        // 其他调用者快速失败
        Assert.False(cb.IsAvailable);
        Assert.False(cb.IsAvailable);
        Assert.False(cb.IsAvailable);

        Assert.Equal(CircuitBreakerState.HalfOpenProbeInFlight, cb.State);
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
