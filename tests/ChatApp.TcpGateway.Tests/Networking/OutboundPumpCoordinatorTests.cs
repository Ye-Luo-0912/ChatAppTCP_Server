using ChatApp.TcpGateway.Gateway.Networking.Executor;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// <see cref="OutboundPumpCoordinator"/> 单元测试。
/// 验证构造参数校验、TrySchedule 计数、StopAsync 清理、WorkerCount 暴露。
/// <para>
/// PumpOutboundAsync 的端到端行为由 TcpGatewayServiceTests 覆盖（需要真实 session + outbound queue）；
/// 本测试聚焦 coordinator 自身状态机。
/// </para>
/// </summary>
public sealed class OutboundPumpCoordinatorTests
{
    [Fact]
    public void ConstructorRejectsNonPositiveBurstLimit()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new OutboundPumpCoordinator(
                burstLimit: 0,
                readyQueueCapacity: 16,
                logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance));
    }

    [Fact]
    public void ConstructorRejectsNonPositiveReadyQueueCapacity()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new OutboundPumpCoordinator(
                burstLimit: 4,
                readyQueueCapacity: 0,
                logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance));
    }

    [Fact]
    public void TryScheduleThrowsOnNullSession()
    {
        var coord = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 16,
            logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        Assert.Throws<ArgumentNullException>(() => coord.TrySchedule(null!));
    }

    [Fact]
    public async Task StartAsyncIsIdempotentAndSetsWorkerCount()
    {
        var coord = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 16,
            logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        Assert.Equal(0, coord.WorkerCount);

        using var cts = new CancellationTokenSource();
        await coord.StartAsync(workerCount: 2, cts.Token);
        Assert.Equal(2, coord.WorkerCount);

        // 重复调用幂等：WorkerCount 不变。
        await coord.StartAsync(workerCount: 4, cts.Token);
        Assert.Equal(2, coord.WorkerCount);

        cts.Cancel();
        await coord.StopAsync(CancellationToken.None);
        coord.Dispose();
    }

    [Fact]
    public async Task StartAsyncRejectsNonPositiveWorkerCount()
    {
        var coord = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 16,
            logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        using var cts = new CancellationTokenSource();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => coord.StartAsync(workerCount: 0, cts.Token));

        coord.Dispose();
    }

    [Fact]
    public async Task StopAsyncResetsReadyQueueCount()
    {
        var coord = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 16,
            logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        using var cts = new CancellationTokenSource();
        await coord.StartAsync(workerCount: 1, cts.Token);

        // StopAsync 会排空 ready queue。
        await coord.StopAsync(CancellationToken.None);

        Assert.Equal(0, coord.ReadyQueueCount);

        coord.Dispose();
    }

    [Fact]
    public async Task TotalScheduledIsCumulativeAcrossSchedules()
    {
        var coord = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 64,
            logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        Assert.Equal(0, coord.TotalScheduled);

        using var cts = new CancellationTokenSource();
        await coord.StartAsync(workerCount: 1, cts.Token);

        // 注意：worker 会立即消费 ready queue 并调用 PumpOutboundAsync。
        // 由于 session 没有 pending 帧，PumpOutboundAsync 会立即返回。
        // TotalScheduled 统计 TrySchedule 成功次数，ReadyQueueCount 是瞬时值。
        var session1 = TestSessionFactory.Create();
        var session2 = TestSessionFactory.Create();

        // 入队两个不同 session（TrySchedule 内部不依赖 _sendScheduled 标志，
        // 测试直接调用绕过 session 的 CAS，因此可以重复入队同一 session）。
        coord.TrySchedule(session1);
        coord.TrySchedule(session2);

        // 给 worker 一点时间消费。
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Equal(2, coord.TotalScheduled);
        Assert.Equal(0, coord.ReadyQueueCount);

        cts.Cancel();
        await coord.StopAsync(CancellationToken.None);
        coord.Dispose();
        await session1.DisposeAsync();
        await session2.DisposeAsync();
    }
}
