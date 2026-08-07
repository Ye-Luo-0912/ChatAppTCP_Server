using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class LoadMonitoringCoordinatorTests
{
    [Fact]
    public async Task FailingChildReturnsBeforeSiblingCompletes()
    {
        var failing = new TaskCompletionSource<LoadExitObservation>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var sibling = new TaskCompletionSource<LoadExitObservation>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var monitoring = LoadMonitoringCoordinator.WaitForCompletionAsync(
            [failing.Task, sibling.Task],
            static () => null,
            TimeSpan.FromMilliseconds(5),
            TestContext.Current.CancellationToken);

        failing.SetResult(new LoadExitObservation(
            "tcp-load-1",
            ExitCode: 1,
            Elapsed: TimeSpan.FromMinutes(1),
            ExpectedMinimumRuntime: TimeSpan.FromHours(8)));

        var result = await monitoring.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);

        Assert.Contains("tcp-load-1 exited with code 1", result.FailFastReason);
        Assert.True(result.ServicesAlive);
        Assert.Single(result.Loads);
        Assert.False(sibling.Task.IsCompleted);
    }

    [Fact]
    public async Task ServiceExitReturnsBeforeAnyLoadCompletes()
    {
        var load = new TaskCompletionSource<LoadExitObservation>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var result = await LoadMonitoringCoordinator.WaitForCompletionAsync(
            [load.Task],
            static () => "gateway-1",
            TimeSpan.FromMilliseconds(5),
            TestContext.Current.CancellationToken);

        Assert.Contains("gateway-1 exited", result.FailFastReason);
        Assert.False(result.ServicesAlive);
        Assert.Empty(result.Loads);
        Assert.False(load.Task.IsCompleted);
    }

    [Fact]
    public async Task HealthyChildrenMayFinishAtDifferentTimes()
    {
        var first = Task.FromResult(new LoadExitObservation(
            "tcp-load-1",
            ExitCode: 0,
            Elapsed: TimeSpan.FromHours(8),
            ExpectedMinimumRuntime: TimeSpan.FromHours(8)));
        var second = Task.FromResult(new LoadExitObservation(
            "tcp-load-2",
            ExitCode: 0,
            Elapsed: TimeSpan.FromHours(8) + TimeSpan.FromSeconds(30),
            ExpectedMinimumRuntime: TimeSpan.FromHours(8)));

        var result = await LoadMonitoringCoordinator.WaitForCompletionAsync(
            [first, second],
            static () => null,
            TimeSpan.FromMilliseconds(5),
            TestContext.Current.CancellationToken);

        Assert.Null(result.FailFastReason);
        Assert.True(result.ServicesAlive);
        Assert.Equal(2, result.Loads.Count);
    }

    [Fact]
    public void ZeroExitCodeStillFailsWhenChildEndsBeforeMeasurementWindow()
    {
        var reason = LoadMonitoringCoordinator.GetFailureReason(
            new LoadExitObservation(
                "tcp-load-1",
                ExitCode: 0,
                Elapsed: TimeSpan.FromMinutes(5),
                ExpectedMinimumRuntime: TimeSpan.FromHours(8)));

        Assert.Contains("exited before its", reason);
    }

    [Fact]
    public void FormalChatTimeoutIncludesDeliveryDrainAndShutdownGrace()
    {
        var timeout = BenchmarkTiming.CalculateLoadTimeout(
            ramp: TimeSpan.FromSeconds(20),
            stabilization: TimeSpan.FromSeconds(300),
            measurement: TimeSpan.FromHours(8),
            isTcpChat: true,
            tcpDeliveryDrain: TimeSpan.FromSeconds(60),
            pipelineEnabled: false,
            pipelineOperationTimeout: TimeSpan.FromSeconds(15));

        Assert.Equal(
            TimeSpan.FromSeconds(20 + 300 + 28_800 + 60 + 30),
            timeout);
    }

    [Fact]
    public void NonChatAndDisabledPipelineDoNotConsumeTheirTailBudgets()
    {
        var timeout = BenchmarkTiming.CalculateLoadTimeout(
            ramp: TimeSpan.FromSeconds(10),
            stabilization: TimeSpan.FromSeconds(20),
            measurement: TimeSpan.FromSeconds(30),
            isTcpChat: false,
            tcpDeliveryDrain: TimeSpan.FromMinutes(5),
            pipelineEnabled: false,
            pipelineOperationTimeout: TimeSpan.FromMinutes(5));

        Assert.Equal(TimeSpan.FromSeconds(10 + 20 + 30 + 30), timeout);
    }
}
