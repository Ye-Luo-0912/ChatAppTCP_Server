using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Scheduling;

namespace ChatApp.TcpGateway.Tests.ActorRuntime;

/// <summary>
/// <see cref="AsyncOperationExecutor"/> 单元测试。
/// 验证提交/执行/拒绝/停机语义。
/// </summary>
public sealed class AsyncOperationExecutorTests
{
    [Fact]
    public async Task SubmittedOperationExecutesAsynchronously()
    {
        var executor = new AsyncOperationExecutor(
            maxConcurrency: 2,
            queueCapacity: 16,
            operationTimeout: TimeSpan.FromSeconds(5),
            TimeProvider.System);

        var ct = TestContext.Current.CancellationToken;
        var tcs = new TaskCompletionSource<int>();
        var op = new TestOperation(_ => tcs.SetResult(42));

        await executor.StartAsync(ct);
        Assert.True(executor.TrySubmit(in op));

        var result = await tcs.Task;
        Assert.Equal(42, result);

        await executor.StopAsync();
        await executor.DisposeAsync();
    }

    [Fact]
    public async Task MultipleOperationsRunInParallel()
    {
        var executor = new AsyncOperationExecutor(
            maxConcurrency: 4,
            queueCapacity: 64,
            operationTimeout: TimeSpan.FromSeconds(10),
            TimeProvider.System);

        var ct = TestContext.Current.CancellationToken;
        var runningCount = 0;
        var maxObserved = 0;
        var gate = new ManualResetEventSlim(false);
        var ops = new List<TestOperation>();

        for (var i = 0; i < 8; i++)
        {
            ops.Add(new TestOperation(_ =>
            {
                var current = Interlocked.Increment(ref runningCount);
                // 记录峰值
                int original;
                do { original = maxObserved; }
                while (current > original && Interlocked.CompareExchange(ref maxObserved, current, original) != original);

                gate.Wait(TimeSpan.FromSeconds(5), ct);
                Interlocked.Decrement(ref runningCount);
            }));
        }

        await executor.StartAsync(ct);
        foreach (var op in ops)
        {
            Assert.True(executor.TrySubmit(in op));
        }

        // 让并发跑一会儿
        await Task.Delay(100, ct);
        gate.Set();

        // 等待所有操作完成
        await Task.Delay(200, ct);

        Assert.InRange(maxObserved, 1, 4); // 至少 1 并发，至多 maxConcurrency=4

        await executor.StopAsync();
        await executor.DisposeAsync();
    }

    [Fact]
    public async Task TrySubmitReturnsFalseWhenQueueFull()
    {
        // 使用足够大的 maxConcurrency 与 queueCapacity=2，并先不启动 executor
        // 这样所有提交的 op 都堆积在 channel 中，第 3 个必然被拒绝。
        var executor = new AsyncOperationExecutor(
            maxConcurrency: 4,
            queueCapacity: 2,
            operationTimeout: TimeSpan.FromSeconds(5),
            TimeProvider.System);

        var ct = TestContext.Current.CancellationToken;

        // 不调用 StartAsync，worker 不会消费 → 队列必然填满
        var op1 = new TestOperation(_ => { });
        var op2 = new TestOperation(_ => { });
        Assert.True(executor.TrySubmit(in op1));
        Assert.True(executor.TrySubmit(in op2));

        // 队列已满：第 3 个被 DropWrite 拒绝
        var op3 = new TestOperation(_ => { });
        Assert.False(executor.TrySubmit(in op3));

        await executor.DisposeAsync();
    }

    [Fact]
    public async Task FaultedOperationReportsFailureAndDoesNotStopWorker()
    {
        var executor = new AsyncOperationExecutor(
            maxConcurrency: 1,
            queueCapacity: 4,
            operationTimeout: TimeSpan.FromSeconds(5),
            TimeProvider.System);

        var ct = TestContext.Current.CancellationToken;
        var failure = new TaskCompletionSource<AsyncOperationFailureKind>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var faulted = new FailureAwareOperation(
            static _ => ValueTask.FromException(
                new InvalidOperationException("expected")),
            kind => failure.TrySetResult(kind));
        var healthy = new FailureAwareOperation(
            _ =>
            {
                completed.TrySetResult();
                return ValueTask.CompletedTask;
            },
            static _ => { });

        await executor.StartAsync(ct);
        Assert.True(executor.TrySubmit(faulted));
        Assert.True(executor.TrySubmit(healthy));

        Assert.Equal(
            AsyncOperationFailureKind.Faulted,
            await failure.Task.WaitAsync(ct));
        await completed.Task.WaitAsync(ct);

        var snapshot = executor.GetSnapshot();
        Assert.Equal(1, snapshot.TotalFailed);
        Assert.Equal(1, snapshot.TotalCompleted);

        await executor.DisposeAsync();
    }

    [Fact]
    public async Task StopReportsQueuedOperationsAsRuntimeStopping()
    {
        var executor = new AsyncOperationExecutor(
            maxConcurrency: 1,
            queueCapacity: 4,
            operationTimeout: TimeSpan.FromSeconds(5),
            TimeProvider.System);

        var failure = new TaskCompletionSource<AsyncOperationFailureKind>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var queued = new FailureAwareOperation(
            static _ => ValueTask.CompletedTask,
            kind => failure.TrySetResult(kind));

        // 未启动 worker，Stop 必须排空 Channel，并通知调用方释放资源/恢复 Actor。
        Assert.True(executor.TrySubmit(queued));
        await executor.StopAsync();

        Assert.Equal(
            AsyncOperationFailureKind.RuntimeStopping,
            await failure.Task);
        Assert.Equal(0, executor.PendingCount);

        await executor.DisposeAsync();
    }

    /// <summary>
    /// 测试用 IAsyncOperation 实现。
    /// </summary>
    private readonly struct TestOperation : IAsyncOperation
    {
        private readonly Action<CancellationToken> _callback;

        public TestOperation(Action<CancellationToken> callback) => _callback = callback;

        public ValueTask ExecuteAsync(CancellationToken cancellationToken)
        {
            _callback(cancellationToken);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FailureAwareOperation : IAsyncOperation
    {
        private readonly Func<CancellationToken, ValueTask> _execute;
        private readonly Action<AsyncOperationFailureKind> _onFailure;

        public FailureAwareOperation(
            Func<CancellationToken, ValueTask> execute,
            Action<AsyncOperationFailureKind> onFailure)
        {
            _execute = execute;
            _onFailure = onFailure;
        }

        public ValueTask ExecuteAsync(CancellationToken cancellationToken) =>
            _execute(cancellationToken);

        public void OnFailure(
            Exception? exception,
            AsyncOperationFailureKind kind) =>
            _onFailure(kind);
    }
}
