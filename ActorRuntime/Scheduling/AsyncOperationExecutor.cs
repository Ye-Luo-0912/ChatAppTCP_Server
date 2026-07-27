using System.Runtime.CompilerServices;
using System.Threading.Channels;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Primitives;

namespace ChatApp.ActorRuntime.Scheduling;

/// <summary>
/// 全局有界异步操作执行器。PendingCount 同时覆盖排队与执行中操作，
/// 供 Runtime Drain 精确判断是否仍有可能返回 Completion。
/// </summary>
internal sealed class AsyncOperationExecutor :
    IAsyncOperationExecutorStats,
    IAsyncDisposable
{
    private readonly int _maxConcurrency;
    private readonly TimeSpan _operationTimeout;
    private readonly Channel<IAsyncOperation> _channel;
    private readonly CacheLinePaddedCounter _submittedCount = new();
    private readonly CacheLinePaddedCounter _completedCount = new();
    private readonly CacheLinePaddedCounter _rejectedCount = new();
    private readonly CacheLinePaddedCounter _failedCount = new();
    private readonly CacheLinePaddedCounter _timeoutCount = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Task> _workers = new();

    private CancellationTokenSource? _linkedCts;
    private volatile bool _stopping;
    private int _queuedCount;
    private int _inflightCount;
    private int _outstandingCount;

    public AsyncOperationExecutor(
        int maxConcurrency,
        int queueCapacity,
        TimeSpan operationTimeout,
        TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxConcurrency, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(queueCapacity, 0);
        _ = timeProvider;

        _maxConcurrency = maxConcurrency;
        _operationTimeout = operationTimeout;
        _channel = Channel.CreateBounded<IAsyncOperation>(
            new BoundedChannelOptions(queueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public int PendingCount => Volatile.Read(ref _outstandingCount);
    public int QueuedCount => Volatile.Read(ref _queuedCount);
    public int InflightCount => Volatile.Read(ref _inflightCount);
    public long TotalSubmitted => _submittedCount.Read();
    public long TotalCompleted => _completedCount.Read();
    public long TotalRejected => _rejectedCount.Read();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TrySubmit<TWork>(in TWork operation)
        where TWork : struct, IAsyncOperation
    {
        if (_stopping)
            return false;

        // 先计数再发布到 Channel，避免 worker 极快取走后出现短暂负数。
        Interlocked.Increment(ref _queuedCount);
        Interlocked.Increment(ref _outstandingCount);
        if (!_channel.Writer.TryWrite(operation))
        {
            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Decrement(ref _outstandingCount);
            _rejectedCount.Increment();
            return false;
        }

        _submittedCount.Increment();
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TrySubmit(IAsyncOperation operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (_stopping)
            return false;

        Interlocked.Increment(ref _queuedCount);
        Interlocked.Increment(ref _outstandingCount);
        if (!_channel.Writer.TryWrite(operation))
        {
            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Decrement(ref _outstandingCount);
            _rejectedCount.Increment();
            return false;
        }

        _submittedCount.Increment();
        return true;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_workers.Count > 0)
            return Task.CompletedTask;

        _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _cts.Token);
        for (var i = 0; i < _maxConcurrency; i++)
            _workers.Add(RunWorkerAsync(_linkedCts.Token));

        return Task.CompletedTask;
    }

    private async Task RunWorkerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var operation in _channel.Reader
                               .ReadAllAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                Interlocked.Decrement(ref _queuedCount);
                Interlocked.Increment(ref _inflightCount);
                try
                {
                    if (_operationTimeout > TimeSpan.Zero)
                    {
                        using var timeoutCts =
                            CancellationTokenSource.CreateLinkedTokenSource(
                                cancellationToken);
                        timeoutCts.CancelAfter(_operationTimeout);
                        await operation
                            .ExecuteAsync(timeoutCts.Token)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await operation
                            .ExecuteAsync(cancellationToken)
                            .ConfigureAwait(false);
                    }

                    _completedCount.Increment();
                }
                catch (OperationCanceledException exception)
                    when (_operationTimeout > TimeSpan.Zero &&
                          !cancellationToken.IsCancellationRequested)
                {
                    _timeoutCount.Increment();
                    NotifyFailure(
                        operation,
                        exception,
                        AsyncOperationFailureKind.TimedOut);
                }
                catch (OperationCanceledException exception)
                    when (cancellationToken.IsCancellationRequested)
                {
                    NotifyFailure(
                        operation,
                        exception,
                        AsyncOperationFailureKind.RuntimeStopping);
                }
                catch (Exception exception)
                {
                    _failedCount.Increment();
                    NotifyFailure(
                        operation,
                        exception,
                        AsyncOperationFailureKind.Faulted);
                }
                finally
                {
                    Interlocked.Decrement(ref _inflightCount);
                    Interlocked.Decrement(ref _outstandingCount);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    public async Task StopAsync()
    {
        if (_stopping)
            return;

        _stopping = true;
        _channel.Writer.TryComplete();
        await _cts.CancelAsync().ConfigureAwait(false);

        try
        {
            await Task.WhenAll(_workers).ConfigureAwait(false);
        }
        catch
        {
        }

        while (_channel.Reader.TryRead(out var abandoned))
        {
            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Decrement(ref _outstandingCount);
            NotifyFailure(
                abandoned,
                exception: null,
                AsyncOperationFailureKind.RuntimeStopping);
        }

        _linkedCts?.Dispose();
        _linkedCts = null;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    public AsyncOperationExecutorSnapshot GetSnapshot()
        => new()
        {
            PendingCount = PendingCount,
            QueuedCount = QueuedCount,
            InflightCount = InflightCount,
            TotalSubmitted = _submittedCount.Read(),
            TotalCompleted = _completedCount.Read(),
            TotalRejected = _rejectedCount.Read(),
            TotalFailed = _failedCount.Read(),
            TotalTimeout = _timeoutCount.Read()
        };

    private static void NotifyFailure(
        IAsyncOperation operation,
        Exception? exception,
        AsyncOperationFailureKind kind)
    {
        try
        {
            operation.OnFailure(exception, kind);
        }
        catch
        {
            // Failure callback cannot terminate a worker.
        }
    }
}

internal readonly struct AsyncOperationExecutorSnapshot
{
    public int PendingCount { get; init; }
    public int QueuedCount { get; init; }
    public int InflightCount { get; init; }
    public long TotalSubmitted { get; init; }
    public long TotalCompleted { get; init; }
    public long TotalRejected { get; init; }
    public long TotalFailed { get; init; }
    public long TotalTimeout { get; init; }
}

internal interface IAsyncOperationExecutorStats
{
    int PendingCount { get; }
    long TotalSubmitted { get; }
    long TotalCompleted { get; }
    long TotalRejected { get; }
}
