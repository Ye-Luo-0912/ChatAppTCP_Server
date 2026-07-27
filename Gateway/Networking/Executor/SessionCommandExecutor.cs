using System.Collections.Concurrent;
using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Gateway.Networking.Executor;

/// <summary>
/// 全局共享的按连接串行命令执行器：替代每连接 Channel + Consumer Task。
/// <para>
/// 核心设计：
/// <list type="bullet">
/// <item>每连接保留轻量 <see cref="ConcurrentQueue{T}"/>，不再保留专属消费者 Task；</item>
/// <item>全局 ready channel 通知 worker 有连接待处理；</item>
/// <item>同连接通过原子 <c>_active</c> 标志 CAS 保证同时只有一个 worker 处理；</item>
/// <item>每次处理固定 burst，避免单连接独占 worker；</item>
/// <item>慢连接（命令处理耗时长）不会阻塞其他连接，因为不同连接并行。</item>
/// </list>
/// 状态和所有权属于连接（队列），执行资源属于进程（worker 池）。
/// </para>
/// <para>
/// 用于 OrderedWrite 与 Query 两条 lane，通过构造参数区分策略。
/// Query lane 可叠加 per-User 并发上限与命令超时。
/// </para>
/// </summary>
internal sealed class SessionCommandExecutor : IAsyncDisposable
{
    private readonly Func<SessionCommand, CancellationToken, ValueTask> _processor;
    private readonly int _workerCount;
    private readonly int _burstLimit;
    private readonly int _perConnectionCapacity;
    private readonly TimeSpan _commandTimeout;
    private readonly Action<Exception>? _onFatalError;
    private readonly ILogger _logger;

    private readonly Channel<uint> _ready;
    private readonly ConcurrentDictionary<uint, ConnectionQueue> _queues = new();
    private readonly SemaphoreSlim? _perUserGate;
    private readonly CancellationTokenSource _cts;
    private Task[] _workers = Array.Empty<Task>();
    private bool _disposed;

    /// <summary>
    /// 创建执行器。
    /// </summary>
    /// <param name="processor">命令处理回调。完成后调用，异常会被捕获并转为 fatal 回调。</param>
    /// <param name="workerCount">全局 worker 数。</param>
    /// <param name="burstLimit">单连接单次调度处理的命令上限，防止单连接独占 worker。</param>
    /// <param name="perConnectionCapacity">每连接队列容量。满了 TryEnqueue 返回 false。</param>
    /// <param name="globalCapacity">全局 ready channel 容量（待调度的连接数上限）。</param>
    /// <param name="commandTimeout">命令处理超时。Zero 表示不启用。</param>
    /// <param name="perUserConcurrency">每用户并发上限。0 表示不限制。</param>
    /// <param name="onFatalError">命令处理致命异常回调（如关闭会话）。</param>
    public SessionCommandExecutor(
        Func<SessionCommand, CancellationToken, ValueTask> processor,
        int workerCount,
        int burstLimit,
        int perConnectionCapacity,
        int globalCapacity,
        TimeSpan commandTimeout,
        int perUserConcurrency,
        Action<Exception>? onFatalError,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(workerCount, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(burstLimit, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(perConnectionCapacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(globalCapacity, 0);

        _processor = processor;
        _workerCount = workerCount;
        _burstLimit = burstLimit;
        _perConnectionCapacity = perConnectionCapacity;
        _commandTimeout = commandTimeout;
        _onFatalError = onFatalError;
        _logger = logger ?? NullLogger.Instance;

        _ready = Channel.CreateBounded<uint>(
            new BoundedChannelOptions(globalCapacity)
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        _perUserGate = perUserConcurrency > 0
            ? new SemaphoreSlim(perUserConcurrency, perUserConcurrency)
            : null;

        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// 注册一个连接。必须在第一次 <see cref="TryEnqueue"/> 前调用。
    /// 重复注册同一 connectionId 是幂等的（返回 false）。
    /// </summary>
    public bool TryRegisterConnection(uint connectionId, long userId)
    {
        return _queues.TryAdd(connectionId, new ConnectionQueue(connectionId, userId));
    }

    /// <summary>
    /// 注销连接并丢弃队列中残留命令（释放缓冲区与入站预算）。
    /// </summary>
    public void UnregisterConnection(uint connectionId)
    {
        if (_queues.TryRemove(connectionId, out var queue))
        {
            while (queue.Commands.TryDequeue(out var command))
            {
                queue.Count--;
                ReleaseCommandResources(in command);
            }
        }
    }

    /// <summary>
    /// 入队命令。队列满时返回 false（调用方负责释放资源）。
    /// 入队成功后会通知 ready channel 唤醒一个 worker。
    /// </summary>
    public bool TryEnqueue(uint connectionId, in SessionCommand command)
    {
        if (!_queues.TryGetValue(connectionId, out var queue))
            return false;

        // ConcurrentQueue 无界，通过 _perConnectionCapacity 在外层包装限流。
        // 容量检查与 Enqueue 不需要原子（生产者间串行由调用方保证，这里容忍轻微超限）。
        if (queue.Count >= _perConnectionCapacity)
            return false;

        queue.Commands.Enqueue(command);
        queue.Count++;

        // CAS _active: 0→1。成功表示此前无 worker 处理该连接，需通知 ready channel。
        // 失败表示已有 worker 在处理，它会在 burst 循环中 drain 到空，无需额外唤醒。
        SignalReadyIfNeeded(queue);

        return true;
    }

    private void SignalReadyIfNeeded(ConnectionQueue queue)
    {
        if (Interlocked.CompareExchange(ref queue.Active, 1, 0) != 0)
            return;

        // ready channel 满时 TryWrite 失败：回退 Active 并重试一次。
        // 若仍失败，依赖 worker burst 末尾的"清除后重检"路径补发。
        if (_ready.Writer.TryWrite(queue.ConnectionId))
            return;

        Interlocked.Exchange(ref queue.Active, 0);

        // 短路重试：可能 worker 在 TryWrite 失败前已排空队列。
        if (queue.Commands.IsEmpty)
            return;

        if (Interlocked.CompareExchange(ref queue.Active, 1, 0) == 0)
            _ready.Writer.TryWrite(queue.ConnectionId);
    }

    /// <summary>
    /// 启动 worker 池。重复调用幂等。
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_workers.Length > 0)
            return Task.CompletedTask;

        var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        _workers = new Task[_workerCount];
        for (var i = 0; i < _workerCount; i++)
        {
            _workers[i] = RunWorkerAsync(linked.Token);
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止 worker 池：取消内部 CTS、完成 ready channel、等待所有 worker 退出，
    /// 并排空所有连接队列（释放缓冲区与入站预算）。
    /// 与 <see cref="DisposeAsync"/> 的区别：本方法等待 worker 退出（DisposeAsync 也排空队列，
    /// 但不重复等待已观察过 StopAsync 的 worker）；二者都保证队列被排空。
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // 不 early-return：即使 StartAsync 未调用（_workers 为空），也必须排空队列
        // 释放缓冲区与入站预算，否则调用方依赖 StopAsync 释放资源的契约会被破坏。
        _cts.Cancel();
        _ready.Writer.TryComplete();

        foreach (var worker in _workers)
        {
            if (worker is null)
                continue;
            try
            {
                await worker.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Worker exceptions observed via onFatalError or swallowed on stop.
            }
        }

        // 排空所有连接队列，释放缓冲区与入站预算。
        foreach (var queue in _queues.Values)
        {
            while (queue.Commands.TryDequeue(out var command))
            {
                queue.Count--;
                ReleaseCommandResources(in command);
            }
        }
        _queues.Clear();
    }

    private async Task RunWorkerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var connectionId in _ready.Reader.ReadAllAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                if (!_queues.TryGetValue(connectionId, out var queue))
                    continue;

                try
                {
                    await ProcessBurstAsync(queue, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _onFatalError?.Invoke(ex);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (ChannelClosedException)
        {
            // Shutdown.
        }
    }

    private async Task ProcessBurstAsync(
        ConnectionQueue queue,
        CancellationToken cancellationToken)
    {
        var processed = 0;
        while (processed < _burstLimit)
        {
            if (!queue.Commands.TryDequeue(out var command))
                break;

            queue.Count--;

            // per-User 并发上限：仅 Query lane 使用。OrderedWrite lane 构造时 perUserConcurrency=0 不触发。
            if (_perUserGate is not null)
            {
                await _perUserGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await ProcessCommandAsync(command, cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    _perUserGate.Release();
                }
            }
            else
            {
                await ProcessCommandAsync(command, cancellationToken)
                    .ConfigureAwait(false);
            }

            processed++;
        }

        // burst 结束：如果队列还有命令（达到 burstLimit），重新入 ready channel 继续处理。
        if (!queue.Commands.IsEmpty)
        {
            _ready.Writer.TryWrite(queue.ConnectionId);
            return;
        }

        // 队列已空：清除 Active 标志。必须严格处理"清除标志与新入队"竞态：
        //   生产者可能在队列空检查与 Active 清除之间入队，
        //   此时生产者的 CAS(0→1) 会失败（因为 Active 仍为 1），
        //   它依赖此处清除后的重检来补发 ready signal。
        Interlocked.Exchange(ref queue.Active, 0);

        // 清除后重检：若队列非空，重新 CAS + TryWrite。
        if (!queue.Commands.IsEmpty
            && Interlocked.CompareExchange(ref queue.Active, 1, 0) == 0)
        {
            _ready.Writer.TryWrite(queue.ConnectionId);
        }
    }

    private async Task ProcessCommandAsync(
        SessionCommand command,
        CancellationToken cancellationToken)
    {
        // 命令超时：为每条命令创建独立 CTS。Zero 表示不启用。
        if (_commandTimeout <= TimeSpan.Zero)
        {
            try
            {
                await _processor(command, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ReleaseCommandResources(in command);
            }
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_commandTimeout);
        try
        {
            await _processor(command, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            ReleaseCommandResources(in command);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _cts.Cancel();
        _ready.Writer.TryComplete();

        foreach (var worker in _workers)
        {
            if (worker is null)
                continue;
            try
            {
                await worker.ConfigureAwait(false);
            }
            catch
            {
                // Worker exceptions observed via onFatalError or swallowed on dispose.
            }
        }

        // 排空所有连接队列，释放缓冲区与入站预算。
        foreach (var queue in _queues.Values)
        {
            while (queue.Commands.TryDequeue(out var command))
            {
                queue.Count--;
                ReleaseCommandResources(in command);
            }
        }
        _queues.Clear();

        _cts.Dispose();
        _perUserGate?.Dispose();
    }

    private static void ReleaseCommandResources(in SessionCommand command)
    {
        if (command.IsPooled && command.RentedBuffer.Length > 0)
            System.Buffers.ArrayPool<byte>.Shared.Return(command.RentedBuffer);

        if (command.ReservedInboundBytes > 0 && command.InboundBudget is not null)
            command.InboundBudget.Release(command.ReservedInboundBytes);
    }

    private sealed class ConnectionQueue
    {
        public readonly ConcurrentQueue<SessionCommand> Commands = new();
        public readonly uint ConnectionId;
        public readonly long UserId;
        public int Active; // 0 = idle, 1 = worker processing
        public int Count; // 入队计数，与 Commands.Count 等价但避免遍历

        public ConnectionQueue(uint connectionId, long userId)
        {
            ConnectionId = connectionId;
            UserId = userId;
        }
    }
}
