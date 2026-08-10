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
/// <item>每连接仅保留轻量 holder，首次入队时才惰性创建
/// <see cref="ConcurrentQueue{T}"/>，不再保留专属消费者 Task；</item>
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

    private readonly Channel<ConnectionQueue> _ready;
    private readonly ConcurrentDictionary<uint, ConnectionQueue> _queues = new();
    private readonly object _registrationGate = new();
    private readonly SemaphoreSlim? _perUserGate;
    private readonly CancellationTokenSource _cts;
    private CancellationTokenSource? _linkedCts;
    private Task[] _workers = Array.Empty<Task>();
    private bool _acceptingRegistrations = true;
    private bool _disposed;

    /// <summary>
    /// 创建执行器。
    /// </summary>
    /// <param name="processor">命令处理回调。完成后调用，异常会被捕获并转为 fatal 回调。</param>
    /// <param name="workerCount">全局 worker 数。</param>
    /// <param name="burstLimit">单连接单次调度处理的命令上限，防止单连接独占 worker。</param>
    /// <param name="perConnectionCapacity">每连接队列容量。满了 TryEnqueue 返回 false。</param>
    /// <param name="globalCapacity">全局 ready channel 容量（待调度的连接数上限）。保留参数用于兼容，实际使用无界 ready channel。</param>
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

        // Ready channel 使用无界：每个 holder 通过 CAS Active 保证最多一个节点在
        // ready queue 中。直接传 holder 而非 connectionId，避免注销后复用同一 ID 时
        // 旧 ready 通知错误驱动新连接（ABA）。
        // 这避免了 BoundedChannel 满时 TryWrite 失败导致的丢失唤醒问题。
        // globalCapacity 参数保留用于未来限流策略，当前不应用。
        _ready = Channel.CreateUnbounded<ConnectionQueue>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
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
    public bool TryRegisterConnection(
        uint connectionId,
        long userId,
        out Registration registration)
    {
        registration = default;

        // 注册不在命令热路径上。与停机关闭准入共用此锁，保证 Stop/Dispose
        // 返回前不存在“枚举之后才加入”的 holder。
        lock (_registrationGate)
        {
            if (!_acceptingRegistrations)
                return false;

            var queue = new ConnectionQueue(connectionId, userId);
            if (!_queues.TryAdd(connectionId, queue))
                return false;

            registration = Registration.Create(this, queue);
            return true;
        }
    }

    /// <summary>
    /// 注销连接并丢弃队列中残留命令（释放缓冲区与入站预算）。
    /// </summary>
    private void UnregisterConnection(ConnectionQueue queue)
    {
        // Lease 同时绑定 executor 与 holder 引用。旧 session 的 finally 或失败回滚
        // 只能删除自己注册的 holder，不会按裸 connectionId 误删后继连接。
        if (_queues.TryRemove(
                new KeyValuePair<uint, ConnectionQueue>(
                    queue.ConnectionId,
                    queue)))
        {
            queue.CloseAndDrain();
        }
    }

    /// <summary>
    /// 入队命令。队列满时返回 false（调用方负责释放资源）。
    /// 入队成功后会通知 ready channel 唤醒一个 worker。
    /// </summary>
    private bool TryEnqueue(
        ConnectionQueue queue,
        in SessionCommand command)
    {
        // holder 内部把“仍开放 + 容量预留 + 首次队列创建 + Enqueue”作为一个
        // admission 临界区。这样 TryGetValue 后并发注销也不会把命令留在已移除 holder。
        if (!queue.TryEnqueue(in command, _perConnectionCapacity))
            return false;

        // CAS _active: 0→1。成功表示此前无 worker 处理该连接，需通知 ready channel。
        // 失败表示已有 worker 在处理，它会在 burst 循环中 drain 到空，无需额外唤醒。
        SignalReadyIfNeeded(queue);

        return true;
    }

    private void SignalReadyIfNeeded(ConnectionQueue queue)
    {
        if (Interlocked.CompareExchange(ref queue.Active, 1, 0) != 0)
            return;

        // 无界 ready channel：TryWrite 不会因容量满而失败。
        // 仅在 channel 已关闭（停机）时返回 false，此时回退 Active 即可。
        if (!_ready.Writer.TryWrite(queue))
        {
            Interlocked.Exchange(ref queue.Active, 0);
        }
    }

    /// <summary>
    /// 启动 worker 池。重复调用幂等。
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_workers.Length > 0)
            return Task.CompletedTask;

        _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _cts.Token);
        _workers = new Task[_workerCount];
        for (var i = 0; i < _workerCount; i++)
        {
            _workers[i] = RunWorkerAsync(_linkedCts.Token);
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
        CloseRegistrationAndDrainConnections();
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

        DrainReadyNotifications();
        // CloseRegistrationAndDrainConnections 已关闭 admission 并排空所有 holder。
    }

    private async Task RunWorkerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var queue in _ready.Reader.ReadAllAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
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
            if (!queue.TryDequeue(out var command))
                break;

            try
            {
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
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                // 单条命令失败不能让 ConnectionQueue 永久保持 Active=1。
                _onFatalError?.Invoke(exception);
            }
            finally
            {
                // dequeue 即转移资源所有权给当前 worker。无论在 per-user gate、
                // timeout 包装还是 processor 的哪一步失败，都必须恰好释放一次。
                SessionCommandResources.Release(in command);
            }

            processed++;
        }

        // burst 结束：如果队列还有命令（达到 burstLimit），重新入 ready channel 继续处理。
        // 无界 ready channel 不会 TryWrite 失败（除非 channel 已关闭，此时连接也在停机）。
        if (queue.HasCommands)
        {
            _ready.Writer.TryWrite(queue);
            return;
        }

        // 队列已空：清除 Active 标志。必须严格处理"清除标志与新入队"竞态：
        //   生产者可能在队列空检查与 Active 清除之间入队，
        //   此时生产者的 CAS(0→1) 会失败（因为 Active 仍为 1），
        //   它依赖此处清除后的重检来补发 ready signal。
        Interlocked.Exchange(ref queue.Active, 0);

        // 清除后重检：若队列非空，重新 CAS + TryWrite。
        if (queue.HasCommands
            && Interlocked.CompareExchange(ref queue.Active, 1, 0) == 0)
        {
            _ready.Writer.TryWrite(queue);
        }
    }

    private async Task ProcessCommandAsync(
        SessionCommand command,
        CancellationToken cancellationToken)
    {
        // 命令超时：为每条命令创建独立 CTS。Zero 表示不启用。
        if (_commandTimeout <= TimeSpan.Zero)
        {
            await _processor(command, cancellationToken).ConfigureAwait(false);
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_commandTimeout);
        await _processor(command, cts.Token).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        CloseRegistrationAndDrainConnections();
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

        DrainReadyNotifications();
        _cts.Dispose();
        _linkedCts?.Dispose();
        _linkedCts = null;
        _perUserGate?.Dispose();
    }

    /// <summary>
    /// 当前已注册 holder 数，仅用于诊断和结构测试。
    /// </summary>
    internal int RegisteredConnectionCount => _queues.Count;

    /// <summary>
    /// 已实际创建命令队列的 holder 数。空闲且从未收到此 lane 命令的连接不计入。
    /// </summary>
    internal int AllocatedCommandQueueCount
    {
        get
        {
            var count = 0;
            foreach (var queue in _queues.Values)
            {
                if (queue.HasAllocatedCommandQueue)
                    count++;
            }

            return count;
        }
    }

    /// <summary>
    /// 一次成功注册返回的不透明租约。租约以 holder 对象身份而非可复用的
    /// connectionId 作为释放凭据；default/其他 executor 的租约均为安全 no-op。
    /// </summary>
    internal readonly struct Registration
    {
        private readonly SessionCommandExecutor? _owner;
        private readonly ConnectionQueue? _queue;

        private Registration(
            SessionCommandExecutor owner,
            ConnectionQueue queue)
        {
            _owner = owner;
            _queue = queue;
        }

        public bool IsValid => _owner is not null && _queue is not null;

        public bool TryEnqueue(in SessionCommand command)
        {
            var owner = _owner;
            var queue = _queue;
            return owner is not null &&
                   queue is not null &&
                   owner.TryEnqueue(queue, in command);
        }

        public void Unregister()
        {
            var owner = _owner;
            var queue = _queue;
            if (owner is not null && queue is not null)
                owner.UnregisterConnection(queue);
        }

        // C# 不允许 enclosing type 调用 nested type 的 private constructor。
        // object 只用于冷路径工厂边界；热路径仍直接持有强类型 holder 引用。
        internal static Registration Create(
            SessionCommandExecutor owner,
            object queue)
            => new(owner, (ConnectionQueue)queue);
    }

    private void CloseRegistrationAndDrainConnections()
    {
        lock (_registrationGate)
        {
            _acceptingRegistrations = false;
        }

        // 关闭注册准入后不会再有新 holder。循环删除而非 Clear，确保每个被移除
        // holder 都先关闭命令 admission，再释放其拥有的 payload 与全局预算。
        while (!_queues.IsEmpty)
        {
            foreach (var entry in _queues)
            {
                if (_queues.TryRemove(entry.Key, out var queue))
                    queue.CloseAndDrain();
            }
        }
    }

    private void DrainReadyNotifications()
    {
        // Ready 项直接持有 holder 以规避 connectionId 复用 ABA。worker 已退出且
        // writer 已关闭后清空残留通知，避免已注销 holder 被 executor 长时间保留。
        while (_ready.Reader.TryRead(out _))
        {
        }
    }

    private sealed class ConnectionQueue
    {
        private ConcurrentQueue<SessionCommand>? _commands;
        private SpinLock _admissionLock = new(enableThreadOwnerTracking: false);
        private int _count;
        private bool _acceptingCommands = true;

        public readonly uint ConnectionId;
        public readonly long UserId;
        public int Active; // 0 = idle, 1 = worker processing

        public bool HasAllocatedCommandQueue =>
            Volatile.Read(ref _commands) is not null;

        public bool HasCommands
        {
            get
            {
                var commands = Volatile.Read(ref _commands);
                return commands is not null && !commands.IsEmpty;
            }
        }

        public ConnectionQueue(uint connectionId, long userId)
        {
            ConnectionId = connectionId;
            UserId = userId;
        }

        public bool TryEnqueue(
            in SessionCommand command,
            int capacity)
        {
            // 单连接通常只有读循环这一个生产者；不额外分配 gate 对象的短时
            // SpinLock 将注销竞态与容量上限做成精确、易审计的原子 admission。
            var lockTaken = false;
            try
            {
                _admissionLock.Enter(ref lockTaken);
                if (!_acceptingCommands ||
                    Volatile.Read(ref _count) >= capacity)
                {
                    return false;
                }

                var commands = _commands;
                if (commands is null)
                {
                    commands = new ConcurrentQueue<SessionCommand>();
                    Volatile.Write(ref _commands, commands);
                }

                // 先预留计数再发布命令。已有 worker 可在生产者持锁期间消费，
                // 因而必须使用 Interlocked，不能用普通 read/modify/write。
                Interlocked.Increment(ref _count);
                commands.Enqueue(command);
                return true;
            }
            finally
            {
                if (lockTaken)
                    _admissionLock.Exit(useMemoryBarrier: true);
            }
        }

        public bool TryDequeue(out SessionCommand command)
        {
            var commands = Volatile.Read(ref _commands);
            if (commands is null || !commands.TryDequeue(out command))
            {
                command = default;
                return false;
            }

            Interlocked.Decrement(ref _count);
            return true;
        }

        public void CloseAndDrain()
        {
            ConcurrentQueue<SessionCommand>? commands;
            var lockTaken = false;
            try
            {
                _admissionLock.Enter(ref lockTaken);
                // 与 TryEnqueue 的 admission 临界区互斥：返回后不会再有新命令
                // 发布到此 holder。重复关闭安全，ConcurrentQueue 保证每条只取一次。
                _acceptingCommands = false;
                commands = _commands;
                // Ready channel 可能短暂保留已注销 holder。立即断开空队列 segment，
                // 仅由当前关闭调用的局部引用完成 drain，避免 stale ready 延长其寿命。
                _commands = null;
            }
            finally
            {
                if (lockTaken)
                    _admissionLock.Exit(useMemoryBarrier: true);
            }

            if (commands is null)
                return;

            while (commands.TryDequeue(out var command))
            {
                Interlocked.Decrement(ref _count);
                SessionCommandResources.Release(in command);
            }
        }
    }
}
