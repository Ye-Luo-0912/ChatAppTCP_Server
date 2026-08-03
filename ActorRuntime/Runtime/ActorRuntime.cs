using System.Runtime.CompilerServices;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Scheduling;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 分片单写 Actor Runtime。外部消息经 MPSC Ring 路由，同 Key 严格串行，
/// 跨 Shard 并行。一个共享 PeriodicTimer 为所有 Shard 提供 deadline pulse。
/// </summary>
public sealed class ActorRuntime<TKey, TState, TMessage> :
    IActorRuntime<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    private readonly ActorShard<TKey, TState, TMessage>[] _shards;
    private readonly AsyncOperationExecutor _asyncExecutor;
    private readonly GlobalActorAdmissionQuota _globalQuota;
    private readonly int _shardMask;
    private readonly ActorRuntimeOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _pulseCts = new();

    // 0=created, 1=started, 2=stopping, 3=stopped
    private int _lifecycle;
    private Task? _pulseTask;

    public ActorRuntime(
        IActorBehavior<TKey, TState, TMessage> behavior,
        ActorMailboxMode mailboxMode,
        ActorRuntimeOptions options,
        TimeProvider? timeProvider = null,
        IActorMessageDropHandler<TMessage>? dropHandler = null)
    {
        ArgumentNullException.ThrowIfNull(behavior);
        ArgumentNullException.ThrowIfNull(options);
        ActorRuntimeOptionsValidation.Validate(options);

        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _shardMask = options.ShardCount - 1;
        _asyncExecutor = new AsyncOperationExecutor(
            options.AsyncOperationConcurrency,
            options.AsyncOperationQueueCapacity,
            options.AsyncOperationTimeout,
            _timeProvider);
        _globalQuota = new GlobalActorAdmissionQuota(options.MaxActiveActors);

        var maxActorsPerShard = options.MaxActiveActorsPerShard > 0
            ? options.MaxActiveActorsPerShard
            : (options.MaxActiveActors + options.ShardCount - 1) /
              options.ShardCount;
        // Completion Ring 容量基于每 Shard 可能的 Outstanding Operation 上限，
        // 而非 MaxActors。每 Shard 的 Outstanding Operation ≤
        // (AsyncOperationQueueCapacity + AsyncOperationConcurrency) / ShardCount + 余量。
        // 这比 maxActorsPerShard 小得多（默认 100k Actor vs 4k+并发的工作队列）。
        var perShardMaxOutstanding =
            (options.AsyncOperationQueueCapacity + options.AsyncOperationConcurrency +
             options.ShardCount - 1) / options.ShardCount;
        var completionCreditCapacity =
            NextPowerOfTwo(Math.Max(2, perShardMaxOutstanding));

        _shards =
            new ActorShard<TKey, TState, TMessage>[options.ShardCount];
        for (var i = 0; i < options.ShardCount; i++)
        {
            _shards[i] = new ActorShard<TKey, TState, TMessage>(
                shardIndex: i,
                behavior,
                dropHandler,
                mailboxMode,
                options.DefaultMailboxCapacity,
                options.ShardIngressCapacity,
                options.ShardBurstLimit,
                options.MaxMessagesPerActorTurn,
                options.ShardTickInterval,
                options.ActorIdleTimeout,
                maxActorsPerShard,
                completionCreditCapacity,
                _timeProvider,
                _asyncExecutor,
                _globalQuota);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryTell(
        in TKey key,
        in TMessage message)
    {
        if (Volatile.Read(ref _lifecycle) >= 2)
            return ActorPostStatus.RuntimeStopping;

        return _shards[GetShardIndex(in key)]
            .TryEnqueue(in key, in message);
    }

    /// <summary>
    /// 临时消息入队：等同于 <see cref="TryTell"/>，不检查 Actor 数量配额。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryTellEphemeral(
        in TKey key,
        in TMessage message)
        => TryTell(in key, in message);

    /// <summary>
    /// 持久消息入队：在生产侧消耗式预留 Actor 激活配额（<see cref="ActorRoute"/> 状态机）。
    /// 若全局配额已满或每 Shard 上限已满，返回 AdmissionRejected 且不入队，避免持久消息被静默丢弃。
    /// <para>
    /// P0-2：通过 <see cref="ActorRoute"/> 状态机原子地预留激活配额 + 邮件配额，消除
    /// "探测 Actor 存在 → 消费时已回收"的 TOCTOU 竞态。不再使用 ContainsActor 快照探测。
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryTellDurable(
        in TKey key,
        in TMessage message)
    {
        if (Volatile.Read(ref _lifecycle) >= 2)
            return ActorPostStatus.RuntimeStopping;

        var shardIndex = GetShardIndex(in key);
        var shard = _shards[shardIndex];

        return shard.TryEnqueueDurable(in key, in message);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryTellCompletion(
        in TKey key,
        ActivationId activation,
        in TMessage message)
    {
        // Drain 期间仍允许在途异步操作回投 Completion；真正 stopped 后拒绝。
        if (Volatile.Read(ref _lifecycle) == 3)
            return ActorPostStatus.RuntimeStopping;

        return _shards[GetShardIndex(in key)]
            .TryEnqueueCompletion(
                in key,
                activation,
                in message);
    }

    /// <summary>
    /// 投递 Invalidation 控制消息：经普通 Ingress Ring 路由，由 Shard Consumer
    /// 投递到 ActorCell 的 Invalidation 控制槽（优先级高于 Completion）。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryTellInvalidation(in TKey key, in TMessage message)
    {
        if (Volatile.Read(ref _lifecycle) >= 2)
            return ActorPostStatus.RuntimeStopping;

        return _shards[GetShardIndex(in key)]
            .TryEnqueueInvalidation(in key, in message);
    }

    public bool TryDeactivate(
        in TKey key,
        ActorDeactivateReason reason)
        => TryDeactivate(in key, ActivationId.None, reason);

    public bool TryDeactivate(
        in TKey key,
        ActivationId activation,
        ActorDeactivateReason reason)
    {
        if (Volatile.Read(ref _lifecycle) >= 2)
            return false;

        return _shards[GetShardIndex(in key)]
            .TryEnqueueDeactivate(
                in key,
                activation,
                reason);
    }

    public async ValueTask StartAsync(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.CompareExchange(
                ref _lifecycle,
                1,
                0) != 0)
        {
            return;
        }

        await _asyncExecutor
            .StartAsync(CancellationToken.None)
            .ConfigureAwait(false);

        for (var i = 0; i < _shards.Length; i++)
        {
            await _shards[i]
                .StartAsync(CancellationToken.None)
                .ConfigureAwait(false);
        }

        _pulseTask = RunPulseLoopAsync();
    }

    public async ValueTask StopAsync(
        ActorStopMode mode,
        CancellationToken cancellationToken)
    {
        int previous;
        do
        {
            previous = Volatile.Read(ref _lifecycle);
            if (previous is 2 or 3)
                return;
        }
        while (Interlocked.CompareExchange(
                   ref _lifecycle,
                   2,
                   previous) != previous);

        // Stop-before-Start cannot drain because no Consumer was started.
        if (mode == ActorStopMode.Drain && previous == 1)
        {
            try
            {
                while (!IsDrained())
                {
                    await Task.Delay(
                            TimeSpan.FromMilliseconds(1),
                            _timeProvider,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                // Drain deadline reached: fall through to bounded immediate cleanup.
            }
        }

        await _pulseCts.CancelAsync().ConfigureAwait(false);
        if (_pulseTask is not null)
        {
            try
            {
                await _pulseTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        // Executor 先停止并触发 in-flight OnFailure；Shard 仍存活，可接收失败 Completion。
        await _asyncExecutor.StopAsync().ConfigureAwait(false);

        for (var i = 0; i < _shards.Length; i++)
            await _shards[i].StopAsync().ConfigureAwait(false);

        Volatile.Write(ref _lifecycle, 3);
    }

    public ActorRuntimeSnapshot GetSnapshot()
    {
        long activeActors = 0;
        long busyActors = 0;
        long pendingIngress = 0;
        long pendingMailbox = 0;
        long pendingDeadlines = 0;
        long totalProcessed = 0;
        long totalMailboxFull = 0;
        long totalShardOverloaded = 0;
        long totalDeactivations = 0;
        long totalActivations = 0;
        long totalAdmissionRejected = 0;
        long totalReplaced = 0;

        foreach (var shard in _shards)
        {
            activeActors += shard.ActiveActorCount;
            busyActors += shard.BusyActorCount;
            pendingIngress += shard.PendingIngress;
            pendingMailbox += shard.PendingMailbox;
            pendingDeadlines += shard.PendingDeadlines;
            totalProcessed += shard.ProcessedCount;
            totalMailboxFull += shard.MailboxFullCount;
            totalShardOverloaded += shard.ShardOverloadedCount;
            totalDeactivations += shard.DeactivationCount;
            totalActivations += shard.ActivationCount;
            totalAdmissionRejected += shard.AdmissionRejectedCount;
            totalReplaced += shard.ReplacedCount;
        }

        var asyncSnapshot = _asyncExecutor.GetSnapshot();
        return new ActorRuntimeSnapshot
        {
            ActiveActors = activeActors,
            BusyActors = busyActors,
            PendingIngress = pendingIngress,
            PendingMailbox = pendingMailbox,
            PendingDeadlines = pendingDeadlines,
            TotalProcessed = totalProcessed,
            TotalMailboxFull = totalMailboxFull,
            TotalShardOverloaded = totalShardOverloaded,
            TotalDeactivations = totalDeactivations,
            TotalActivations = totalActivations,
            TotalActiveActorAdmissionRejected = totalAdmissionRejected,
            TotalReplaced = totalReplaced,
            PendingAsyncOperations = asyncSnapshot.PendingCount,
            TotalAsyncOperationsSubmitted = asyncSnapshot.TotalSubmitted,
            TotalAsyncOperationsCompleted = asyncSnapshot.TotalCompleted,
            TotalAsyncOperationsRejected = asyncSnapshot.TotalRejected
        };
    }

    private bool IsDrained()
    {
        if (_asyncExecutor.PendingCount != 0)
            return false;

        foreach (var shard in _shards)
        {
            if (shard.PendingIngress != 0 ||
                shard.PendingMailbox != 0 ||
                shard.BusyActorCount != 0)
            {
                return false;
            }
        }

        return true;
    }

    private async Task RunPulseLoopAsync()
    {
        using var timer = new PeriodicTimer(
            _options.ShardTickInterval,
            _timeProvider);

        try
        {
            while (await timer
                       .WaitForNextTickAsync(_pulseCts.Token)
                       .ConfigureAwait(false))
            {
                for (var i = 0; i < _shards.Length; i++)
                    _shards[i].Pulse();
            }
        }
        catch (OperationCanceledException)
            when (_pulseCts.IsCancellationRequested)
        {
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetShardIndex(in TKey key)
    {
        // Runtime 是进程内调度器，不承诺跨进程分片稳定性。
        var hash = key.GetHashCode();
        unchecked
        {
            hash = (hash ^ (hash >>> 16)) * 0x45D9F3B;
            hash = (hash ^ (hash >>> 16)) * 0x45D9F3B;
            hash ^= hash >>> 16;
        }

        return hash & _shardMask;
    }

    private static int NextPowerOfTwo(int value)
    {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(
                ActorStopMode.Immediate,
                CancellationToken.None)
            .ConfigureAwait(false);
        _pulseCts.Dispose();
        await _asyncExecutor.DisposeAsync().ConfigureAwait(false);
    }
}
