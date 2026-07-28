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
        // Completion Ring 容量 == 每 Shard Completion Credit 上限。
        // 严格上界：单 Outstanding Operation 约束下每 Actor 至多持有一个 Credit，
        // 而每 Shard 活跃 Actor 数 ≤ maxActorsPerShard，故 Ring 永远不会溢出。
        var completionCreditCapacity =
            NextPowerOfTwo(Math.Max(2, maxActorsPerShard));

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
