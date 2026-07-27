using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Primitives;
using ChatApp.ActorRuntime.Scheduling;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 单写 Shard：MPSC Ingress、ActorCell 表、侵入式 Ready Queue 与 DeadlineWheel。
/// 跨线程只接触 Ingress、FIFO admission 和原子统计；Actor 状态与 Mailbox 始终由单线程拥有。
/// </summary>
internal sealed class ActorShard<TKey, TState, TMessage>
    : IActorContextSink<TKey, TState, TMessage>, IDeadlineCallback<TKey, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    private readonly int _shardIndex;
    private readonly IActorBehavior<TKey, TState, TMessage> _behavior;
    private readonly IActorMessageDropHandler<TMessage>? _dropHandler;
    private readonly ActorMailboxMode _mailboxMode;
    private readonly int _mailboxCapacity;
    private readonly int _shardBurstLimit;
    private readonly int _maxMessagesPerActorTurn;
    private readonly int _maxIngressDrain;
    private readonly TimeProvider _timeProvider;
    private readonly long _idleTimeoutTimestamp;
    private readonly long _idleSweepIntervalTimestamp;

    private readonly BoundedMpscRing<ActorEnvelope<TKey, TMessage>> _ingress;
    // Completion 常态走预分配 MPSC Ring；极端瞬时溢出才惰性创建 ConcurrentQueue。
    // 该通道与普通 Ingress 隔离，保证恢复信号不会因业务消息满载而丢失。
    private readonly BoundedMpscRing<ActorEnvelope<TKey, TMessage>> _completionIngress;
    private ConcurrentQueue<ActorEnvelope<TKey, TMessage>>? _completionOverflow;
    private readonly ActorCellTable<TKey, TState, TMessage> _cells;
    private readonly ConcurrentDictionary<TKey, ActorAdmission>? _fifoAdmissions;
    private readonly SingleWaiterSignal _signal;
    private readonly ShardDeadlineWheel<TKey, TState, TMessage> _deadlines;
    private readonly AsyncOperationExecutor _asyncExecutor;

    // 单线程侵入式 Ready Queue：不分配节点、不设固定容量、不会丢失调度。
    private ActorCell<TKey, TState, TMessage>? _readyHead;
    private ActorCell<TKey, TState, TMessage>? _readyTail;
    private int _readyCount;

    private readonly List<TKey> _idleRemovalKeys = new(64);
    private long _nextIdleSweepTimestamp;

    // Consumer-only 计数用普通 long；快照通过 Volatile.Read，避免每消息 Interlocked。
    private long _processedCount;
    private long _deactivationCount;
    private long _activeActorCount;
    private long _busyActorCount;
    private long _pendingMailboxCount;

    // Producer 也会写入的拒绝计数必须原子更新。
    private long _mailboxFullCount;
    private long _shardOverloadedCount;
    private int _pendingCompletionIngress;

    private CancellationTokenSource? _cts;
    private Task? _loopTask;
    private volatile bool _stopping;

    [ThreadStatic]
    private static TKey? _currentKey;

    [ThreadStatic]
    private static bool _hasCurrentKey;

    public ActorShard(
        int shardIndex,
        IActorBehavior<TKey, TState, TMessage> behavior,
        IActorMessageDropHandler<TMessage>? dropHandler,
        ActorMailboxMode mailboxMode,
        int mailboxCapacity,
        int ingressCapacity,
        int shardBurstLimit,
        int maxMessagesPerActorTurn,
        TimeSpan tickInterval,
        TimeSpan idleTimeout,
        TimeProvider timeProvider,
        AsyncOperationExecutor asyncExecutor)
    {
        _shardIndex = shardIndex;
        _behavior = behavior;
        _dropHandler = dropHandler;
        _mailboxMode = mailboxMode;
        _mailboxCapacity = mailboxCapacity;
        _shardBurstLimit = shardBurstLimit;
        _maxMessagesPerActorTurn = maxMessagesPerActorTurn;
        _maxIngressDrain = Math.Max(64, shardBurstLimit * maxMessagesPerActorTurn);
        _timeProvider = timeProvider;
        _idleTimeoutTimestamp = ToTimestampUnits(idleTimeout, timeProvider);
        _idleSweepIntervalTimestamp = _idleTimeoutTimestamp > 0
            ? Math.Min(
                Math.Max(_idleTimeoutTimestamp / 4, ToTimestampUnits(tickInterval, timeProvider)),
                ToTimestampUnits(TimeSpan.FromSeconds(30), timeProvider))
            : 0;
        _nextIdleSweepTimestamp = timeProvider.GetTimestamp() + _idleSweepIntervalTimestamp;

        _ingress = new BoundedMpscRing<ActorEnvelope<TKey, TMessage>>(ingressCapacity);
        _completionIngress =
            new BoundedMpscRing<ActorEnvelope<TKey, TMessage>>(
                Math.Min(256, ingressCapacity));
        _cells = new ActorCellTable<TKey, TState, TMessage>(initialCapacity: 64);
        _fifoAdmissions = mailboxMode == ActorMailboxMode.Fifo
            ? new ConcurrentDictionary<TKey, ActorAdmission>()
            : null;
        _signal = new SingleWaiterSignal();
        _deadlines = new ShardDeadlineWheel<TKey, TState, TMessage>(
            timeProvider,
            tickInterval,
            this);
        _asyncExecutor = asyncExecutor;
    }

    public int ShardIndex => _shardIndex;
    public long ProcessedCount => Volatile.Read(ref _processedCount);
    public long MailboxFullCount => Volatile.Read(ref _mailboxFullCount);
    public long ShardOverloadedCount => Volatile.Read(ref _shardOverloadedCount);
    public long DeactivationCount => Volatile.Read(ref _deactivationCount);
    public long PendingIngress =>
        _ingress.Count + Volatile.Read(ref _pendingCompletionIngress);
    public long PendingMailbox => Volatile.Read(ref _pendingMailboxCount);
    public long ActiveActorCount => Volatile.Read(ref _activeActorCount);
    public long BusyActorCount => Volatile.Read(ref _busyActorCount);
    public int PendingDeadlines => _deadlines.PendingCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueue(in TKey key, in TMessage message)
        => TryEnqueueCore(
            in key,
            in message,
            generation: 0,
            ActorEnvelopeKind.Message);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueueCompletion(
        in TKey key,
        uint generation,
        in TMessage message)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            in message,
            admission: null,
            generation,
            ActorEnvelopeKind.Completion);
        Interlocked.Increment(ref _pendingCompletionIngress);
        if (!_completionIngress.TryEnqueue(in envelope))
            EnqueueCompletionOverflow(in envelope);

        _signal.Signal();
        return ActorPostStatus.Accepted;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ActorPostStatus TryEnqueueCore(
        in TKey key,
        in TMessage message,
        uint generation,
        ActorEnvelopeKind kind)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        ActorAdmission? admission = null;
        if (_fifoAdmissions is not null && kind != ActorEnvelopeKind.Completion)
        {
            while (true)
            {
                admission = _fifoAdmissions.GetOrAdd(
                    key,
                    static _ => new ActorAdmission());
                if (admission.TryReserve(_mailboxCapacity))
                    break;

                if (!admission.IsRetired)
                {
                    Interlocked.Increment(ref _mailboxFullCount);
                    return ActorPostStatus.MailboxFull;
                }

                RemoveAdmissionIfSame(in key, admission);
            }
        }

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            in message,
            admission,
            generation,
            kind);
        if (!_ingress.TryEnqueue(in envelope))
        {
            admission?.Release();
            Interlocked.Increment(ref _shardOverloadedCount);
            return ActorPostStatus.ShardOverloaded;
        }

        _signal.Signal();
        return ActorPostStatus.Accepted;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_loopTask is not null)
            return Task.CompletedTask;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loopTask = RunConsumerLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public void Pulse()
    {
        if (!_stopping)
            _signal.Signal();
    }

    public async Task StopAsync()
    {
        if (_stopping)
            return;

        _stopping = true;
        _signal.Signal();
        _signal.Complete();
        if (_cts is not null)
            await _cts.CancelAsync().ConfigureAwait(false);

        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch
            {
                // Runtime shutdown is best effort; resources are released below.
            }
        }

        while (_ingress.TryDequeue(out var envelope))
            DropEnvelope(in envelope, ActorMessageDropReason.RuntimeStopping);
        while (_completionIngress.TryDequeue(out var completion))
        {
            Interlocked.Decrement(ref _pendingCompletionIngress);
            DropEnvelope(in completion, ActorMessageDropReason.RuntimeStopping);
        }
        var overflow = Volatile.Read(ref _completionOverflow);
        if (overflow is not null)
        {
            while (overflow.TryDequeue(out var completion))
            {
                Interlocked.Decrement(ref _pendingCompletionIngress);
                DropEnvelope(
                    in completion,
                    ActorMessageDropReason.RuntimeStopping);
            }
        }

        _deadlines.Stop();

        if (_cells.Count > 0)
        {
            var timestamp = _timeProvider.GetTimestamp();
            foreach (var cell in _cells.Values)
            {
                if (cell is null || !cell.IsActive)
                    continue;

                SetCurrentKey(in cell.Key);
                var ctx = new ActorContext<TKey, TState, TMessage>(
                    this,
                    timestamp,
                    cell.Generation);
                try
                {
                    _behavior.Deactivate(
                        in cell.Key,
                        ref cell.State,
                        ActorDeactivateReason.RuntimeStopping,
                        ref ctx);
                }
                catch
                {
                }

                DrainCellMessages(cell, ActorMessageDropReason.RuntimeStopping);
                cell.Deactivate();
                cell.ReleaseStorage();
                _deactivationCount++;
            }

            ClearCurrentKey();
            _cells.Clear();
        }

        ClearReadyQueue();
        _fifoAdmissions?.Clear();
        Volatile.Write(ref _activeActorCount, 0);
        Volatile.Write(ref _busyActorCount, 0);
        Volatile.Write(ref _pendingMailboxCount, 0);

        _cts?.Dispose();
        _cts = null;
    }

    private async Task RunConsumerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                DrainIngress(_maxIngressDrain);
                ProcessReadyQueue(cancellationToken);
                _deadlines.PumpExpired();
                SweepIdleActorsIfDue();

                if (_ingress.Count == 0 &&
                    Volatile.Read(ref _pendingCompletionIngress) == 0 &&
                    _readyCount == 0)
                {
                    await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
                    _signal.TryReset();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ChannelClosedException)
        {
        }
    }

    private void DrainIngress(int max)
    {
        var drained = 0;
        while (drained < max &&
               _completionIngress.TryDequeue(out var completion))
        {
            Interlocked.Decrement(ref _pendingCompletionIngress);
            RouteEnvelope(in completion);
            drained++;
        }

        var overflow = Volatile.Read(ref _completionOverflow);
        while (drained < max &&
               overflow is not null &&
               overflow.TryDequeue(out var overflowCompletion))
        {
            Interlocked.Decrement(ref _pendingCompletionIngress);
            RouteEnvelope(in overflowCompletion);
            drained++;
        }

        while (drained < max && _ingress.TryDequeue(out var envelope))
        {
            RouteEnvelope(in envelope);
            drained++;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EnqueueCompletionOverflow(
        in ActorEnvelope<TKey, TMessage> envelope)
    {
        var overflow = Volatile.Read(ref _completionOverflow);
        if (overflow is null)
        {
            var created =
                new ConcurrentQueue<ActorEnvelope<TKey, TMessage>>();
            overflow = Interlocked.CompareExchange(
                           ref _completionOverflow,
                           created,
                           comparand: null) ??
                       created;
        }

        overflow.Enqueue(envelope);
    }

    private void RouteEnvelope(in ActorEnvelope<TKey, TMessage> envelope)
    {
        if (envelope.Kind == ActorEnvelopeKind.Completion)
        {
            RouteCompletion(in envelope);
            return;
        }

        ref var cellRef = ref _cells.GetOrAddRef(in envelope.Key);
        if (cellRef is null)
        {
            if (envelope.Kind == ActorEnvelopeKind.Scheduled)
            {
                DropEnvelope(in envelope, ActorMessageDropReason.StaleGeneration);
                _cells.Remove(in envelope.Key);
                TryRetireAdmission(in envelope.Key);
                return;
            }

            var timestamp = _timeProvider.GetTimestamp();
            var cell = ActorCell<TKey, TState, TMessage>.Create(
                in envelope.Key,
                generation: 1,
                timestamp);
            cellRef = cell;

            SetCurrentKey(in envelope.Key);
            var ctx = new ActorContext<TKey, TState, TMessage>(
                this,
                timestamp,
                cell.Generation);
            try
            {
                _behavior.Activate(
                    in envelope.Key,
                    ref cell.State,
                    ref ctx);
            }
            catch
            {
                try
                {
                    _behavior.Deactivate(
                        in envelope.Key,
                        ref cell.State,
                        ActorDeactivateReason.Faulted,
                        ref ctx);
                }
                catch
                {
                }

                _deactivationCount++;
                ClearCurrentKey();
                _cells.Remove(in envelope.Key);
                DropEnvelope(in envelope, ActorMessageDropReason.ActivationFailed);
                TryRetireAdmission(in envelope.Key);
                return;
            }

            ClearCurrentKey();
            _activeActorCount++;
        }

        var actor = cellRef!;
        if (envelope.Kind == ActorEnvelopeKind.Scheduled &&
            actor.Generation != envelope.Generation)
        {
            DropEnvelope(in envelope, ActorMessageDropReason.StaleGeneration);
            return;
        }

        var item = new ActorMailboxItem<TMessage>(
            in envelope.Message,
            envelope.Admission);
        var status = actor.TryEnqueueMessage(
            in item,
            _mailboxMode,
            _mailboxCapacity,
            out var becameReady,
            out var replaced,
            out var hasReplaced);

        if (status is ActorPostStatus.MailboxFull or ActorPostStatus.ActorClosed)
        {
            ReleaseAdmission(item.Admission);
            DropMessage(
                in item.Message,
                status == ActorPostStatus.MailboxFull
                    ? ActorMessageDropReason.MailboxFull
                    : ActorMessageDropReason.ActorClosed);
            if (status == ActorPostStatus.MailboxFull)
                Interlocked.Increment(ref _mailboxFullCount);
            TryRetireAdmission(in actor.Key);
            return;
        }

        if (hasReplaced)
            DropMailboxItem(in replaced, ActorMessageDropReason.Replaced);
        else
            _pendingMailboxCount++;

        if (becameReady && !actor.IsBusy)
            ScheduleReady(actor);

        actor.LastActiveTimestamp = _timeProvider.GetTimestamp();
    }

    private void RouteCompletion(in ActorEnvelope<TKey, TMessage> envelope)
    {
        if (!_cells.TryGetValue(in envelope.Key, out var actor) ||
            actor is null ||
            !actor.IsActive ||
            actor.Generation != envelope.Generation)
        {
            DropMessage(in envelope.Message, ActorMessageDropReason.StaleGeneration);
            return;
        }

        var item = new ActorMailboxItem<TMessage>(
            in envelope.Message,
            admission: null);
        var status = actor.TryEnqueueCompletion(
            in item,
            out _,
            out _);
        if (status != ActorPostStatus.Accepted)
        {
            DropMessage(in envelope.Message, ActorMessageDropReason.MailboxFull);
            Interlocked.Increment(ref _mailboxFullCount);
            return;
        }

        _pendingMailboxCount++;
        ScheduleReady(actor);
        actor.LastActiveTimestamp = _timeProvider.GetTimestamp();
    }

    private void ProcessReadyQueue(CancellationToken cancellationToken)
    {
        var processedActors = 0;
        while (processedActors < _shardBurstLimit && TryTakeReady(out var actor))
        {
            ProcessCell(actor, cancellationToken);
            processedActors++;
        }
    }

    private void ProcessCell(
        ActorCell<TKey, TState, TMessage> actor,
        CancellationToken cancellationToken)
    {
        if (!actor.IsActive)
            return;

        SetCurrentKey(in actor.Key);
        var processedMessages = 0;

        try
        {
            while (processedMessages < _maxMessagesPerActorTurn)
            {
                var completionOnly = actor.IsBusy;
                if (!actor.TryDequeue(
                        completionOnly,
                        out var item,
                        out var wasCompletion))
                {
                    break;
                }

                _pendingMailboxCount--;
                ReleaseAdmission(item.Admission);

                if (cancellationToken.IsCancellationRequested)
                {
                    DropMessage(in item.Message, ActorMessageDropReason.RuntimeStopping);
                    return;
                }

                var timestamp = _timeProvider.GetTimestamp();
                var ctx = new ActorContext<TKey, TState, TMessage>(
                    this,
                    timestamp,
                    actor.Generation);
                ActorTurnResult result;
                try
                {
                    result = _behavior.Receive(
                        in actor.Key,
                        ref actor.State,
                        in item.Message,
                        ref ctx);
                }
                catch
                {
                    DropMessage(in item.Message, ActorMessageDropReason.BehaviorFaulted);
                    DeactivateActor(
                        actor,
                        ActorDeactivateReason.Faulted,
                        ActorMessageDropReason.BehaviorFaulted,
                        timestamp);
                    return;
                }

                _processedCount++;
                processedMessages++;
                actor.LastActiveTimestamp = timestamp;

                if (wasCompletion &&
                    actor.IsBusy &&
                    result != ActorTurnResult.Suspend)
                {
                    actor.ClearBusy();
                    _busyActorCount--;
                }

                switch (result)
                {
                    case ActorTurnResult.Continue:
                        continue;

                    case ActorTurnResult.Suspend:
                        if (!actor.IsBusy)
                        {
                            actor.MarkBusy();
                            _busyActorCount++;
                        }
                        return;

                    case ActorTurnResult.ResumeMailbox:
                        if (actor.IsBusy)
                        {
                            actor.ClearBusy();
                            _busyActorCount--;
                        }
                        continue;

                    case ActorTurnResult.Complete:
                        DeactivateActor(
                            actor,
                            ActorDeactivateReason.Completed,
                            ActorMessageDropReason.ActorCompleted,
                            timestamp);
                        return;
                }
            }

            if (actor.IsActive && !actor.IsBusy && actor.PendingCount > 0)
                ScheduleReady(actor);
        }
        finally
        {
            ClearCurrentKey();
        }
    }

    private void DeactivateActor(
        ActorCell<TKey, TState, TMessage> actor,
        ActorDeactivateReason reason,
        ActorMessageDropReason dropReason,
        long timestamp)
    {
        var ctx = new ActorContext<TKey, TState, TMessage>(
            this,
            timestamp,
            actor.Generation);
        try
        {
            _behavior.Deactivate(
                in actor.Key,
                ref actor.State,
                reason,
                ref ctx);
        }
        catch
        {
        }

        if (actor.IsBusy)
            _busyActorCount--;

        DrainCellMessages(actor, dropReason);
        actor.Deactivate();
        actor.ReleaseStorage();
        _deactivationCount++;
        _activeActorCount--;
        _cells.Remove(in actor.Key);
        TryRetireAdmission(in actor.Key);
    }

    private void DrainCellMessages(
        ActorCell<TKey, TState, TMessage> actor,
        ActorMessageDropReason reason)
    {
        while (actor.TryDequeue(
                   completionOnly: false,
                   out var item,
                   out _))
        {
            _pendingMailboxCount--;
            DropMailboxItem(in item, reason);
        }
    }

    private void SweepIdleActorsIfDue()
    {
        if (_idleTimeoutTimestamp == 0)
            return;

        var now = _timeProvider.GetTimestamp();
        if (now < _nextIdleSweepTimestamp)
            return;

        _nextIdleSweepTimestamp = now + _idleSweepIntervalTimestamp;
        _idleRemovalKeys.Clear();

        foreach (var pair in _cells)
        {
            var actor = pair.Value;
            if (actor is null ||
                !actor.IsActive ||
                actor.IsBusy ||
                actor.PendingCount != 0 ||
                now - actor.LastActiveTimestamp <= _idleTimeoutTimestamp)
            {
                continue;
            }

            _idleRemovalKeys.Add(pair.Key);
        }

        foreach (var key in _idleRemovalKeys)
        {
            if (_cells.TryGetValue(in key, out var actor) &&
                actor is not null &&
                actor.IsActive &&
                !actor.IsBusy &&
                actor.PendingCount == 0)
            {
                SetCurrentKey(in key);
                DeactivateActor(
                    actor,
                    ActorDeactivateReason.IdleTimeout,
                    ActorMessageDropReason.IdleTimeout,
                    now);
                ClearCurrentKey();
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ScheduleReady(ActorCell<TKey, TState, TMessage> actor)
    {
        if (!actor.IsActive || actor.IsScheduled)
            return;

        actor.MarkScheduled();
        actor.ReadyNext = null;
        if (_readyTail is null)
        {
            _readyHead = actor;
            _readyTail = actor;
        }
        else
        {
            _readyTail.ReadyNext = actor;
            _readyTail = actor;
        }

        _readyCount++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryTakeReady(
        out ActorCell<TKey, TState, TMessage> actor)
    {
        var head = _readyHead;
        if (head is null)
        {
            actor = null!;
            return false;
        }

        _readyHead = head.ReadyNext;
        if (_readyHead is null)
            _readyTail = null;

        head.ReadyNext = null;
        head.ClearScheduled();
        _readyCount--;
        actor = head;
        return true;
    }

    private void ClearReadyQueue()
    {
        while (TryTakeReady(out _))
        {
        }
    }

    bool IActorContextSink<TKey, TState, TMessage>.TryPostLocal(
        in TMessage message)
    {
        if (!_hasCurrentKey)
            return false;

        return TryEnqueue(in _currentKey!, in message) ==
               ActorPostStatus.Accepted;
    }

    bool IActorContextSink<TKey, TState, TMessage>.TrySubmitOperation<TWork>(
        in TWork operation)
        => _asyncExecutor.TrySubmit(in operation);

    bool IActorContextSink<TKey, TState, TMessage>.TrySubmitOperation(
        IAsyncOperation operation)
        => _asyncExecutor.TrySubmit(operation);

    bool IActorContextSink<TKey, TState, TMessage>.TrySchedule(
        TimeSpan delay,
        uint generation,
        in TMessage message)
    {
        if (!_hasCurrentKey || delay <= TimeSpan.Zero)
            return false;

        _deadlines.Schedule(
            delay,
            generation,
            in _currentKey!,
            in message);
        return true;
    }

    bool IDeadlineCallback<TKey, TMessage>.TryPostExpired(
        in TKey key,
        uint generation,
        in TMessage message)
    {
        var status = TryEnqueueCore(
            in key,
            in message,
            generation,
            ActorEnvelopeKind.Scheduled);
        if (status == ActorPostStatus.Accepted)
            return true;

        DropMessage(in message, ActorMessageDropReason.DeadlineRejected);
        return false;
    }

    void IDeadlineCallback<TKey, TMessage>.DropScheduled(
        in TMessage message)
        => DropMessage(in message, ActorMessageDropReason.RuntimeStopping);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ReleaseAdmission(ActorAdmission? admission)
        => admission?.Release();

    private void TryRetireAdmission(in TKey key)
    {
        if (_fifoAdmissions is null ||
            !_fifoAdmissions.TryGetValue(key, out var admission) ||
            !admission.TryRetireIfIdle())
        {
            return;
        }

        RemoveAdmissionIfSame(in key, admission);
    }

    private void RemoveAdmissionIfSame(
        in TKey key,
        ActorAdmission admission)
    {
        if (_fifoAdmissions is null)
            return;

        ((ICollection<KeyValuePair<TKey, ActorAdmission>>)_fifoAdmissions)
            .Remove(new KeyValuePair<TKey, ActorAdmission>(key, admission));
    }

    private void DropEnvelope(
        in ActorEnvelope<TKey, TMessage> envelope,
        ActorMessageDropReason reason)
    {
        ReleaseAdmission(envelope.Admission);
        DropMessage(in envelope.Message, reason);
    }

    private void DropMailboxItem(
        in ActorMailboxItem<TMessage> item,
        ActorMessageDropReason reason)
    {
        ReleaseAdmission(item.Admission);
        DropMessage(in item.Message, reason);
    }

    private void DropMessage(
        in TMessage message,
        ActorMessageDropReason reason)
    {
        if (_dropHandler is null)
            return;

        try
        {
            _dropHandler.OnDropped(in message, reason);
        }
        catch
        {
            // 资源释放回调不能破坏 Shard Consumer。
        }
    }

    internal static void SetCurrentKey(in TKey key)
    {
        _currentKey = key;
        _hasCurrentKey = true;
    }

    internal static void ClearCurrentKey()
    {
        _currentKey = default;
        _hasCurrentKey = false;
    }

    private static long ToTimestampUnits(
        TimeSpan value,
        TimeProvider timeProvider)
        => value > TimeSpan.Zero
            ? (long)(value.TotalSeconds * timeProvider.TimestampFrequency)
            : 0;
}
