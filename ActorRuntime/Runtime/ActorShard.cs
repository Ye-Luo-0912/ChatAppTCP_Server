using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Primitives;
using ChatApp.ActorRuntime.Scheduling;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 单写 Shard：MPSC Ingress、ActorCell 表、侵入式 Ready Queue 与 DeadlineWheel。
/// 跨线程只接触 Ingress、Completion Ring、FIFO admission 和原子统计；
/// Actor 状态、Mailbox 与控制通道始终由单线程拥有。
/// <para>
/// 关键不变量：
/// <list type="bullet">
/// <item>激活纪元：每次 Activate 从 Shard 单调计数器分配 ActivationId（不按 Key 重置），
/// Completion / Deadline / Deactivate 均携带并校验（防 ABA）；</item>
/// <item>控制通道：Completion 单槽 + Deadline FIFO 与业务 Mailbox 分离，Busy Actor 仍处理控制消息；</item>
/// <item>单 Outstanding Operation：提交前预留 Completion Credit，Ring 满即内部不变量失败；</item>
/// <item>Idle Sweep 不回收持有未触发 Deadline 或 Outstanding Operation 的 Actor。</item>
/// </list>
/// </para>
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
    private readonly int _maxActorsPerShard;
    private readonly TimeProvider _timeProvider;
    private readonly long _idleTimeoutTimestamp;
    private readonly long _idleSweepIntervalTimestamp;

    private readonly BoundedMpscRing<ActorEnvelope<TKey, TMessage>> _ingress;
    // Completion 走预分配有界 MPSC Ring：容量 == Completion Credit 上限。
    // 提交异步操作时预留 Credit，因此回投时 Ring 必然有槽——满即内部不变量失败。
    private readonly BoundedMpscRing<ActorEnvelope<TKey, TMessage>> _completionIngress;
    private readonly int _completionCreditCapacity;
    private int _completionCredits;
    private readonly ActorCellTable<TKey, TState, TMessage> _cells;
    private readonly ConcurrentDictionary<TKey, ActorAdmission>? _fifoAdmissions;
    private readonly SingleWaiterSignal _signal;
    private readonly ShardDeadlineWheel<TKey, TState, TMessage> _deadlines;
    private readonly AsyncOperationExecutor _asyncExecutor;
    private readonly GlobalActorAdmissionQuota _globalQuota;

    // Shard 单调激活计数器。仅 Consumer 线程访问；1 起始（0 保留为 ActivationId.None）。
    private ulong _nextActivationId = 1;

    // 单线程侵入式 Ready Queue：不分配节点、不设固定容量、不会丢失调度。
    private ActorCell<TKey, TState, TMessage>? _readyHead;
    private ActorCell<TKey, TState, TMessage>? _readyTail;
    private int _readyCount;

    private readonly List<TKey> _idleRemovalKeys = new(64);
    private long _nextIdleSweepTimestamp;

    // 当前 Turn 状态：仅 Consumer 线程访问，Receive/Activate 前重置。
    private bool _turnOperationSubmitted;

    // Consumer-only 计数用普通 long；快照通过 Volatile.Read，避免每消息 Interlocked。
    private long _processedCount;
    private long _activationCount;
    private long _deactivationCount;
    private long _activeActorCount;
    private long _busyActorCount;
    private long _pendingMailboxCount;

    // Producer 也会写入的拒绝计数必须原子更新。
    private long _mailboxFullCount;
    private long _shardOverloadedCount;
    private long _admissionRejectedCount;
    private int _pendingCompletionIngress;

    private CancellationTokenSource? _cts;
    private Task? _loopTask;
    private volatile bool _stopping;

    [ThreadStatic]
    private static ActorCell<TKey, TState, TMessage>? _currentCell;

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
        int maxActorsPerShard,
        int completionCreditCapacity,
        TimeProvider timeProvider,
        AsyncOperationExecutor asyncExecutor,
        GlobalActorAdmissionQuota globalQuota)
    {
        _shardIndex = shardIndex;
        _behavior = behavior;
        _dropHandler = dropHandler;
        _mailboxMode = mailboxMode;
        _mailboxCapacity = mailboxCapacity;
        _shardBurstLimit = shardBurstLimit;
        _maxMessagesPerActorTurn = maxMessagesPerActorTurn;
        _maxIngressDrain = Math.Max(64, shardBurstLimit * maxMessagesPerActorTurn);
        _maxActorsPerShard = maxActorsPerShard;
        _completionCreditCapacity = completionCreditCapacity;
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
                completionCreditCapacity);
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
        _globalQuota = globalQuota;
    }

    public int ShardIndex => _shardIndex;
    public long ProcessedCount => Volatile.Read(ref _processedCount);
    public long MailboxFullCount => Volatile.Read(ref _mailboxFullCount);
    public long ShardOverloadedCount => Volatile.Read(ref _shardOverloadedCount);
    public long DeactivationCount => Volatile.Read(ref _deactivationCount);
    public long ActivationCount => Volatile.Read(ref _activationCount);
    public long AdmissionRejectedCount => Volatile.Read(ref _admissionRejectedCount);
    public long PendingIngress =>
        _ingress.Count + Volatile.Read(ref _pendingCompletionIngress);
    public long PendingMailbox => Volatile.Read(ref _pendingMailboxCount);
    public long ActiveActorCount => Volatile.Read(ref _activeActorCount);
    public long BusyActorCount => Volatile.Read(ref _busyActorCount);
    public int PendingDeadlines => _deadlines.PendingCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueue(in TKey key, in TMessage message)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        ActorAdmission? admission = null;
        if (_fifoAdmissions is not null)
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
            ActivationId.None,
            ActorEnvelopeKind.Message);
        if (!_ingress.TryEnqueue(in envelope))
        {
            admission?.Release();
            Interlocked.Increment(ref _shardOverloadedCount);
            return ActorPostStatus.ShardOverloaded;
        }

        _signal.Signal();
        return ActorPostStatus.Accepted;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueueCompletion(
        in TKey key,
        ActivationId activation,
        in TMessage message)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            in message,
            admission: null,
            activation,
            ActorEnvelopeKind.Completion);
        Interlocked.Increment(ref _pendingCompletionIngress);
        if (!_completionIngress.TryEnqueue(in envelope))
        {
            // 提交时已预留 Completion Credit，Ring 必然有槽；走到这里是不变量被破坏。
            // 释放 Credit 避免泄漏，绝不扩展无界内存。
            Interlocked.Decrement(ref _pendingCompletionIngress);
            ReleaseCompletionCredit();
            Debug.Fail("Completion ring full despite reserved credit.");
            return ActorPostStatus.MailboxFull;
        }

        _signal.Signal();
        return ActorPostStatus.Accepted;
    }

    /// <summary>
    /// 显式 Deactivate 请求。不占 Mailbox 准入容量，经普通 Ingress 与业务消息保序。
    /// </summary>
    public bool TryEnqueueDeactivate(
        in TKey key,
        ActivationId activation,
        ActorDeactivateReason reason)
    {
        if (_stopping)
            return false;

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            message: default,
            admission: null,
            activation,
            ActorEnvelopeKind.Deactivate,
            reason);
        if (!_ingress.TryEnqueue(in envelope))
        {
            Interlocked.Increment(ref _shardOverloadedCount);
            return false;
        }

        _signal.Signal();
        return true;
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
        {
            // Deactivate 信封不携带业务消息，直接跳过（无资源可释放）。
            if (envelope.Kind != ActorEnvelopeKind.Deactivate)
                DropEnvelope(in envelope, ActorMessageDropReason.RuntimeStopping);
        }

        while (_completionIngress.TryDequeue(out var completion))
        {
            Interlocked.Decrement(ref _pendingCompletionIngress);
            ReleaseCompletionCredit();
            DropEnvelope(in completion, ActorMessageDropReason.RuntimeStopping);
        }

        _deadlines.Stop();

        if (_cells.Count > 0)
        {
            var timestamp = _timeProvider.GetTimestamp();
            foreach (var cell in _cells.Values)
            {
                if (cell is null || !cell.IsActive)
                    continue;

                SetCurrentCell(cell);
                var ctx = new ActorContext<TKey, TState, TMessage>(
                    this,
                    timestamp,
                    cell.Activation);
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
                _globalQuota.Release();
            }

            ClearCurrentCell();
            _cells.Clear();
        }

        ClearReadyQueue();
        _fifoAdmissions?.Clear();
        Volatile.Write(ref _activeActorCount, 0);
        Volatile.Write(ref _busyActorCount, 0);
        Volatile.Write(ref _pendingMailboxCount, 0);
        Volatile.Write(ref _completionCredits, 0);

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
            ReleaseCompletionCredit();
            RouteCompletion(in completion);
            drained++;
        }

        while (drained < max && _ingress.TryDequeue(out var envelope))
        {
            if (envelope.Kind == ActorEnvelopeKind.Deactivate)
                RouteDeactivate(in envelope);
            else
                RouteEnvelope(in envelope);
            drained++;
        }
    }

    private void RouteEnvelope(in ActorEnvelope<TKey, TMessage> envelope)
    {
        ref var cellRef = ref _cells.GetOrAddRef(in envelope.Key);
        if (cellRef is null)
        {
            // 新 Actor 准入：每 Shard 上限 + 全局配额双层检查。
            if (_cells.Count >= _maxActorsPerShard ||
                !_globalQuota.TryAcquire())
            {
                _cells.Remove(in envelope.Key);
                Interlocked.Increment(ref _admissionRejectedCount);
                DropEnvelope(in envelope, ActorMessageDropReason.AdmissionRejected);
                TryRetireAdmission(in envelope.Key);
                return;
            }

            var timestamp = _timeProvider.GetTimestamp();
            var cell = ActorCell<TKey, TState, TMessage>.Create(
                in envelope.Key,
                new ActivationId(_nextActivationId++),
                timestamp);
            cellRef = cell;

            SetCurrentCell(cell);
            _turnOperationSubmitted = false;
            var ctx = new ActorContext<TKey, TState, TMessage>(
                this,
                timestamp,
                cell.Activation);
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
                _globalQuota.Release();
                ClearCurrentCell();
                _cells.Remove(in envelope.Key);
                DropEnvelope(in envelope, ActorMessageDropReason.ActivationFailed);
                TryRetireAdmission(in envelope.Key);
                return;
            }

            ClearCurrentCell();
            _activeActorCount++;
            _activationCount++;
        }

        var actor = cellRef!;
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
            actor.Activation != envelope.Activation ||
            !actor.HasOutstandingOperation)
        {
            // 过期 / 重复回投：Activation 不匹配或没有 Outstanding Operation。
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
            // 单 Outstanding 约束保证 Completion 槽必然为空。
            Debug.Fail("Completion slot occupied despite single outstanding operation.");
            DropMessage(in envelope.Message, ActorMessageDropReason.MailboxFull);
            Interlocked.Increment(ref _mailboxFullCount);
            return;
        }

        _pendingMailboxCount++;
        ScheduleReady(actor);
        actor.LastActiveTimestamp = _timeProvider.GetTimestamp();
    }

    private void RouteDeactivate(in ActorEnvelope<TKey, TMessage> envelope)
    {
        if (!_cells.TryGetValue(in envelope.Key, out var actor) ||
            actor is null ||
            !actor.IsActive)
        {
            return;
        }

        if (envelope.Activation.IsValid &&
            actor.Activation != envelope.Activation)
        {
            // 显式指定了激活纪元但已不匹配：过期请求，直接忽略。
            return;
        }

        DeactivateActor(
            actor,
            envelope.DeactivateReason,
            ActorMessageDropReason.ExplicitlyDeactivated,
            _timeProvider.GetTimestamp());
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

        SetCurrentCell(actor);
        var processedMessages = 0;

        try
        {
            while (processedMessages < _maxMessagesPerActorTurn)
            {
                // Busy Actor 暂停业务 Mailbox，但继续处理控制通道（Completion/Deadline）。
                var controlOnly = actor.IsBusy;
                if (!actor.TryDequeue(
                        controlOnly,
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
                _turnOperationSubmitted = false;
                var ctx = new ActorContext<TKey, TState, TMessage>(
                    this,
                    timestamp,
                    actor.Activation);
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

                if (_turnOperationSubmitted && result != ActorTurnResult.Suspend)
                {
                    // 契约违反：提交了 Operation 却未 Suspend——Completion 将无人等待。
                    Debug.Fail("Operation submitted but turn did not suspend.");
                    DropMessage(in item.Message, ActorMessageDropReason.BehaviorFaulted);
                    DeactivateActor(
                        actor,
                        ActorDeactivateReason.Faulted,
                        ActorMessageDropReason.BehaviorFaulted,
                        timestamp);
                    return;
                }

                if (result == ActorTurnResult.Suspend &&
                    !actor.HasOutstandingOperation &&
                    actor.PendingDeadlineCount == 0)
                {
                    // 契约违反：Suspend 但无 Outstanding Operation 且无未触发 Deadline——
                    // Actor 将永远无法被唤醒。
                    Debug.Fail("Suspend without outstanding operation or pending deadline.");
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

            // 还有剩余：非 Busy 继续业务 Mailbox；Busy 但控制通道非空也继续。
            if (actor.IsActive &&
                actor.PendingCount > 0 &&
                (!actor.IsBusy || actor.HasPendingControl))
            {
                ScheduleReady(actor);
            }
        }
        finally
        {
            ClearCurrentCell();
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
            actor.Activation);
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
        _globalQuota.Release();
        _cells.Remove(in actor.Key);
        TryRetireAdmission(in actor.Key);
        // 时间轮中该 Actor 的未触发条目不显式移除：
        // 触发时 Activation 匹配失败被识别为过期丢弃（惰性取消）。
    }

    private void DrainCellMessages(
        ActorCell<TKey, TState, TMessage> actor,
        ActorMessageDropReason reason)
    {
        while (actor.TryDequeue(
                   controlOnly: false,
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
                actor.PendingDeadlineCount != 0 ||
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
                actor.PendingCount == 0 &&
                actor.PendingDeadlineCount == 0)
            {
                SetCurrentCell(actor);
                DeactivateActor(
                    actor,
                    ActorDeactivateReason.IdleTimeout,
                    ActorMessageDropReason.IdleTimeout,
                    now);
                ClearCurrentCell();
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryReserveCompletionCredit()
    {
        var current = Volatile.Read(ref _completionCredits);
        while (current < _completionCreditCapacity)
        {
            var observed = Interlocked.CompareExchange(
                ref _completionCredits,
                current + 1,
                current);
            if (observed == current)
                return true;

            current = observed;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReleaseCompletionCredit()
    {
        var remaining = Interlocked.Decrement(ref _completionCredits);
        if (remaining < 0)
        {
            Interlocked.Increment(ref _completionCredits);
            Debug.Fail("Completion credit released more than reserved.");
        }
    }

    bool IActorContextSink<TKey, TState, TMessage>.TryPostLocal(
        in TMessage message)
    {
        var cell = _currentCell;
        if (cell is null)
            return false;

        return TryEnqueue(in cell.Key, in message) ==
               ActorPostStatus.Accepted;
    }

    bool IActorContextSink<TKey, TState, TMessage>.TrySubmitOperation<TWork>(
        in TWork operation)
    {
        var cell = _currentCell;
        if (cell is null ||
            cell.HasOutstandingOperation ||
            _turnOperationSubmitted)
        {
            return false;
        }

        if (!TryReserveCompletionCredit())
            return false;

        if (!_asyncExecutor.TrySubmit(in operation))
        {
            ReleaseCompletionCredit();
            return false;
        }

        cell.HasOutstandingOperation = true;
        _turnOperationSubmitted = true;
        return true;
    }

    bool IActorContextSink<TKey, TState, TMessage>.TrySubmitOperation(
        IAsyncOperation operation)
    {
        var cell = _currentCell;
        if (cell is null ||
            cell.HasOutstandingOperation ||
            _turnOperationSubmitted)
        {
            return false;
        }

        if (!TryReserveCompletionCredit())
            return false;

        if (!_asyncExecutor.TrySubmit(operation))
        {
            ReleaseCompletionCredit();
            return false;
        }

        cell.HasOutstandingOperation = true;
        _turnOperationSubmitted = true;
        return true;
    }

    bool IActorContextSink<TKey, TState, TMessage>.TryReserveOutstandingOperation()
    {
        var cell = _currentCell;
        if (cell is null ||
            cell.HasOutstandingOperation ||
            _turnOperationSubmitted)
        {
            return false;
        }

        if (!TryReserveCompletionCredit())
            return false;

        // 领域 Lane 的 TrySubmit 已由 Behavior 调用方完成（成功），
        // 此处仅标记 Outstanding 状态，不提交到通用 Executor。
        cell.HasOutstandingOperation = true;
        _turnOperationSubmitted = true;
        return true;
    }

    void IActorContextSink<TKey, TState, TMessage>.ReleaseOutstandingOperation()
    {
        var cell = _currentCell;
        if (cell is null)
            return;

        // 回滚 TryReserveOutstandingOperation 的标记
        cell.HasOutstandingOperation = false;
        _turnOperationSubmitted = false;
        ReleaseCompletionCredit();
    }

    bool IActorContextSink<TKey, TState, TMessage>.TrySchedule(
        TimeSpan delay,
        bool replaceExisting,
        in TMessage message)
    {
        var cell = _currentCell;
        if (cell is null || !cell.IsActive || delay <= TimeSpan.Zero)
            return false;

        if (replaceExisting)
        {
            // 惰性取消：bump 代际使时间轮中未触发条目在触发时被视为过期。
            cell.DeadlineEpoch++;
            cell.PendingDeadlineCount = 0;
        }

        // 不变量：未触发 + 已触发未消费 ≤ MaxControlDeadlines，
        // 保证触发时控制 FIFO 必有槽。
        if (cell.PendingDeadlineCount + cell.PendingDeadlineControlCount >=
            ActorCell<TKey, TState, TMessage>.MaxControlDeadlines)
        {
            return false;
        }

        _deadlines.Schedule(
            delay,
            cell.Activation,
            cell.DeadlineEpoch,
            in cell.Key,
            in message);
        cell.PendingDeadlineCount++;
        return true;
    }

    void IActorContextSink<TKey, TState, TMessage>.CancelDeadlines()
    {
        var cell = _currentCell;
        if (cell is null)
            return;

        cell.DeadlineEpoch++;
        cell.PendingDeadlineCount = 0;
    }

    void IDeadlineCallback<TKey, TMessage>.OnExpired(
        in TKey key,
        ActivationId activation,
        uint deadlineEpoch,
        in TMessage message)
    {
        // 在 Shard Consumer 线程上执行：直接投递控制通道，不经过 Ingress。
        if (!_cells.TryGetValue(in key, out var actor) ||
            actor is null ||
            !actor.IsActive ||
            actor.Activation != activation ||
            actor.DeadlineEpoch != deadlineEpoch)
        {
            // Actor 已重建（Activation 变化）或 Deadline 已被替换/取消（Epoch 变化）。
            DropMessage(in message, ActorMessageDropReason.StaleGeneration);
            return;
        }

        actor.PendingDeadlineCount--;
        var item = new ActorMailboxItem<TMessage>(
            in message,
            admission: null);
        if (!actor.TryEnqueueDeadline(in item))
        {
            // 调度侧不变量保证控制 FIFO 必有槽。
            Debug.Fail("Deadline control queue full despite scheduling bound.");
            DropMessage(in message, ActorMessageDropReason.DeadlineRejected);
            return;
        }

        _pendingMailboxCount++;
        ScheduleReady(actor);
        actor.LastActiveTimestamp = _timeProvider.GetTimestamp();
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

    internal static void SetCurrentCell(
        ActorCell<TKey, TState, TMessage> cell)
        => _currentCell = cell;

    internal static void ClearCurrentCell()
        => _currentCell = null;

    private static long ToTimestampUnits(
        TimeSpan value,
        TimeProvider timeProvider)
        => value > TimeSpan.Zero
            ? (long)(value.TotalSeconds * timeProvider.TimestampFrequency)
            : 0;
}
