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
    // P0-2：每个 Actor Key 的线程安全路由对象。承担生产侧准入决策（邮件配额 + 激活配额 + 状态机），
    // 消除"探测 Actor 存在 → 消费时已回收"的 TOCTOU 竞态。替代旧的 _activeActorKeys + _fifoAdmissions。
    private readonly ConcurrentDictionary<TKey, ActorRoute> _routes = new();
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
    private long _replacedCount;
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
    public long ReplacedCount => Volatile.Read(ref _replacedCount);
    public long PendingIngress =>
        _ingress.Count + Volatile.Read(ref _pendingCompletionIngress);
    public long PendingMailbox => Volatile.Read(ref _pendingMailboxCount);
    public long ActiveActorCount => Volatile.Read(ref _activeActorCount);
    public long BusyActorCount => Volatile.Read(ref _busyActorCount);
    public int PendingDeadlines => _deadlines.PendingCount;
    public int MaxActorsPerShard => _maxActorsPerShard;

    /// <summary>
    /// 获取（或创建）指定 Key 的路由对象。生产侧准入决策的唯一入口。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ActorRoute GetOrCreateRoute(in TKey key)
        => _routes.GetOrAdd(key, static _ => new ActorRoute());

    /// <summary>
    /// FIFO 模式下的邮件配额预留。成功返回 true（route 已计入在途消息）。
    /// 失败且 route 已退休时返回 false，调用方应重取路由。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryReserveMailboxCapacity(in TKey key, ActorRoute route)
    {
        if (_mailboxMode != ActorMailboxMode.Fifo)
            return true;

        while (true)
        {
            if (route.TryReserveMailbox(_mailboxCapacity))
                return true;

            if (!route.IsRetired)
            {
                Interlocked.Increment(ref _mailboxFullCount);
                return false;
            }

            RemoveRouteIfSame(in key, route);
            route = GetOrCreateRoute(in key);
        }
    }

    /// <summary>
    /// 释放一条消息的邮件配额（仅 FIFO 模式；Latest 模式在 <see cref="TryReserveMailboxCapacity"/>
    /// 中不预留配额，故此处为 no-op，避免 _pending 越界为负触发 "released more than once"）。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReleaseMailboxCapacity(ActorRoute? route)
    {
        if (_mailboxMode != ActorMailboxMode.Fifo)
            return;

        route?.ReleaseMailbox();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueue(in TKey key, in TMessage message)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        var route = GetOrCreateRoute(in key);
        if (!TryReserveMailboxCapacity(in key, route))
            return ActorPostStatus.MailboxFull;

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            in message,
            route,
            ActivationId.None,
            ActorEnvelopeKind.Message);
        if (!_ingress.TryEnqueue(in envelope))
        {
            ReleaseMailboxCapacity(route);
            Interlocked.Increment(ref _shardOverloadedCount);
            return ActorPostStatus.ShardOverloaded;
        }

        _signal.Signal();
        return ActorPostStatus.Accepted;
    }

    /// <summary>
    /// P0-2：Durable 消息入队。在 Route 状态机上原子地预留激活配额 + 邮件配额，
    /// 消除"探测 Actor 存在 → 消费时已回收"的 TOCTOU 竞态：
    /// <list type="bullet">
    /// <item>Inactive → Activating：预留全局激活配额 + 每 Shard 预检；</item>
    /// <item>Activating / Active：激活已在进行或已存在，仅预留邮件配额；</item>
    /// <item>Retiring：接管（Retiring → Activating），转移配额，建立下一代激活。</item>
    /// </list>
    /// 返回 Accepted 时保证激活配额 + 邮件配额均已预留；入队失败时释放。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActorPostStatus TryEnqueueDurable(
        in TKey key,
        in TMessage message)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        var route = GetOrCreateRoute(in key);
        if (!TryReserveMailboxCapacity(in key, route))
            return ActorPostStatus.MailboxFull;

        // 在 Route 状态机上原子地预留激活配额。
        if (!route.TryBeginActivation(_globalQuota, out var quotaReserved))
        {
            ReleaseMailboxCapacity(route);
            return ActorPostStatus.AdmissionRejected;
        }

        // 新激活槽（quotaReserved=true）需预检每 Shard 上限，避免入队后消费侧静默丢弃。
        // 预检是"最佳努力"——消费侧 _cells.Count 仍是权威检查。
        if (quotaReserved && Volatile.Read(ref _activeActorCount) >= _maxActorsPerShard)
        {
            ReleaseMailboxCapacity(route);
            if (route.TryRollbackActivation())
                _globalQuota.Release();
            return ActorPostStatus.AdmissionRejected;
        }

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            in message,
            route,
            ActivationId.None,
            ActorEnvelopeKind.Message);
        if (!_ingress.TryEnqueue(in envelope))
        {
            ReleaseMailboxCapacity(route);
            if (quotaReserved && route.TryRollbackActivation())
                _globalQuota.Release();
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
            route: null,
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
            route: null,
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

    /// <summary>
    /// Invalidation 控制消息入队。经普通 Ingress Ring 路由（与 Deactivate 相同通道），
    /// 由 Consumer 投递到 ActorCell 的 Invalidation 控制槽。不占 Mailbox 准入容量。
    /// </summary>
    public ActorPostStatus TryEnqueueInvalidation(in TKey key, in TMessage message)
    {
        if (_stopping)
            return ActorPostStatus.RuntimeStopping;

        var envelope = new ActorEnvelope<TKey, TMessage>(
            in key,
            in message,
            route: null,
            ActivationId.None,
            ActorEnvelopeKind.Invalidation);
        if (!_ingress.TryEnqueue(in envelope))
        {
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
        {
            // Deactivate / Invalidation 信封不携带业务消息，直接跳过（无资源可释放）。
            if (envelope.Kind is not (ActorEnvelopeKind.Deactivate
                or ActorEnvelopeKind.Invalidation))
            {
                DropEnvelope(in envelope, ActorMessageDropReason.RuntimeStopping);
            }
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
            _routes.Clear();
        }

        ClearReadyQueue();
        _routes.Clear();
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
            switch (envelope.Kind)
            {
                case ActorEnvelopeKind.Deactivate:
                    RouteDeactivate(in envelope);
                    break;
                case ActorEnvelopeKind.Invalidation:
                    RouteInvalidation(in envelope);
                    break;
                default:
                    RouteEnvelope(in envelope);
                    break;
            }
            drained++;
        }
    }

    private void RouteEnvelope(in ActorEnvelope<TKey, TMessage> envelope)
    {
        var route = envelope.Route;
        ref var cellRef = ref _cells.GetOrAddRef(in envelope.Key);

        if (cellRef is null)
        {
            // 新 Actor 准入。Route 状态机是权威来源：
            // 已持有配额（非 Inactive）→ 复用预留并 Commit；未持有 → TryAcquire 安全网。
            var hasReservation = route?.HasReservation ?? false;

            if (_cells.Count > _maxActorsPerShard)
            {
                _cells.Remove(in envelope.Key);
                Interlocked.Increment(ref _admissionRejectedCount);
                DropEnvelope(in envelope, ActorMessageDropReason.AdmissionRejected);
                if (route is not null && hasReservation && route.TryRollbackActivation())
                    _globalQuota.Release();
                TryRetireRoute(in envelope.Key);
                return;
            }

            if (!hasReservation && !_globalQuota.TryAcquire())
            {
                _cells.Remove(in envelope.Key);
                Interlocked.Increment(ref _admissionRejectedCount);
                DropEnvelope(in envelope, ActorMessageDropReason.AdmissionRejected);
                TryRetireRoute(in envelope.Key);
                return;
            }

            var timestamp = _timeProvider.GetTimestamp();
            var cell = ActorCell<TKey, TState, TMessage>.Create(
                in envelope.Key,
                new ActivationId(_nextActivationId++),
                timestamp,
                _mailboxMode);
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
                // 激活失败：释放本路径持有的配额。若已预留（非 Inactive），
                // 仅在无其他在途消息时回滚并释放；否则保留以供后续消息重新激活。
                if (!hasReservation)
                    _globalQuota.Release();
                else if (route is not null && route.TryRollbackActivation())
                    _globalQuota.Release();
                ClearCurrentCell();
                _cells.Remove(in envelope.Key);
                DropEnvelope(in envelope, ActorMessageDropReason.BehaviorFaulted);
                TryRetireRoute(in envelope.Key);
                return;
            }

            ClearCurrentCell();
            _activeActorCount++;
            _activationCount++;
            // P0-2：提交 Route 为 Active。配额由活跃 Actor 持有，直到退休时释放。
            route?.CommitActive();
        }

        var actor = cellRef!;
        var item = new ActorMailboxItem<TMessage>(
            in envelope.Message,
            envelope.Route);
        var status = actor.TryEnqueueMessage(
            in item,
            _mailboxCapacity,
            out var becameReady,
            out var replaced,
            out var hasReplaced);

        if (status is ActorPostStatus.MailboxFull or ActorPostStatus.ActorClosed)
        {
            ReleaseMailboxCapacity(item.Route);
            DropMessage(
                in item.Message,
                status == ActorPostStatus.MailboxFull
                    ? ActorMessageDropReason.MailboxFull
                    : ActorMessageDropReason.ActorClosed);
            if (status == ActorPostStatus.MailboxFull)
                Interlocked.Increment(ref _mailboxFullCount);
            TryRetireRoute(in actor.Key);
            return;
        }

        if (hasReplaced)
        {
            Interlocked.Increment(ref _replacedCount);
            DropMailboxItem(in replaced, ActorMessageDropReason.Replaced);
        }
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
            route: null);
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

    /// <summary>
    /// 路由 Invalidation 控制消息到 ActorCell 的 Invalidation 控制槽。
    /// Actor 不存在时静默丢弃（Ephemeral 语义：TTL 兜底）。
    /// 不校验 Activation：Invalidation 是 Key 级别的（非 Activation 级别），
    /// 即使 Actor 已重建也应应用失效（清空新 Actor 的授权缓存）。
    /// </summary>
    private void RouteInvalidation(in ActorEnvelope<TKey, TMessage> envelope)
    {
        if (!_cells.TryGetValue(in envelope.Key, out var actor) ||
            actor is null ||
            !actor.IsActive)
        {
            // Actor 不存在：静默丢弃。关系变更后新 Notify 会重新触发授权 I/O。
            return;
        }

        var item = new ActorMailboxItem<TMessage>(
            in envelope.Message,
            route: null);
        if (actor.TryEnqueueInvalidation(in item))
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
                ReleaseMailboxCapacity(item.Route);

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
        ReleaseRouteQuota(in actor.Key);
        _cells.Remove(in actor.Key);
        TryRetireRoute(in actor.Key);
        // 时间轮中该 Actor 的未触发条目不显式移除：
        // 触发时 Activation 匹配失败被识别为过期丢弃（惰性取消）。
    }

    /// <summary>
    /// 退休时协调 ActorRoute 的配额释放。Active → Retiring → Inactive（无在途消息）时释放全局配额；
    /// 存在在途消息（<see cref="ActorRoute.Pending"/> &gt; 0）或已被生产侧接管（Retiring → Activating）时，
    /// 保留配额供后续消息重新激活。Route 非 Active（如已接管）但不持有配额时也释放。
    /// </summary>
    private void ReleaseRouteQuota(in TKey key)
    {
        if (!_routes.TryGetValue(key, out var route))
            return;

        if (route.TryBeginRetirement())
        {
            // 本次退休由本路径完成：无在途消息才真正释放配额。
            if (route.TryCompleteRetirement())
                _globalQuota.Release();
        }
        else if (!route.HasReservation)
        {
            // Route 非 Active 且未持有配额（已接管时 HasReservation=true，配额由新一代持有）。
            _globalQuota.Release();
        }
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
    internal bool TryReserveCompletionCredit()
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
            // 显式取消旧条目：在桶中标记为 Cancelled，避免高频 ScheduleOrReplace
            // 导致物理桶积累过期条目（仅靠 Epoch bump 会让旧条目留到触发时才丢弃）。
            if (cell.LastDeadlineBucketIndex >= 0)
            {
                _deadlines.TryCancelScheduled(
                    cell.LastDeadlineBucketIndex,
                    cell.Activation,
                    cell.DeadlineEpoch);
            }

            // bump 代际作为双重防线：即使显式取消遗漏（如桶已开始排空），
            // 触发时仍会因 Epoch 不匹配被丢弃。
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

        var bucketIndex = _deadlines.Schedule(
            delay,
            cell.Activation,
            cell.DeadlineEpoch,
            in cell.Key,
            in message);
        cell.LastDeadlineBucketIndex = bucketIndex;
        cell.PendingDeadlineCount++;
        return true;
    }

    void IActorContextSink<TKey, TState, TMessage>.CancelDeadlines()
    {
        var cell = _currentCell;
        if (cell is null)
            return;

        // 显式取消最近的 Deadline 条目（如果有多个未触发条目，
        // 其余仍由 Epoch bump 在触发时惰性丢弃）。
        if (cell.LastDeadlineBucketIndex >= 0)
        {
            _deadlines.TryCancelScheduled(
                cell.LastDeadlineBucketIndex,
                cell.Activation,
                cell.DeadlineEpoch);
        }

        cell.DeadlineEpoch++;
        cell.PendingDeadlineCount = 0;
        cell.LastDeadlineBucketIndex = -1;
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
        if (actor.PendingDeadlineCount == 0)
            actor.LastDeadlineBucketIndex = -1;
        var item = new ActorMailboxItem<TMessage>(
            in message,
            route: null);
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
    private void TryRetireRoute(in TKey key)
    {
        if (!_routes.TryGetValue(key, out var route))
            return;

        if (!route.TryRetireIfIdle())
            return;

        RemoveRouteIfSame(in key, route);
    }

    private void RemoveRouteIfSame(
        in TKey key,
        ActorRoute route)
    {
        ((ICollection<KeyValuePair<TKey, ActorRoute>>)_routes)
            .Remove(new KeyValuePair<TKey, ActorRoute>(key, route));
    }

    private void DropEnvelope(
        in ActorEnvelope<TKey, TMessage> envelope,
        ActorMessageDropReason reason)
    {
        ReleaseMailboxCapacity(envelope.Route);
        DropMessage(in envelope.Message, reason);
    }

    private void DropMailboxItem(
        in ActorMailboxItem<TMessage> item,
        ActorMessageDropReason reason)
    {
        ReleaseMailboxCapacity(item.Route);
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
