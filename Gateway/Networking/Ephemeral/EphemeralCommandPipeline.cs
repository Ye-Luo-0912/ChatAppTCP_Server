using System.Collections.Concurrent;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Runtime;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// Ephemeral 入站命令调度边界。可在轻量 ActorRuntime 与旧
/// SessionCommandExecutor 间切换，不改变 SessionRuntime 或 wire 协议。
/// <para>
/// 三种模式：
/// <list type="bullet">
/// <item><see cref="EphemeralPipelineMode.Disabled"/>：不创建任何调度资源。
///   用于 Specialized Typing 模式：TypingNotify 被快路径截获，通用调度完全冗余。
///   Register/Unregister 为 no-op；TryEnqueue 返回 false；Start/Stop 为 no-op。</item>
/// <item><see cref="EphemeralPipelineMode.Legacy"/>：使用 SessionCommandExecutor（Worker 池 + 每连接 ConcurrentQueue）。</item>
/// <item><see cref="EphemeralPipelineMode.GenericActor"/>：使用 ActorRuntime（FIFO Mailbox + 异步操作执行器）。</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class EphemeralCommandPipeline : IAsyncDisposable
{
    private const int ActorAdmissionLockCount = 256;

    private readonly EphemeralPipelineMode _mode;
    private readonly SessionCommandExecutor? _legacy;
    private readonly ActorRuntime<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage>? _actor;
    private readonly ConcurrentDictionary<uint, long>? _actorRegistrations;
    private readonly SpinLock[]? _actorAdmissionLocks;
    private readonly object? _actorRegistrationGate;
    private readonly TimeSpan _operationTimeout;
    private long _nextActorRegistration;
    private bool _acceptingActorRegistrations;
    private bool _disposed;

    public EphemeralCommandPipeline(
        TcpGatewayOptions options,
        Func<SessionCommand, CancellationToken, ValueTask> processor,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger)
        : this(options, ephemeralMode: null, processor, metrics, timeProvider, logger)
    {
    }

    /// <summary>
    /// 构造指定模式的 Ephemeral 调度管道。
    /// <paramref name="ephemeralMode"/> 为 null 时从 <paramref name="options"/> 推导。
    /// </summary>
    public EphemeralCommandPipeline(
        TcpGatewayOptions options,
        EphemeralPipelineMode? ephemeralMode,
        Func<SessionCommand, CancellationToken, ValueTask> processor,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(processor);
        _operationTimeout = options.EphemeralActorOperationTimeout;

        // 显式模式优先：调用方可通过 ephemeralMode 覆盖布尔标志推导结果。
        // 未指定时由 options.ResolveEphemeralPipelineMode() 推导。
        _mode = ephemeralMode ?? options.ResolveEphemeralPipelineMode();

        if (_mode == EphemeralPipelineMode.Disabled)
        {
            // Disabled：不创建任何 Worker / Actor / ConnectionQueue 资源。
            // 用于 Specialized Typing 模式——TypingNotify 已被快路径截获。
            return;
        }

        if (_mode == EphemeralPipelineMode.Legacy)
        {
            _legacy = new SessionCommandExecutor(
                processor,
                workerCount: Math.Max(2, Environment.ProcessorCount),
                burstLimit: 4,
                perConnectionCapacity: options.CommandSchedulerEphemeralCapacity,
                globalCapacity: Math.Max(
                    1024,
                    options.CommandSchedulerEphemeralCapacity * 256),
                commandTimeout: TimeSpan.Zero,
                perUserConcurrency: 0,
                onFatalError: ex => LogEphemeralCommandFatal(logger, ex),
                logger);
            return;
        }

        var behavior = new EphemeralActorBehavior(
            processor,
            metrics,
            logger);
        var dropHandler = new EphemeralActorDropHandler(metrics);
        _actor = new ActorRuntime<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage>(
            behavior,
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = options.EphemeralActorShardCount > 0
                    ? options.EphemeralActorShardCount
                    : NextPowerOfTwo(Math.Max(2, Environment.ProcessorCount)),
                ShardIngressCapacity = options.EphemeralActorIngressCapacity,
                DefaultMailboxCapacity = options.CommandSchedulerEphemeralCapacity,
                ShardBurstLimit = 64,
                MaxMessagesPerActorTurn = 4,
                ShardTickInterval = TimeSpan.FromMilliseconds(25),
                AsyncOperationConcurrency =
                    options.EphemeralActorAsyncConcurrency > 0
                        ? options.EphemeralActorAsyncConcurrency
                        : Math.Max(2, Environment.ProcessorCount * 2),
                AsyncOperationQueueCapacity =
                    Math.Max(1024, options.EphemeralActorIngressCapacity),
                AsyncOperationTimeout = options.EphemeralActorOperationTimeout,
                ActorIdleTimeout = options.EphemeralActorIdleTimeout
            },
            timeProvider,
            dropHandler);
        _actorRegistrations = new ConcurrentDictionary<uint, long>();
        _actorRegistrationGate = new object();
        _acceptingActorRegistrations = true;
        // 共享条带锁把 generation 校验 + Actor post 与注销线性化。
        // 固定 256 个值类型锁（约 1 KiB/进程 pipeline），避免每连接 gate 对象。
        _actorAdmissionLocks = new SpinLock[ActorAdmissionLockCount];
        behavior.Attach(_actor);
    }

    /// <summary>
    /// 当前调度模式。Disabled 下所有资源相关方法为 no-op。
    /// </summary>
    public EphemeralPipelineMode Mode => _mode;

    public bool UsesActorRuntime => _actor is not null;

    public ActorRuntimeSnapshot Snapshot =>
        _actor?.GetSnapshot() ?? default;

    /// <summary>
    /// 注册连接。Disabled 模式下为 no-op（返回 true 维持调用方契约）。
    /// Legacy 模式下委托 SessionCommandExecutor；GenericActor 模式下取得独立
    /// generation lease，使 connectionId 复用时旧 session 不能投递或回收后继 Actor。
    /// </summary>
    public bool TryRegisterConnection(
        uint connectionId,
        long userId,
        out Registration registration)
    {
        if (_mode == EphemeralPipelineMode.Disabled)
        {
            registration = Registration.CreateNoop(this);
            return true;
        }

        if (_legacy is not null)
        {
            if (!_legacy.TryRegisterConnection(
                    connectionId,
                    userId,
                    out var legacyRegistration))
            {
                registration = default;
                return false;
            }

            registration = Registration.CreateLegacy(
                this,
                legacyRegistration);
            return true;
        }

        lock (_actorRegistrationGate!)
        {
            if (!_acceptingActorRegistrations)
            {
                registration = default;
                return false;
            }

            var generation = Interlocked.Increment(
                ref _nextActorRegistration);
            if (!_actorRegistrations!.TryAdd(connectionId, generation))
            {
                registration = default;
                return false;
            }

            registration = Registration.CreateActor(
                this,
                connectionId,
                generation);
            return true;
        }
    }

    /// <summary>
    /// 注销连接。Disabled 模式下为 no-op。
    /// Legacy 模式下排空队列；GenericActor 模式下立即 Deactivate 对应 Actor。
    /// </summary>
    private void UnregisterActorConnection(
        uint connectionId,
        long generation)
    {
        var locks = _actorAdmissionLocks!;
        ref var admissionLock = ref locks[
            connectionId & (ActorAdmissionLockCount - 1)];
        var lockTaken = false;
        try
        {
            admissionLock.Enter(ref lockTaken);
            if (!_actorRegistrations!.TryRemove(
                    new KeyValuePair<uint, long>(
                        connectionId,
                        generation)))
            {
                return;
            }

            // 锁内删除 admission，保证返回后旧 lease 不会再成功发布消息。
        }
        finally
        {
            if (lockTaken)
                admissionLock.Exit(useMemoryBarrier: true);
        }

        // 只有仍持有当前 generation 的 lease 才能回收 Actor；旧 session finally
        // 不会按裸 connectionId 关闭后继 session 的 Actor。
        var key = new EphemeralActorKey(
            connectionId,
            generation);
        _actor!.TryDeactivate(in key, ActorDeactivateReason.Explicit);
    }

    /// <summary>
    /// 入队命令。Disabled 模式下返回 false——调用方不应到达此路径
    /// （Specialized Typing 模式下 TypingNotify 已被快路径截获）。
    /// </summary>
    private bool TryEnqueueActor(
        uint connectionId,
        long generation,
        in SessionCommand command)
    {
        var locks = _actorAdmissionLocks!;
        ref var admissionLock = ref locks[
            connectionId & (ActorAdmissionLockCount - 1)];
        var lockTaken = false;
        try
        {
            admissionLock.Enter(ref lockTaken);
            if (!_actorRegistrations!.TryGetValue(
                    connectionId,
                    out var currentGeneration) ||
                currentGeneration != generation)
            {
                return false;
            }

            var key = new EphemeralActorKey(
                connectionId,
                generation);
            var message = EphemeralActorMessage.FromCommand(in command);
            return _actor!.TryTellEphemeral(in key, in message) ==
                   ActorPostStatus.Accepted;
        }
        finally
        {
            if (lockTaken)
                admissionLock.Exit(useMemoryBarrier: true);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_mode == EphemeralPipelineMode.Disabled)
            return Task.CompletedTask;
        return _legacy is not null
            ? _legacy.StartAsync(cancellationToken)
            : _actor!.StartAsync(cancellationToken).AsTask();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_mode == EphemeralPipelineMode.Disabled)
            return;

        if (_legacy is not null)
        {
            await _legacy.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        CloseActorRegistrationAdmission();
        try
        {
            using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
            drainCts.CancelAfter(_operationTimeout + TimeSpan.FromSeconds(2));
            await _actor!
                .StopAsync(ActorStopMode.Drain, drainCts.Token)
                .ConfigureAwait(false);
        }
        finally
        {
            _actorRegistrations!.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        if (_legacy is not null)
            await _legacy.DisposeAsync().ConfigureAwait(false);
        if (_actor is not null)
        {
            CloseActorRegistrationAdmission();
            try
            {
                await _actor.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _actorRegistrations!.Clear();
            }
        }
    }

    private void CloseActorRegistrationAdmission()
    {
        lock (_actorRegistrationGate!)
        {
            _acceptingActorRegistrations = false;
        }
    }

    internal int RegisteredConnectionCount =>
        _legacy?.RegisteredConnectionCount ??
        _actorRegistrations?.Count ??
        0;

    internal readonly struct Registration
    {
        private readonly EphemeralCommandPipeline? _owner;
        private readonly RegistrationKind _kind;
        private readonly SessionCommandExecutor.Registration _legacy;
        private readonly uint _connectionId;
        private readonly long _generation;

        private Registration(
            EphemeralCommandPipeline owner,
            RegistrationKind kind,
            in SessionCommandExecutor.Registration legacy,
            uint connectionId,
            long generation)
        {
            _owner = owner;
            _kind = kind;
            _legacy = legacy;
            _connectionId = connectionId;
            _generation = generation;
        }

        public bool IsValid => _owner is not null;

        public bool TryEnqueue(in SessionCommand command)
        {
            return _kind switch
            {
                RegistrationKind.Legacy => _legacy.TryEnqueue(in command),
                RegistrationKind.Actor when _owner is { } owner =>
                    owner.TryEnqueueActor(
                        _connectionId,
                        _generation,
                        in command),
                _ => false
            };
        }

        public void Unregister()
        {
            switch (_kind)
            {
                case RegistrationKind.Legacy:
                    _legacy.Unregister();
                    break;
                case RegistrationKind.Actor when _owner is { } owner:
                    owner.UnregisterActorConnection(
                        _connectionId,
                        _generation);
                    break;
            }
        }

        internal static Registration CreateNoop(
            EphemeralCommandPipeline owner)
            => new(
                owner,
                RegistrationKind.Noop,
                default,
                connectionId: 0,
                generation: 0);

        internal static Registration CreateLegacy(
            EphemeralCommandPipeline owner,
            in SessionCommandExecutor.Registration legacy)
            => new(
                owner,
                RegistrationKind.Legacy,
                in legacy,
                connectionId: 0,
                generation: 0);

        internal static Registration CreateActor(
            EphemeralCommandPipeline owner,
            uint connectionId,
            long generation)
            => new(
                owner,
                RegistrationKind.Actor,
                default,
                connectionId,
                generation);
    }

    private enum RegistrationKind : byte
    {
        None = 0,
        Noop = 1,
        Legacy = 2,
        Actor = 3
    }

    private readonly record struct EphemeralActorKey(
        uint ConnectionId,
        long Generation);

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

    private struct EphemeralActorState
    {
        public byte Reserved;
    }

    private enum EphemeralActorMessageKind : byte
    {
        Command = 0,
        Completion = 1
    }

    private readonly struct EphemeralActorMessage
    {
        public readonly EphemeralActorMessageKind Kind;
        public readonly SessionCommand Command;

        private EphemeralActorMessage(
            EphemeralActorMessageKind kind,
            in SessionCommand command)
        {
            Kind = kind;
            Command = command;
        }

        public static EphemeralActorMessage FromCommand(
            in SessionCommand command)
            => new(EphemeralActorMessageKind.Command, in command);

        public static EphemeralActorMessage Completion =>
            new(EphemeralActorMessageKind.Completion, default);
    }

    private sealed class EphemeralActorBehavior :
        IActorBehavior<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage>
    {
        private readonly Func<SessionCommand, CancellationToken, ValueTask> _processor;
        private readonly GatewayMetrics _metrics;
        private readonly ILogger _logger;
        private IActorRuntime<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage>? _runtime;

        public EphemeralActorBehavior(
            Func<SessionCommand, CancellationToken, ValueTask> processor,
            GatewayMetrics metrics,
            ILogger logger)
        {
            _processor = processor;
            _metrics = metrics;
            _logger = logger;
        }

        public void Attach(
            IActorRuntime<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage> runtime)
            => _runtime = runtime;

        public void Activate(
            in EphemeralActorKey key,
            ref EphemeralActorState state,
            ref ActorContext<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage> context)
        {
            state.Reserved = 0;
        }

        public ActorTurnResult Receive(
            in EphemeralActorKey key,
            ref EphemeralActorState state,
            in EphemeralActorMessage message,
            ref ActorContext<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage> context)
        {
            if (message.Kind == EphemeralActorMessageKind.Completion)
                return ActorTurnResult.ResumeMailbox;

            var operation = new EphemeralCommandOperation(
                _runtime!,
                key,
                context.Activation,
                in message.Command,
                _processor,
                _metrics,
                _logger);
            if (context.TrySubmitOperation(operation))
                return ActorTurnResult.Suspend;

            SessionCommandResources.Release(in message.Command);
            _metrics.EphemeralEventDropped("actor_async_overloaded");
            return ActorTurnResult.Continue;
        }

        public void Deactivate(
            in EphemeralActorKey key,
            ref EphemeralActorState state,
            ActorDeactivateReason reason,
            ref ActorContext<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage> context)
        {
        }
    }

    private sealed class EphemeralCommandOperation : IAsyncOperation
    {
        private readonly IActorRuntime<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage> _runtime;
        private readonly EphemeralActorKey _key;
        private readonly ActivationId _activation;
        private readonly SessionCommand _command;
        private readonly Func<SessionCommand, CancellationToken, ValueTask> _processor;
        private readonly GatewayMetrics _metrics;
        private readonly ILogger _logger;
        private int _finished;

        public EphemeralCommandOperation(
            IActorRuntime<EphemeralActorKey, EphemeralActorState, EphemeralActorMessage> runtime,
            EphemeralActorKey key,
            ActivationId activation,
            in SessionCommand command,
            Func<SessionCommand, CancellationToken, ValueTask> processor,
            GatewayMetrics metrics,
            ILogger logger)
        {
            _runtime = runtime;
            _key = key;
            _activation = activation;
            _command = command;
            _processor = processor;
            _metrics = metrics;
            _logger = logger;
        }

        public async ValueTask ExecuteAsync(
            CancellationToken cancellationToken)
        {
            try
            {
                if (_command.Session.IsConnected)
                {
                    await _processor(_command, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            finally
            {
                Finish();
            }
        }

        public void OnFailure(
            Exception? exception,
            AsyncOperationFailureKind kind)
        {
            if (kind != AsyncOperationFailureKind.RuntimeStopping &&
                exception is not null)
            {
                _metrics.CommandFailed(_command.Command);
                _logger.CommandFailed(
                    _command.Command,
                    _command.Session.ConnectionId,
                    "ephemeral-actor",
                    exception);
            }

            Finish();
        }

        private void Finish()
        {
            if (Interlocked.Exchange(ref _finished, 1) != 0)
                return;

            SessionCommandResources.Release(in _command);
            var completion = EphemeralActorMessage.Completion;
            _runtime.TryTellCompletion(
                in _key,
                _activation,
                in completion);
        }
    }

    private sealed class EphemeralActorDropHandler :
        IActorMessageDropHandler<EphemeralActorMessage>
    {
        private readonly GatewayMetrics _metrics;

        public EphemeralActorDropHandler(GatewayMetrics metrics)
        {
            _metrics = metrics;
        }

        public void OnDropped(
            in EphemeralActorMessage message,
            ActorMessageDropReason reason)
        {
            if (message.Kind != EphemeralActorMessageKind.Command)
                return;

            SessionCommandResources.Release(in message.Command);
            _metrics.EphemeralEventDropped(GetReason(reason));
        }

        private static string GetReason(ActorMessageDropReason reason)
            => reason switch
            {
                ActorMessageDropReason.Replaced => "actor_replaced",
                ActorMessageDropReason.MailboxFull => "actor_mailbox_full",
                ActorMessageDropReason.RuntimeStopping => "actor_stopping",
                ActorMessageDropReason.BehaviorFaulted => "actor_behavior_fault",
                ActorMessageDropReason.IdleTimeout => "actor_idle_timeout",
                _ => "actor_dropped"
            };
    }
    [LoggerMessage(
        EventId = 3000,
        Level = LogLevel.Error,
        Message = "Ephemeral 排队命令处理致命异常")]
    private static partial void LogEphemeralCommandFatal(ILogger logger, Exception exception);

}
