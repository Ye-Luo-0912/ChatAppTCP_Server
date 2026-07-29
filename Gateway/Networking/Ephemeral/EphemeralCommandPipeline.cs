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
/// </summary>
internal sealed class EphemeralCommandPipeline : IAsyncDisposable
{
    private readonly SessionCommandExecutor? _legacy;
    private readonly ActorRuntime<uint, EphemeralActorState, EphemeralActorMessage>? _actor;
    private readonly TimeSpan _operationTimeout;
    private bool _disposed;

    public EphemeralCommandPipeline(
        TcpGatewayOptions options,
        Func<SessionCommand, CancellationToken, ValueTask> processor,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(processor);
        _operationTimeout = options.EphemeralActorOperationTimeout;

        if (!options.UseActorRuntimeForEphemeralCommands)
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
                onFatalError: null,
                logger);
            return;
        }

        var behavior = new EphemeralActorBehavior(
            processor,
            metrics,
            logger);
        var dropHandler = new EphemeralActorDropHandler(metrics);
        _actor = new ActorRuntime<uint, EphemeralActorState, EphemeralActorMessage>(
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
        behavior.Attach(_actor);
    }

    public bool UsesActorRuntime => _actor is not null;

    public ActorRuntimeSnapshot Snapshot =>
        _actor?.GetSnapshot() ?? default;

    public bool TryRegisterConnection(uint connectionId, long userId)
        => _legacy?.TryRegisterConnection(connectionId, userId) ?? true;

    public void UnregisterConnection(uint connectionId)
    {
        if (_legacy is not null)
        {
            _legacy.UnregisterConnection(connectionId);
            return;
        }

        // 连接断开立即回收对应 Ephemeral Actor，不等待 Idle Sweep（P0-6）。
        // ActivationId.None 匹配当前任意激活；Shard Ingress 满时退回 Idle 回收兜底。
        _actor!.TryDeactivate(
            in connectionId,
            ActorDeactivateReason.Explicit);
    }

    public bool TryEnqueue(
        uint connectionId,
        in SessionCommand command)
    {
        if (_legacy is not null)
            return _legacy.TryEnqueue(connectionId, in command);

        var message = EphemeralActorMessage.FromCommand(in command);
        return _actor!.TryTellEphemeral(in connectionId, in message) ==
               ActorPostStatus.Accepted;
    }

    public Task StartAsync(CancellationToken cancellationToken)
        => _legacy is not null
            ? _legacy.StartAsync(cancellationToken)
            : _actor!.StartAsync(cancellationToken).AsTask();

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_legacy is not null)
        {
            await _legacy.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        drainCts.CancelAfter(_operationTimeout + TimeSpan.FromSeconds(2));
        await _actor!
            .StopAsync(ActorStopMode.Drain, drainCts.Token)
            .ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        if (_legacy is not null)
            await _legacy.DisposeAsync().ConfigureAwait(false);
        if (_actor is not null)
            await _actor.DisposeAsync().ConfigureAwait(false);
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
        IActorBehavior<uint, EphemeralActorState, EphemeralActorMessage>
    {
        private readonly Func<SessionCommand, CancellationToken, ValueTask> _processor;
        private readonly GatewayMetrics _metrics;
        private readonly ILogger _logger;
        private IActorRuntime<uint, EphemeralActorState, EphemeralActorMessage>? _runtime;

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
            IActorRuntime<uint, EphemeralActorState, EphemeralActorMessage> runtime)
            => _runtime = runtime;

        public void Activate(
            in uint key,
            ref EphemeralActorState state,
            ref ActorContext<uint, EphemeralActorState, EphemeralActorMessage> context)
        {
            state.Reserved = 0;
        }

        public ActorTurnResult Receive(
            in uint key,
            ref EphemeralActorState state,
            in EphemeralActorMessage message,
            ref ActorContext<uint, EphemeralActorState, EphemeralActorMessage> context)
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
            in uint key,
            ref EphemeralActorState state,
            ActorDeactivateReason reason,
            ref ActorContext<uint, EphemeralActorState, EphemeralActorMessage> context)
        {
        }
    }

    private sealed class EphemeralCommandOperation : IAsyncOperation
    {
        private readonly IActorRuntime<uint, EphemeralActorState, EphemeralActorMessage> _runtime;
        private readonly uint _key;
        private readonly ActivationId _activation;
        private readonly SessionCommand _command;
        private readonly Func<SessionCommand, CancellationToken, ValueTask> _processor;
        private readonly GatewayMetrics _metrics;
        private readonly ILogger _logger;
        private int _finished;

        public EphemeralCommandOperation(
            IActorRuntime<uint, EphemeralActorState, EphemeralActorMessage> runtime,
            uint key,
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
}
