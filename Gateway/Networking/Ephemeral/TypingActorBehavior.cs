using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Runtime;
using ChatApp.ActorRuntime.Scheduling;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// Typing 领域 Actor 行为：LatestOnly Mailbox + 缓存授权 + fanout。
/// <para>
/// 处理流程：
/// <list type="bullet">
/// <item>Notify 消息：更新期望状态，若授权未缓存则提交授权 I/O 并 Suspend；</item>
/// <item>AuthorizationCompleted：标记授权结果，ResumeMailbox 让最新 Notify 重新处理；</item>
/// <item>授权命中时直接调用 <see cref="TypingFanoutCoordinator.TryAccept"/>，不阻塞 Shard。</item>
/// </list>
/// </para>
/// <para>
/// typing=true → typing=false 合并：LatestOnly Mailbox 在授权 I/O 进行期间仅保留最新 Notify。
/// 授权完成后 ResumeMailbox 处理最新状态，中间状态被自然丢弃。
/// </para>
/// </summary>
internal sealed class TypingActorBehavior
    : IActorBehavior<TypingActorKey, TypingActorState, TypingActorMessage>
{
    private readonly IDirectConversationAuthorizer? _authorizer;
    private readonly TypingFanoutCoordinator _typingFanout;
    private readonly DomainWorkLane<TypingAuthorizationWork> _authLane;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;
    private IActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage>? _runtime;

    public TypingActorBehavior(
        DomainWorkLane<TypingAuthorizationWork> authLane,
        IDirectConversationAuthorizer? authorizer,
        TypingFanoutCoordinator typingFanout,
        GatewayMetrics metrics,
        ILogger logger)
    {
        _authLane = authLane;
        _authorizer = authorizer;
        _typingFanout = typingFanout;
        _metrics = metrics;
        _logger = logger;
    }

    public void Attach(
        IActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage> runtime)
        => _runtime = runtime;

    public void Activate(
        in TypingActorKey key,
        ref TypingActorState state,
        ref ActorContext<TypingActorKey, TypingActorState, TypingActorMessage> context)
    {
        state.ConversationId = key.ToConversationId();
        state.DesiredIsTyping = false;
        state.LastEmittedIsTyping = false;
        state.Authorized = false;
        state.AuthPending = false;
        state.LastNotifyTimestamp = context.Timestamp;
        state.SessionGeneration = 0;
    }

    public ActorTurnResult Receive(
        in TypingActorKey key,
        ref TypingActorState state,
        in TypingActorMessage message,
        ref ActorContext<TypingActorKey, TypingActorState, TypingActorMessage> context)
    {
        return message.Kind == TypingActorMessageKind.AuthorizationCompleted
            ? ReceiveAuthorizationCompleted(in key, in message, ref state)
            : ReceiveNotify(in key, ref state, in message, ref context);
    }

    private ActorTurnResult ReceiveNotify(
        in TypingActorKey key,
        ref TypingActorState state,
        in TypingActorMessage message,
        ref ActorContext<TypingActorKey, TypingActorState, TypingActorMessage> context)
    {
        state.DesiredIsTyping = message.IsTyping;
        state.LastNotifyTimestamp = context.Timestamp;
        state.SessionGeneration = message.SessionGeneration;

        // 授权已缓存：直接发射，无需 I/O。
        if (state.Authorized)
        {
            TryEmit(ref state, in key);
            return ActorTurnResult.Continue;
        }

        // 授权未缓存且无 I/O 进行中：提交授权查询到领域 Lane（不装箱）。
        if (!state.AuthPending)
        {
            // 先预留 Outstanding 槽位（Credit + HasOutstandingOperation），
            // 再提交到领域 Lane。避免 submit 成功但 reserve 失败导致 Completion 丢失。
            if (!context.TryReserveOutstandingOperation())
            {
                _metrics.EphemeralEventDropped("typing_auth_outstanding_busy");
                return ActorTurnResult.Continue;
            }

            var work = new TypingAuthorizationWork(
                _runtime!,
                in key,
                context.Activation,
                key.SenderUserId,
                key.TargetUserId,
                _authorizer,
                _metrics,
                _logger);
            if (_authLane.TrySubmit(in work))
            {
                state.AuthPending = true;
                return ActorTurnResult.Suspend;
            }

            // Lane 满载：回滚 Outstanding 预留，丢弃当前 typing 事件。
            context.ReleaseOutstandingOperation();
            _metrics.EphemeralEventDropped("typing_auth_lane_full");
            return ActorTurnResult.Continue;
        }

        // 授权 I/O 进行中：消息已被 LatestOnly Mailbox 保留，等 Completion 回来后处理。
        return ActorTurnResult.Continue;
    }

    private ActorTurnResult ReceiveAuthorizationCompleted(
        in TypingActorKey key,
        in TypingActorMessage message,
        ref TypingActorState state)
    {
        state.AuthPending = false;
        state.Authorized = message.Authorized;

        if (!message.Authorized)
        {
            // 授权拒绝：丢弃当前期望状态，不发射。
            _metrics.EphemeralEventDropped("typing_auth_denied");
            return ActorTurnResult.Continue;
        }

        // 授权通过：直接发射当前期望状态。
        // 原始 Notify 在提交 auth I/O 时已从 Mailbox 消费，
        // 若无新 Notify 到达，ResumeMailbox 无消息可恢复——
        // 必须在此主动 TryEmit。
        // 若授权期间有新 Notify 到达，LatestOnly Mailbox 保留了最新状态，
        // ResumeMailbox 会处理它并可能再次 TryEmit（幂等：状态相同则跳过）。
        TryEmit(ref state, in key);
        return ActorTurnResult.ResumeMailbox;
    }

    /// <summary>
    /// 授权已获得时调用 TryAccept 发射 typing 状态。
    /// 仅当期望状态与上次发射状态不同时才调用，避免重复发射。
    /// </summary>
    private void TryEmit(ref TypingActorState state, in TypingActorKey key)
    {
        if (state.DesiredIsTyping == state.LastEmittedIsTyping)
            return;

        var emitted = _typingFanout.TryAccept(
            key.SenderUserId,
            key.TargetUserId,
            state.ConversationId,
            state.DesiredIsTyping);

        if (emitted)
            state.LastEmittedIsTyping = state.DesiredIsTyping;
    }

    public void Deactivate(
        in TypingActorKey key,
        ref TypingActorState state,
        ActorDeactivateReason reason,
        ref ActorContext<TypingActorKey, TypingActorState, TypingActorMessage> context)
    {
        // 若 typing 仍活跃且曾发射，发送 typing=false 以清理对端状态。
        // TypingFanoutCoordinator 的 TTL 也会兜底，此处仅做尽力而为的即时清理。
        if (state.LastEmittedIsTyping && reason != ActorDeactivateReason.RuntimeStopping)
        {
            _typingFanout.TryAccept(
                key.SenderUserId,
                key.TargetUserId,
                state.ConversationId,
                isTyping: false);
        }
    }
}

/// <summary>
/// Typing 授权 I/O 操作：调用 <see cref="IDirectConversationAuthorizer.AuthorizeAsync"/>。
/// <para>
/// 缓存命中时几乎同步完成（仅 Channel 往返）；缓存未命中时执行远程授权查询。
/// 完成后通过 <see cref="IActorRuntime{TKey,TState,TMessage}.TryTellCompletion"/>
/// 回投 <see cref="TypingActorMessage.AuthorizationCompleted"/> 唤醒 Actor。
/// </para>
/// </summary>
internal readonly struct TypingAuthorizationWork : IAsyncOperation
{
    private readonly IActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage> _runtime;
    private readonly TypingActorKey _key;
    private readonly ActivationId _activation;
    private readonly long _senderUserId;
    private readonly long _targetUserId;
    private readonly IDirectConversationAuthorizer? _authorizer;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;

    public TypingAuthorizationWork(
        IActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage> runtime,
        in TypingActorKey key,
        ActivationId activation,
        long senderUserId,
        long targetUserId,
        IDirectConversationAuthorizer? authorizer,
        GatewayMetrics metrics,
        ILogger logger)
    {
        _runtime = runtime;
        _key = key;
        _activation = activation;
        _senderUserId = senderUserId;
        _targetUserId = targetUserId;
        _authorizer = authorizer;
        _metrics = metrics;
        _logger = logger;
    }

    public async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        bool authorized;
        if (_authorizer is null)
        {
            // 测试场景：无授权器注入时默认允许。
            authorized = true;
        }
        else
        {
            try
            {
                authorized = await _authorizer
                    .AuthorizeAsync(_senderUserId, _targetUserId, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // 停机时不再回投 Completion——Runtime 正在停止。
                throw;
            }
            catch (Exception ex)
            {
                // 授权失败视为拒绝，不阻断 Actor 后续消息处理。
                _metrics.EphemeralEventDropped("typing_auth_failed");
                _logger.DependencyOperationFailed(
                    GatewayDependency.RealtimeService,
                    GatewayDependencyOperation.EphemeralTypingPublish,
                    ex);
                authorized = false;
            }
        }

        var completion = TypingActorMessage.AuthorizationCompleted(authorized);
        _runtime.TryTellCompletion(in _key, _activation, in completion);
    }

    public void OnFailure(Exception? exception, AsyncOperationFailureKind kind)
    {
        if (kind == AsyncOperationFailureKind.RuntimeStopping)
            return;

        // 超时或异常：回投 denied Completion 以唤醒 Suspend 的 Actor。
        var completion = TypingActorMessage.AuthorizationCompleted(authorized: false);
        _runtime.TryTellCompletion(in _key, _activation, in completion);
    }
}

/// <summary>
/// Typing Actor 消息丢弃处理器：记录 metric。
/// </summary>
internal sealed class TypingActorDropHandler
    : IActorMessageDropHandler<TypingActorMessage>
{
    private readonly GatewayMetrics _metrics;

    public TypingActorDropHandler(GatewayMetrics metrics)
    {
        _metrics = metrics;
    }

    public void OnDropped(
        in TypingActorMessage message,
        ActorMessageDropReason reason)
    {
        _metrics.EphemeralEventDropped(GetReason(reason));
    }

    private static string GetReason(ActorMessageDropReason reason)
        => reason switch
        {
            ActorMessageDropReason.Replaced => "typing_actor_replaced",
            ActorMessageDropReason.MailboxFull => "typing_actor_mailbox_full",
            ActorMessageDropReason.RuntimeStopping => "typing_actor_stopping",
            ActorMessageDropReason.BehaviorFaulted => "typing_actor_fault",
            ActorMessageDropReason.IdleTimeout => "typing_actor_idle_timeout",
            _ => "typing_actor_dropped"
        };
}
