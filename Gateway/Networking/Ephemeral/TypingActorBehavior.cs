using System.Diagnostics;
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
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _authorizationTtl;
    private IActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage>? _runtime;

    public TypingActorBehavior(
        DomainWorkLane<TypingAuthorizationWork> authLane,
        IDirectConversationAuthorizer? authorizer,
        TypingFanoutCoordinator typingFanout,
        GatewayMetrics metrics,
        ILogger logger,
        TimeProvider timeProvider,
        TimeSpan authorizationTtl)
    {
        _authLane = authLane;
        _authorizer = authorizer;
        _typingFanout = typingFanout;
        _metrics = metrics;
        _logger = logger;
        _timeProvider = timeProvider;
        _authorizationTtl = authorizationTtl;
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
        return message.Kind switch
        {
            TypingActorMessageKind.AuthorizationCompleted =>
                ReceiveAuthorizationCompleted(in key, in message, ref state),
            TypingActorMessageKind.AuthorizationInvalidated =>
                ReceiveAuthorizationInvalidated(in key, ref state),
            _ => ReceiveNotify(in key, ref state, in message, ref context)
        };
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

        // 授权已缓存且未过 TTL：直接发射，无需 I/O。
        // TTL 由 TypingAuthorizationTtl 配置（默认与 CachedDirectConversationAuthorizer
        // 的 allowTtl 对齐）。超过 TTL 后 Authorized 自动失效，下一次 Notify 触发新 I/O。
        if (state.Authorized && context.Timestamp < state.AuthorizedUntilTimestamp)
        {
            TryEmit(ref state, in key);
            return ActorTurnResult.Continue;
        }

        // 授权过期：清空缓存，触发重新授权。
        if (state.Authorized)
        {
            state.Authorized = false;
            state.AuthorizedUntilTimestamp = 0;
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

            // P0-9：捕获当前授权纪元，Work 完成时携带此 epoch 回投 Completion。
            // 若 Invalidation 在 I/O 期间到达并自增 epoch，Completion 的旧 epoch
            // 会被 ReceiveAuthorizationCompleted 拒绝，避免 stale 结果覆盖失效。
            var work = new TypingAuthorizationWork(
                _runtime!,
                in key,
                context.Activation,
                key.SenderUserId,
                key.TargetUserId,
                _authorizer,
                _metrics,
                _logger,
                state.AuthorizationEpoch);
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

        // P0-9：授权纪元校验——若 Invalidation 在 I/O 期间到达并自增了 epoch，
        // 此 Completion 携带的旧 epoch 不匹配，拒绝结果（Authorized 保持 false）。
        // 这防止了"关系变更 → 失效 → 旧 I/O 完成回投 Authorized=true 覆盖失效"的竞态。
        if (message.AuthorizationEpoch != state.AuthorizationEpoch)
        {
            _metrics.EphemeralEventDropped("typing_auth_stale_completion");
            // 不发射、不缓存授权。DesiredIsTyping 已被 Invalidation 重置为 false，
            // 此处无需再次 TryEmit。若 Invalidation 之后有新 Notify 到达，
            // LatestOnly Mailbox 会保留它，ResumeMailbox 后重新触发授权 I/O。
            return ActorTurnResult.ResumeMailbox;
        }

        state.Authorized = message.Authorized;

        if (!message.Authorized)
        {
            // 授权拒绝：丢弃当前期望状态，不发射。
            _metrics.EphemeralEventDropped("typing_auth_denied");
            return ActorTurnResult.Continue;
        }

        // 授权通过：记录 TTL 截止时间戳，到期后下一次 Notify 必须重新走 I/O。
        // 与 CachedDirectConversationAuthorizer 的 allowTtl 对齐（默认 30s）。
        state.AuthorizedUntilTimestamp = _timeProvider.GetTimestamp() +
            (long)(_authorizationTtl.TotalSeconds * _timeProvider.TimestampFrequency);

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
    /// 关系变更触发的授权失效：自增授权纪元（使在途的 stale Completion 被拒绝）、
    /// 清空缓存的 Authorized=true 与 TTL、发射 typing=false 清理对端状态、
    /// 重置 DesiredIsTyping=false（避免 stale Completion 后 ResumeMailbox 误发射）。
    /// <para>
    /// P0-9：经控制通道投递（TryTellInvalidation），不被 LatestOnly Mailbox 中的
    /// 后续 Notify 覆盖。Invalidation 优先级高于 Completion，确保在处理 stale Completion
    /// 前完成 epoch 自增。
    /// </para>
    /// <para>
    /// 不主动 Deactivate——若有在途 I/O，Completion 仍会到达并被 epoch 校验拒绝。
    /// 不提交新 I/O——Outstanding 可能仍被在途 I/O 占用。
    /// </para>
    /// </summary>
    private ActorTurnResult ReceiveAuthorizationInvalidated(
        in TypingActorKey key,
        ref TypingActorState state)
    {
        // 自增 epoch：使在途 Completion 的旧 epoch 不匹配，被 ReceiveAuthorizationCompleted 拒绝。
        state.AuthorizationEpoch++;

        // 清空缓存授权与 TTL。
        state.Authorized = false;
        state.AuthorizedUntilTimestamp = 0;

        // 发射 typing=false 清理对端状态（若当前正在 typing）。
        // 重置 DesiredIsTyping 与 LastEmittedIsTyping 以保持一致。
        if (state.LastEmittedIsTyping)
        {
            _typingFanout.TryAccept(
                key.SenderUserId,
                key.TargetUserId,
                state.ConversationId,
                isTyping: false);
            state.LastEmittedIsTyping = false;
        }

        // 重置期望状态：失效后不应继续尝试发射旧 typing=true。
        // 若用户在失效后仍想 typing，需发送新 Notify 触发新一轮授权 I/O。
        state.DesiredIsTyping = false;

        // AuthPending 保持原值：
        // - 若 I/O 在途（AuthPending=true），Completion 仍会到达并被 epoch 拒绝。
        //   拒绝后 AuthPending 被置 false，Outstanding 被释放。
        // - 若无 I/O 在途（AuthPending=false），无需额外处理。
        return ActorTurnResult.Continue;
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
    private readonly uint _authorizationEpoch;

    public TypingAuthorizationWork(
        IActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage> runtime,
        in TypingActorKey key,
        ActivationId activation,
        long senderUserId,
        long targetUserId,
        IDirectConversationAuthorizer? authorizer,
        GatewayMetrics metrics,
        ILogger logger,
        uint authorizationEpoch)
    {
        _runtime = runtime;
        _key = key;
        _activation = activation;
        _senderUserId = senderUserId;
        _targetUserId = targetUserId;
        _authorizer = authorizer;
        _metrics = metrics;
        _logger = logger;
        _authorizationEpoch = authorizationEpoch;
    }

    public async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        // 用 Stopwatch 高频时钟记录授权 I/O 耗时，避免向 struct 注入 TimeProvider 增大体积。
        var startTimestamp = Stopwatch.GetTimestamp();
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

        // 记录授权 I/O 耗时直方图：含缓存命中与远程查询，按 outcome 区分。
        var elapsedMs = (double)(Stopwatch.GetTimestamp() - startTimestamp)
            / Stopwatch.Frequency * 1000.0;
        _metrics.TypingAuthCompleted(elapsedMs, authorized);

        // P0-9：Completion 携带提交时捕获的授权纪元。
        // Behavior 比较此 epoch 与 state.AuthorizationEpoch 以拒绝 stale Completion。
        var completion = TypingActorMessage.AuthorizationCompleted(authorized, _authorizationEpoch);
        _runtime.TryTellCompletion(in _key, _activation, in completion);
    }

    public void OnFailure(Exception? exception, AsyncOperationFailureKind kind)
    {
        if (kind == AsyncOperationFailureKind.RuntimeStopping)
            return;

        // 超时或异常：回投 denied Completion 以唤醒 Suspend 的 Actor。
        // 携带原始 epoch——若 Invalidation 在此期间到达，epoch 不匹配会被拒绝，
        // Actor 保持 Authorized=false（安全语义：失效优先）。
        var completion = TypingActorMessage.AuthorizationCompleted(
            authorized: false,
            _authorizationEpoch);
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
