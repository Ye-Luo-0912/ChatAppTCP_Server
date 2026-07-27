namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Actor 行为契约：每个 Actor Domain（Session/Typing/Presence/Realtime）实现一份。
/// <para>
/// 所有方法必须同步执行——外部 I/O 通过 <see cref="ActorContext.TrySubmitOperation{TWork}"/>
/// 转交独立 Executor，Shard 在 Suspend 期间可继续处理其他 Actor。
/// </para>
/// <para>
/// 约束：
/// <list type="bullet">
/// <item>Activate/Receive/Deactivate 在 Shard 单写线程调用，无需自旋锁；</item>
/// <item>禁止直接 await 外部 I/O（NATS/Redis/Query）；</item>
/// <item>状态变更通过 ref TState 直接修改（值类型，零分配）；</item>
/// <item>异常会让 Actor 进入 Faulted 状态并被 Deactivate。</item>
/// </list>
/// </para>
/// </summary>
public interface IActorBehavior<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    /// <summary>
    /// Actor 首次收到消息时激活。初始化 <paramref name="state"/> 默认值。
    /// 仅在 Actor 首次入队前调用一次；后续消息直接走 <see cref="Receive"/>。
    /// </summary>
    void Activate(
        in TKey key,
        ref TState state,
        ref ActorContext<TKey, TState, TMessage> context);

    /// <summary>
    /// 处理一条消息。返回值控制 Shard 调度循环（继续/挂起/完成）。
    /// <para>
    /// 实现规则：
    /// <list type="bullet">
    /// <item>纯状态变更返回 <see cref="ActorTurnResult.Continue"/>；</item>
    /// <item>提交 I/O 后返回 <see cref="ActorTurnResult.Suspend"/>，等 OperationCompleted 回来后再 ResumeMailbox；</item>
    /// <item>显式关闭返回 <see cref="ActorTurnResult.Complete"/>。</item>
    /// </list>
    /// </para>
    /// </summary>
    ActorTurnResult Receive(
        in TKey key,
        ref TState state,
        in TMessage message,
        ref ActorContext<TKey, TState, TMessage> context);

    /// <summary>
    /// Actor 被 Deactivate 时调用。释放资源、回写最终状态、发回执等。
    /// </summary>
    void Deactivate(
        in TKey key,
        ref TState state,
        ActorDeactivateReason reason,
        ref ActorContext<TKey, TState, TMessage> context);
}
