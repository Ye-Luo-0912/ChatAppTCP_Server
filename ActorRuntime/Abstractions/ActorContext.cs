namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Receive/Activate/Deactivate 调用期间的上下文。ref struct 禁止逃逸到异步或字段。
/// <para>
/// 提供 Actor 向 Shard/Runtime 提交副作用的入口：
/// <list type="bullet">
/// <item><see cref="TryPostLocal"/>：自投递消息到当前 Shard 的 Ingress Ring；</item>
/// <item><see cref="TrySubmitOperation{TWork}"/>：提交异步 I/O 到全局 Executor；</item>
/// <item><see cref="TrySchedule"/>/<see cref="TryScheduleOrReplace"/>：注册 deadline，到期后回投消息。</item>
/// </list>
/// </para>
/// <para>
/// 单 Outstanding Operation 约束（v2）：一个 Actor 同一时刻最多一个未完成的异步操作。
/// 同一 Turn 内第二次 <see cref="TrySubmitOperation{TWork}"/> 返回 false；
/// 返回 <see cref="ActorTurnResult.Suspend"/> 时必须已成功提交 Operation 或持有未触发 Deadline，
/// 否则 Actor 会被判定为契约违反并 Faulted Deactivate。
/// </para>
/// <para>
/// 当前版本不提供 <c>TryComplete&lt;TResult&gt;</c> 池化 Completion 原语。
/// Query 等需要向 TCP 调用方返回 <c>ValueTask&lt;TResult&gt;</c> 的场景，
/// 由 Behavior 在自己的 State 中持有 TaskCompletionSource 并在 Receive 中 Resolve。
/// 这是 v1 简化：池化 MREVTS Pool 为后续优化点。
/// </para>
/// </summary>
public ref struct ActorContext<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    // 持有 Shard 内部接口的引用；由 ActorShard 在调用 Behavior 时构造。
    // 字段类型使用接口而非具体 Shard 类，避免在 Abstractions 层暴露 internal 类型。
    private readonly IActorContextSink<TKey, TState, TMessage> _sink;
    private readonly long _timestamp;
    private readonly ActivationId _activation;

    internal ActorContext(
        IActorContextSink<TKey, TState, TMessage> sink,
        long timestamp,
        ActivationId activation)
    {
        _sink = sink;
        _timestamp = timestamp;
        _activation = activation;
    }

    /// <summary>当前 monotonic timestamp（TimeProvider.GetTimestamp），供行为逻辑判断时间。</summary>
    public long Timestamp => _timestamp;

    /// <summary>
    /// 当前 Actor 的激活纪元。每次 Activate 从 Shard 单调计数器分配（不按 Key 重置）；
    /// 迟到的 Completion / Deadline 通过此值识别并丢弃。
    /// </summary>
    public ActivationId Activation => _activation;

    /// <summary>
    /// 自投递消息到当前 Shard 的 Ingress Ring。
    /// 用于 Behavior 在 Receive 中触发后续消息（如完成一条后投递"继续处理下一条"信号）。
    /// </summary>
    public bool TryPostLocal(in TMessage message)
        => _sink.TryPostLocal(in message);

    /// <summary>
    /// 提交异步 I/O 操作到全局 <c>AsyncOperationExecutor</c>。
    /// <para>
    /// 调用后 Behavior 应返回 <see cref="ActorTurnResult.Suspend"/>——Shard 不再处理此 Actor 的普通 Mailbox，
    /// 直到 Operation 自己 Post 回 OperationCompleted 消息唤醒（控制通道，Busy 时仍会被处理）。
    /// </para>
    /// <para>
    /// 约束：一个 Actor 同一时刻最多一个 Outstanding Operation。Actor 已有未完成操作、
    /// 或同一 Turn 内重复提交时返回 false。提交成功即预留 Completion 槽位（Credit），
    /// Completion 回投保证不会因控制通道满而丢失。
    /// </para>
    /// <para>
    /// TWork 是 struct 实现 <see cref="IAsyncOperation"/>；提交时会被装箱一次存入 Executor 队列。
    /// I/O 路径已有分配（NATS/Redis buffer），此次装箱可接受；后续可用泛型强类型 Executor 优化。
    /// </para>
    /// </summary>
    public bool TrySubmitOperation<TWork>(in TWork operation)
        where TWork : struct, IAsyncOperation
        => _sink.TrySubmitOperation(in operation);

    /// <summary>
    /// 提交引用类型异步操作。适合本身需要保存委托/所有权状态的 I/O 操作，
    /// 避免 class 再被装箱为接口。约束同 <see cref="TrySubmitOperation{TWork}"/>。
    /// </summary>
    public bool TrySubmitOperation(IAsyncOperation operation)
        => _sink.TrySubmitOperation(operation);

    /// <summary>
    /// 预留 Outstanding Operation 槽位（Completion Credit + HasOutstandingOperation 标志），
    /// 不实际提交到 Executor。用于领域强类型路径：Behavior 直接调用
    /// <c>DomainWorkLane&lt;TWork&gt;.TrySubmit(work)</c> 后，用此方法通知 Shard
    /// 已有未完成操作，避免通用 <see cref="TrySubmitOperation{TWork}"/> 装箱到
    /// <see cref="IAsyncOperation"/>。
    /// <para>
    /// 推荐调用顺序：先 <c>TryReserveOutstandingOperation()</c>，成功后再 <c>Lane.TrySubmit(work)</c>。
    /// 若 <c>TrySubmit</c> 失败，必须调用 <see cref="ReleaseOutstandingOperation"/> 回滚。
    /// </para>
    /// </summary>
    public bool TryReserveOutstandingOperation()
        => _sink.TryReserveOutstandingOperation();

    /// <summary>
    /// 回滚 <see cref="TryReserveOutstandingOperation"/> 的预留。
    /// 仅在 Reserve 成功但后续 Lane.TrySubmit 失败时调用。
    /// </summary>
    public void ReleaseOutstandingOperation()
        => _sink.ReleaseOutstandingOperation();

    /// <summary>
    /// 注册一个 deadline，到期后 Runtime 会向当前 Actor 的控制通道投递 <paramref name="message"/>。
    /// <para>
    /// 用于 Typing 过期、Session Idle 超时、发送超时等。Deadline 消息走控制通道：
    /// 即使 Actor 处于 Busy（Suspend 等待 I/O）也会被处理。
    /// 激活纪元与 Deadline 代际由 Runtime 隐式携带——Actor 重新激活或被
    /// <see cref="TryScheduleOrReplace"/>/<see cref="CancelDeadlines"/> 替换后，
    /// 旧 Deadline 触发时会被识别为过期并丢弃。
    /// </para>
    /// <para>
    /// 每个 Actor 未触发 Deadline 数有上限（含已触发未消费的 Deadline 控制消息）；
    /// 超过上限返回 false。持有未触发 Deadline 的 Actor 不会被 Idle Sweep 回收。
    /// </para>
    /// </summary>
    public bool TrySchedule(TimeSpan delay, in TMessage message)
        => _sink.TrySchedule(delay, replaceExisting: false, in message);

    /// <summary>
    /// 注册 deadline 并使该 Actor 此前所有未触发 Deadline 失效（惰性取消）。
    /// 典型用法：Typing 每次刷新只保留最新过期时间。
    /// </summary>
    public bool TryScheduleOrReplace(TimeSpan delay, in TMessage message)
        => _sink.TrySchedule(delay, replaceExisting: true, in message);

    /// <summary>
    /// 惰性取消当前 Actor 所有未触发 Deadline（已触发进入控制通道的消息不受影响）。
    /// 取消后若不再有未触发 Deadline，Actor 重新满足 Idle Sweep 条件。
    /// </summary>
    public void CancelDeadlines()
        => _sink.CancelDeadlines();
}

/// <summary>
/// ActorContext 转发的内部 Sink 接口。由 Runtime 内部 Shard 实现。
/// 公开为 internal 以便 ActorContext 在 Abstractions 层引用 Shard 而不破坏依赖方向。
/// </summary>
public interface IActorContextSink<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    bool TryPostLocal(in TMessage message);
    bool TrySubmitOperation<TWork>(in TWork operation) where TWork : struct, IAsyncOperation;
    bool TrySubmitOperation(IAsyncOperation operation);
    bool TryReserveOutstandingOperation();
    void ReleaseOutstandingOperation();
    bool TrySchedule(TimeSpan delay, bool replaceExisting, in TMessage message);
    void CancelDeadlines();
}
