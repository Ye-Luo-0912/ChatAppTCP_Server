namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Receive/Activate/Deactivate 调用期间的上下文。ref struct 禁止逃逸到异步或字段。
/// <para>
/// 提供 Actor 向 Shard/Runtime 提交副作用的入口：
/// <list type="bullet">
/// <item><see cref="TryPostLocal"/>：自投递消息到当前 Shard 的 Ingress Ring；</item>
/// <item><see cref="TrySubmitOperation{TWork}"/>：提交异步 I/O 到全局 Executor；</item>
/// <item><see cref="TrySchedule"/>：注册一个 deadline，到期后回投 DeadlineExpired 消息。</item>
/// </list>
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
    private readonly uint _generation;

    internal ActorContext(
        IActorContextSink<TKey, TState, TMessage> sink,
        long timestamp,
        uint generation)
    {
        _sink = sink;
        _timestamp = timestamp;
        _generation = generation;
    }

    /// <summary>当前 monotonic timestamp（TimeProvider.GetTimestamp），供行为逻辑判断时间。</summary>
    public long Timestamp => _timestamp;

    /// <summary>当前 Actor 的 generation。每次 Activate/重新激活自增；迟到的 Completion 通过此值识别。</summary>
    public uint Generation => _generation;

    /// <summary>
    /// 自投递消息到当前 Shard 的 Ingress Ring。
    /// 用于 Behavior 在 Receive 中触发后续消息（如完成一条后投递"继续处理下一条"信号）。
    /// </summary>
    public bool TryPostLocal(in TMessage message)
        => _sink.TryPostLocal(in message);

    /// <summary>
    /// 提交异步 I/O 操作到全局 <c>AsyncOperationExecutor</c>。
    /// <para>
    /// 调用后 Behavior 应返回 <see cref="ActorTurnResult.Suspend"/>——Shard 不再处理此 Actor Mailbox，
    /// 直到 Operation 自己 Post 回 OperationCompleted 消息唤醒。
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
    /// 避免 class 再被装箱为接口。
    /// </summary>
    public bool TrySubmitOperation(IAsyncOperation operation)
        => _sink.TrySubmitOperation(operation);

    /// <summary>
    /// 注册一个 deadline，到期后 Runtime 会向当前 Actor Post 一条由 <paramref name="message"/> 指定的消息。
    /// <para>
    /// 用于 Typing 过期、Session Idle 超时、发送超时等。
    /// <paramref name="generation"/> 用于取消过期定时器：若 Actor 已重新激活（generation 变化），
    /// 旧的 DeadlineExpired 消息会被 Behavior 识别为 StaleGeneration 并忽略。
    /// </para>
    /// </summary>
    public bool TrySchedule(TimeSpan delay, uint generation, in TMessage message)
        => _sink.TrySchedule(delay, generation, in message);
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
    bool TrySchedule(TimeSpan delay, uint generation, in TMessage message);
}
