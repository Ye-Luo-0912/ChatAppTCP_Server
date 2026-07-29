namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Actor Runtime 公共入口。Sharded 单写者模型：消息路由到固定 Shard 的 MPSC Ingress Ring，
/// Shard 单 Consumer Loop 串行处理同 Actor 的消息，跨 Actor 并行。
/// </summary>
public interface IActorRuntime<TKey, TState, TMessage> : IAsyncDisposable
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    /// <summary>
    /// 同步入队消息。高频默认 API：不创建 Task/CTS/CompletionSource/回执消息对象。
    /// <para>
    /// 注意：Accepted 仅表示通过生产侧准入；若 Shard 消费时 Actor 数已达上限
    /// （MaxActiveActors / MaxActiveActorsPerShard），消息仍会被丢弃并计入
    /// <see cref="ActorRuntimeSnapshot.TotalActiveActorAdmissionRejected"/>。
    /// </para>
    /// <para>
    /// 临时消息（可丢）应使用 <see cref="TryTellEphemeral"/>；
    /// 持久消息（不可丢）应使用 <see cref="TryTellDurable"/> 以获得生产侧配额检查。
    /// </para>
    /// </summary>
    ActorPostStatus TryTell(in TKey key, in TMessage message);

    /// <summary>
    /// 同步入队临时消息（Typing/Presence 等）。等同于 <see cref="TryTell"/>，
    /// 不检查 Actor 数量配额——若 Shard 消费时 Actor 数已达上限，
    /// 消息会被丢弃并计入 <see cref="ActorRuntimeSnapshot.TotalActiveActorAdmissionRejected"/>。
    /// <para>
    /// 适用于可丢消息场景。<see cref="ActorPostStatus.Accepted"/> 仅表示进入 Ingress Ring。
    /// </para>
    /// </summary>
    ActorPostStatus TryTellEphemeral(in TKey key, in TMessage message)
        => TryTell(in key, in message);

    /// <summary>
    /// 同步入队持久消息（Chat/Receipt/Edit/Recall 等）。
    /// 在生产侧同步检查 Actor 数量配额（全局 + 每 Shard）：
    /// <list type="bullet">
    /// <item><see cref="ActorPostStatus.AdmissionRejected"/>：配额已满，消息未入队，调用方应拒绝或关闭连接；</item>
    /// </list>
    /// 其余返回值与 <see cref="TryTell"/> 一致。
    /// <para>
    /// 注意：由于生产侧检查与消费侧创建存在竞态窗口（极小），
    /// 消费侧仍保留配额安全网。若竞态导致消费侧拒绝（极罕见），
    /// 消息会被丢弃并计入 <see cref="ActorRuntimeSnapshot.TotalActiveActorAdmissionRejected"/>。
    /// 调用方应在收到 AdmissionRejected 时拒绝客户端请求而非接受重试。
    /// </para>
    /// </summary>
    ActorPostStatus TryTellDurable(in TKey key, in TMessage message);

    /// <summary>
    /// 投递异步操作完成消息。Completion 走控制通道（独立于普通 Mailbox 的高优先级槽），
    /// 即使 Actor 处于 Suspend 且普通 Mailbox 非空也会被优先调度。
    /// <para>
    /// 仅允许由经 <see cref="ActorContext{TKey,TState,TMessage}.TrySubmitOperation{TWork}"/>
    /// 提交的操作回投：<paramref name="activation"/> 必须是提交时 Actor 的
    /// <see cref="ActorContext{TKey,TState,TMessage}.Activation"/>。
    /// 提交成功时已预留 Completion 槽位（Credit），回投不会失败于容量。
    /// </para>
    /// </summary>
    ActorPostStatus TryTellCompletion(
        in TKey key,
        ActivationId activation,
        in TMessage message);

    /// <summary>
    /// 投递控制通道 Invalidation 消息（如 Typing 授权失效）。走独立的 Invalidation 控制槽，
    /// 优先级高于 Completion 与普通 Mailbox——确保失效在处理过期 Completion 前生效。
    /// <para>
    /// 与 <see cref="TryTellEphemeral"/> 的关键区别：Invalidation 不会被 LatestOnly Mailbox
    /// 中的后续 Notify 覆盖。这使得"授权失效"不会被"新 typing 状态"静默丢弃。
    /// </para>
    /// <para>
    /// 幂等：多次 Invalidation 投递等效于一次（控制槽覆盖语义）。
    /// Actor 不存在时静默丢弃（Ephemeral 语义，关系变更后 TTL 兜底）。
    /// </para>
    /// </summary>
    /// <returns>
    /// <see cref="ActorPostStatus.Accepted"/> 表示已进入 Ingress；
    /// <see cref="ActorPostStatus.RuntimeStopping"/> 表示 Runtime 正在停止；
    /// <see cref="ActorPostStatus.ShardOverloaded"/> 表示 Ingress Ring 满（丢弃）。
    /// </returns>
    ActorPostStatus TryTellInvalidation(in TKey key, in TMessage message);

    /// <summary>
    /// 请求显式 Deactivate 指定 Actor（如连接断开时立即回收对应 Ephemeral Actor，
    /// 不等待 Idle Sweep）。按 Key 当前任意激活匹配；Key 会被复用的场景应使用
    /// <see cref="TryDeactivate(in TKey, ActivationId, ActorDeactivateReason)"/> 精确指定。
    /// </summary>
    /// <returns>false 表示 Runtime 正在停止或 Shard Ingress 已满（调用方可稍后重试）。</returns>
    bool TryDeactivate(in TKey key, ActorDeactivateReason reason)
        => TryDeactivate(in key, ActivationId.None, reason);

    /// <summary>
    /// 请求显式 Deactivate 指定 Actor，仅当当前激活纪元等于 <paramref name="activation"/> 时生效；
    /// 否则视为过期请求直接忽略（防 ABA）。
    /// </summary>
    /// <returns>false 表示 Runtime 正在停止或 Shard Ingress 已满（调用方可稍后重试）。</returns>
    bool TryDeactivate(
        in TKey key,
        ActivationId activation,
        ActorDeactivateReason reason);

    /// <summary>启动所有 Shard Consumer Loop 与 AsyncOperationExecutor。重复调用幂等。</summary>
    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>停止 Runtime：取消所有 Loop、排空或丢弃 Mailbox、Deactivate 所有 Actor。</summary>
    ValueTask StopAsync(ActorStopMode mode, CancellationToken cancellationToken);

    /// <summary>获取当前 Runtime 的原子计数快照（不保证强一致），供宿主映射到 Metrics。</summary>
    ActorRuntimeSnapshot GetSnapshot();
}
