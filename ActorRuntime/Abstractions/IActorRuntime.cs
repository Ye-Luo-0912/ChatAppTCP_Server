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
    /// 返回值决定调用方后续动作：
    /// <list type="bullet">
    /// <item><see cref="ActorPostStatus.Accepted"/>：消息已入 Shard Ingress Ring；</item>
    /// <item><see cref="ActorPostStatus.MailboxFull"/>：FIFO Mailbox 满，调用方应拒绝或关闭连接；</item>
    /// <item><see cref="ActorPostStatus.ShardOverloaded"/>：Shard Ingress Ring 满，背压；</item>
    /// <item><see cref="ActorPostStatus.ActorClosed"/>：Actor 已 Deactivate 或从未 Activate；</item>
    /// <item><see cref="ActorPostStatus.RuntimeStopping"/>：Runtime 已停止。</item>
    /// </list>
    /// </para>
    /// <para>
    /// 注意：Accepted 仅表示通过生产侧准入；若 Shard 消费时 Actor 数已达上限
    /// （MaxActiveActors / MaxActiveActorsPerShard），消息仍会被丢弃并计入
    /// <see cref="ActorRuntimeSnapshot.TotalActiveActorAdmissionRejected"/>。
    /// </para>
    /// </summary>
    ActorPostStatus TryTell(in TKey key, in TMessage message);

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
