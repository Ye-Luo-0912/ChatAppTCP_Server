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
    /// </summary>
    ActorPostStatus TryTell(in TKey key, in TMessage message);

    /// <summary>
    /// 投递异步操作完成消息。Completion 使用独立高优先级槽，即使 Actor 处于
    /// Suspend 且普通 Mailbox 非空也会被优先调度。
    /// </summary>
    ActorPostStatus TryTellCompletion(
        in TKey key,
        uint generation,
        in TMessage message);

    /// <summary>启动所有 Shard Consumer Loop 与 AsyncOperationExecutor。重复调用幂等。</summary>
    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>停止 Runtime：取消所有 Loop、排空或丢弃 Mailbox、Deactivate 所有 Actor。</summary>
    ValueTask StopAsync(ActorStopMode mode, CancellationToken cancellationToken);

    /// <summary>获取当前 Runtime 的原子计数快照（不保证强一致），供宿主映射到 Metrics。</summary>
    ActorRuntimeSnapshot GetSnapshot();
}
