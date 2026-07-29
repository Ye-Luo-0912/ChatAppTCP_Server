namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Runtime 原子计数快照。宿主读取后映射成现有 GatewayMetrics。
/// 字段全部为快照值，不保证强一致——读取时各 Shard 可能正在变更。
/// </summary>
public readonly struct ActorRuntimeSnapshot
{
    /// <summary>活跃 Actor 数（已 Activate 未 Deactivate）。</summary>
    public long ActiveActors { get; init; }

    /// <summary>正在等待异步 Completion 的 Actor 数。</summary>
    public long BusyActors { get; init; }

    /// <summary>各 Shard Ingress Ring 中待消费消息总数。</summary>
    public long PendingIngress { get; init; }

    /// <summary>各 Actor Mailbox 中待处理消息总数（FIFO + LatestOnly）。</summary>
    public long PendingMailbox { get; init; }

    /// <summary>累计处理完成的消息总数。</summary>
    public long TotalProcessed { get; init; }

    /// <summary>累计因 Mailbox 满被拒绝的消息总数。</summary>
    public long TotalMailboxFull { get; init; }

    /// <summary>累计因 Shard Ingress 满被拒绝的消息总数。</summary>
    public long TotalShardOverloaded { get; init; }

    /// <summary>累计 Actor Deactivate 总数（按 reason 分类聚合）。</summary>
    public long TotalDeactivations { get; init; }

    /// <summary>累计 Actor Activate 总数。与 <see cref="TotalDeactivations"/> 共同反映 Actor Churn。</summary>
    public long TotalActivations { get; init; }

    /// <summary>累计因 MaxActiveActors / MaxActiveActorsPerShard 上限被拒绝的激活总数。</summary>
    public long TotalActiveActorAdmissionRejected { get; init; }

    /// <summary>
    /// 累计 LatestOnly Mailbox 因新消息替换旧消息而被丢弃的总数。
    /// 反映 typing=true→false 等快速状态变更的合并频率。
    /// </summary>
    public long TotalReplaced { get; init; }

    /// <summary>当前已注册但尚未触发的 deadline 数。</summary>
    public long PendingDeadlines { get; init; }

    /// <summary>AsyncOperationExecutor 当前排队或执行中的操作数。</summary>
    public int PendingAsyncOperations { get; init; }

    /// <summary>累计提交的异步操作总数。</summary>
    public long TotalAsyncOperationsSubmitted { get; init; }

    /// <summary>累计完成的异步操作总数。</summary>
    public long TotalAsyncOperationsCompleted { get; init; }

    /// <summary>累计因 Executor 队列满被拒绝的异步操作总数。</summary>
    public long TotalAsyncOperationsRejected { get; init; }
}
