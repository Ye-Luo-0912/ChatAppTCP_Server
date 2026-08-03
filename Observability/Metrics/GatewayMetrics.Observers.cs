using System.Diagnostics.Metrics;

namespace ChatApp.TcpGateway.Observability.Metrics;

/// <summary>
/// <see cref="GatewayMetrics"/> 的 ObservableGauge / ObservableCounter 注册方法。
/// <para>
/// 这些方法在网关启动时由 <c>TcpGatewayService</c> 构造函数调用一次，将 BCL provider 委托
/// 绑定到 <see cref="Meter"/> 仪表上。Instrument 生命周期与 Meter 绑定，无需单独释放；
/// 调用方须持有 provider 委托引用以避免 GC 回收（委托捕获的闭包在 scrape 时被回调）。
/// </para>
/// <para>
/// 与主文件中业务计数方法（<c>ConnectionAccepted</c> 等 Counter.Add 热路径）分离，
/// 使主文件聚焦在同步计数 API，本文件聚焦在拉模式可观测性接线。
/// </para>
/// </summary>
public sealed partial class GatewayMetrics
{
    /// <summary>
    /// 八.4：注册心跳队列 ObservableGauge——queue.depth 与 queue.oldest_age。
    /// 调用方须持有 provider 委托引用避免 GC 回收（与 RegisterInboundBudgetObservers 同模式）。
    /// </summary>
    public void RegisterHeartbeatQueueObservers(
        Func<int> queueDepthProvider,
        Func<double> oldestAgeMsProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.heartbeat.queue.depth",
            () => queueDepthProvider(),
            unit: "{work_items}",
            description: "心跳刷新队列当前待处理工作项数。逼近 Channel 容量(WorkerCount×4)时 tick 循环阻塞。");
        _meter.CreateObservableGauge(
            "gateway.heartbeat.queue.oldest_age",
            () => oldestAgeMsProvider(),
            unit: "ms",
            description: "队列中最老待处理项的排队年龄。持续增长超过 LeaseTtl 安全余量时告警。");
    }

    // ===== 入站预算可观测性 =====
    //
    // 通过 ObservableGauge 暴露 GlobalInboundBudget / GlobalOutboundBudget 的当前值。
    // Instrument 生命周期与 Meter 绑定，无需单独释放；调用方须持有 provider 委托引用以避免 GC 回收。

    /// <summary>
    /// 注册入站预算 ObservableGauge：committed（已预留）字节数、max 上限。
    /// 调用方须持有 provider 委托的引用，避免委托被 GC 回收导致观察失败。
    /// </summary>
    public void RegisterInboundBudgetObservers(
        Func<long> committedBytesProvider,
        Func<long> maxBytesProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.inbound.committed.bytes",
            () => committedBytesProvider(),
            unit: "By",
            description: "已向 GlobalInboundBudget 预留的入站字节数（含 Pipe segment + lane 复制/池化 payload）。");
        _meter.CreateObservableGauge(
            "gateway.inbound.max.bytes",
            () => maxBytesProvider(),
            unit: "By",
            description: "GlobalInboundBudget 上限，对应 TcpGatewayOptions.GlobalMaxInboundBufferedBytes。");
    }

    /// <summary>
    /// 注册出站预算 ObservableGauge：committed（已入队）字节数、max 上限。
    /// </summary>
    public void RegisterOutboundBudgetObservers(
        Func<long> committedBytesProvider,
        Func<long> maxBytesProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.outbound.committed.bytes",
            () => committedBytesProvider(),
            unit: "By",
            description: "全局出站队列当前暂存字节数。");
        _meter.CreateObservableGauge(
            "gateway.outbound.max.bytes",
            () => maxBytesProvider(),
            unit: "By",
            description: "全局出站队列字节上限，对应 TcpGatewayOptions.GlobalMaxOutboundQueuedBytes。");
    }

    /// <summary>
    /// 注册进程级与派生资源 ObservableGauge，用于观测 GlobalInboundBudget 与物理内存的差距。
    /// <para>
    /// GlobalInboundBudget 只跟踪已 <c>TryReserve</c> 的字节数，但 Fill 路径在
    /// <c>socket.ReceiveAsync</c> 返回前已向 Pipe 申请 segment，此部分未计入预算；
    /// ArrayPool/MemoryPool 的池化余量与对象开销也不在 committed 内。
    /// 通过 working set 与 committed 的差值（unaccounted）可观测这部分"隐藏"内存，
    /// 避免将 committed 误用为物理内存硬上限。
    /// </para>
    /// </summary>
    /// <param name="workingSetBytesProvider">进程 WorkingSet64（物理内存）。</param>
    /// <param name="activeSessionsProvider">当前活跃连接数。</param>
    /// <param name="inboundCommittedBytesProvider">GlobalInboundBudget.CurrentBytes。</param>
    public void RegisterResourceObservers(
        Func<long> workingSetBytesProvider,
        Func<int> activeSessionsProvider,
        Func<long> inboundCommittedBytesProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.process.working_set.bytes",
            () => workingSetBytesProvider(),
            unit: "By",
            description: "进程物理内存 WorkingSet64，用于与 GlobalInboundBudget.committed 对比观测隐藏内存。");

        _meter.CreateObservableGauge(
            "gateway.inbound.avg_per_session.bytes",
            () =>
            {
                var sessions = activeSessionsProvider();
                return sessions > 0
                    ? inboundCommittedBytesProvider() / sessions
                    : 0;
            },
            unit: "By",
            description: "每连接平均已提交入站字节数 = committed / active_sessions，用于估算单连接内存成本。");

        _meter.CreateObservableGauge(
            "gateway.inbound.unaccounted.bytes",
            () => Math.Max(0, workingSetBytesProvider() - inboundCommittedBytesProvider()),
            unit: "By",
            description: "WorkingSet 与 committed 的差值，包含 Pipe 未计入 segment、池化余量、对象开销等隐藏内存。");
    }

    /// <summary>
    /// 注册 Runtime V2 共享执行器 ObservableGauge：DeadlineWheel 活跃 deadline 数、
    /// OnDemandSendPump ready queue 深度与累计调度次数。
    /// <para>
    /// provider 为 null 时对应指标跳过注册：PersistentSendLoop 模式下 OutboundPumpCoordinator 为 null，
    /// DeadlineWheel 在测试场景下也可为 null。调用方须持有 provider 委托引用避免 GC。
    /// </para>
    /// </summary>
    /// <param name="activeDeadlinesProvider">DeadlineWheel.ActiveDeadlineCount。</param>
    /// <param name="outboundPumpReadyQueueProvider">OutboundPumpCoordinator.ReadyQueueCount（OnDemandSendPump 模式）。</param>
    /// <param name="outboundPumpTotalScheduledProvider">OutboundPumpCoordinator.TotalScheduled 累计调度次数。</param>
    /// <param name="outboundPumpWorkerCountProvider">OutboundPumpCoordinator.WorkerCount。</param>
    public void RegisterRuntimeV2Observers(
        Func<long>? activeDeadlinesProvider,
        Func<long>? outboundPumpReadyQueueProvider,
        Func<long>? outboundPumpTotalScheduledProvider,
        Func<int>? outboundPumpWorkerCountProvider,
        Func<int>? sendTimeoutActiveSendersProvider = null,
        Func<int>? frameAssemblyActiveProvider = null)
    {
        if (activeDeadlinesProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.deadline_wheel.active_deadlines",
                () => activeDeadlinesProvider(),
                unit: "{deadlines}",
                description: "全局 DeadlineWheel 当前活跃 deadline 数（仅 Auth/Idle 超时；发送超时已迁移到 SendTimeoutTracker；帧装配超时已迁移到 FrameAssemblyTimeoutTracker）。");
        }

        if (sendTimeoutActiveSendersProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.send_timeout.active_senders",
                () => sendTimeoutActiveSendersProvider(),
                unit: "{sessions}",
                description: "SendTimeoutTracker 当前活跃发送方数（正在执行 Socket.SendAsync 的 Session）。空闲时为 0。");
        }

        if (frameAssemblyActiveProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.frame_assembly.active",
                () => frameAssemblyActiveProvider(),
                unit: "{sessions}",
                description: "FrameAssemblyTimeoutTracker 当前正在装配不完整帧的 Session 数。空闲时为 0。");
        }

        if (outboundPumpReadyQueueProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.outbound_pump.ready_queue.depth",
                () => outboundPumpReadyQueueProvider(),
                unit: "{sessions}",
                description: "OnDemandSendPump 模式下 ready queue 中待 pump 的 session 数。PersistentSendLoop 模式下不注册此指标。");
        }

        if (outboundPumpTotalScheduledProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.outbound_pump.total_scheduled",
                () => outboundPumpTotalScheduledProvider(),
                unit: "{schedules}",
                description: "OnDemandSendPump 模式下累计成功调度的 session 次数（含重新入队的轮转）。用于 A/B 评估唤醒频率。");
        }

        if (outboundPumpWorkerCountProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.outbound_pump.worker_count",
                () => outboundPumpWorkerCountProvider(),
                unit: "{workers}",
                description: "OnDemandSendPump 模式下共享出站 worker 池大小。");
        }
    }

    /// <summary>
    /// 注册 Ephemeral ActorRuntime 的低基数运行时指标。
    /// Observability 只接收 BCL provider，不依赖 ActorRuntime 项目。
    /// </summary>
    public void RegisterActorRuntimeObservers(
        Func<long> activeActorsProvider,
        Func<long> busyActorsProvider,
        Func<long> pendingIngressProvider,
        Func<long> pendingMailboxProvider,
        Func<int> pendingAsyncProvider,
        Func<long> totalProcessedProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.actor.active",
            () => activeActorsProvider(),
            unit: "{actors}");
        _meter.CreateObservableGauge(
            "gateway.actor.busy",
            () => busyActorsProvider(),
            unit: "{actors}");
        _meter.CreateObservableGauge(
            "gateway.actor.ingress.pending",
            () => pendingIngressProvider(),
            unit: "{messages}");
        _meter.CreateObservableGauge(
            "gateway.actor.mailbox.pending",
            () => pendingMailboxProvider(),
            unit: "{messages}");
        _meter.CreateObservableGauge(
            "gateway.actor.async.pending",
            () => pendingAsyncProvider(),
            unit: "{operations}");
        _meter.CreateObservableGauge(
            "gateway.actor.messages.processed",
            () => totalProcessedProvider(),
            unit: "{messages}");
    }

    /// <summary>
    /// 注册 Specialized Typing Actor 的扩展运行时指标。
    /// 仅在 UseTypingActorPipeline=true 时调用，补充基础 <c>gateway.actor.*</c> 指标未覆盖的领域维度。
    /// <para>
    /// 指标列表：
    /// <list type="bullet">
    /// <item><c>gateway.typing_actor.active</c>：活跃 Typing Actor 数；</item>
    /// <item><c>gateway.typing_actor.busy</c>：等待异步授权的 Actor 数；</item>
    /// <item><c>gateway.typing_actor.ingress.pending</c>：Shard Ingress 待消费消息数；</item>
    /// <item><c>gateway.typing_actor.replaced</c>：LatestOnly Mailbox 合并丢弃累计数；</item>
    /// <item><c>gateway.typing_actor.admission_rejected</c>：MaxActiveActors 拒绝激活累计数；</item>
    /// <item>deadlines/activations/deactivations/mailbox_full/shard_overloaded/async.*：补充领域维度。</item>
    /// </list>
    /// </para>
    /// </summary>
    public void RegisterTypingActorRuntimeObservers(
        Func<long> activeActorsProvider,
        Func<long> busyActorsProvider,
        Func<long> ingressPendingProvider,
        Func<long> replacedProvider,
        Func<long> admissionRejectedProvider,
        Func<long> pendingDeadlinesProvider,
        Func<long> activationsProvider,
        Func<long> deactivationsProvider,
        Func<long> mailboxFullProvider,
        Func<long> shardOverloadedProvider,
        Func<long> asyncSubmittedProvider,
        Func<long> asyncCompletedProvider,
        Func<long> asyncRejectedProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.typing_actor.active",
            () => activeActorsProvider(),
            unit: "{actors}",
            description: "Specialized Typing Actor Runtime 当前活跃 Actor 数。");
        _meter.CreateObservableGauge(
            "gateway.typing_actor.busy",
            () => busyActorsProvider(),
            unit: "{actors}",
            description: "Specialized Typing Actor Runtime 当前等待异步授权 Completion 的 Actor 数。");
        _meter.CreateObservableGauge(
            "gateway.typing_actor.ingress.pending",
            () => ingressPendingProvider(),
            unit: "{messages}",
            description: "Specialized Typing Actor 各 Shard Ingress Ring 待消费消息总数。");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.replaced",
            () => replacedProvider(),
            unit: "{messages}",
            description: "LatestOnly Mailbox 因新消息替换旧消息的累计丢弃数，反映快速状态变更合并频率。");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.admission_rejected",
            () => admissionRejectedProvider(),
            unit: "{actors}",
            description: "因 MaxActiveActors/MaxActiveActorsPerShard 上限被拒绝的激活累计数。");
        _meter.CreateObservableGauge(
            "gateway.typing_actor.deadlines.pending",
            () => pendingDeadlinesProvider(),
            unit: "{deadlines}",
            description: "Specialized Typing Actor 当前已注册未触发的 deadline 数。");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.activations",
            () => activationsProvider(),
            unit: "{actors}");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.deactivations",
            () => deactivationsProvider(),
            unit: "{actors}");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.mailbox_full",
            () => mailboxFullProvider(),
            unit: "{messages}");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.shard_overloaded",
            () => shardOverloadedProvider(),
            unit: "{messages}");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.async.submitted",
            () => asyncSubmittedProvider(),
            unit: "{operations}");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.async.completed",
            () => asyncCompletedProvider(),
            unit: "{operations}");
        _meter.CreateObservableCounter(
            "gateway.typing_actor.async.rejected",
            () => asyncRejectedProvider(),
            unit: "{operations}");
    }

    /// <summary>
    /// 注册 Typing 授权 I/O DomainWorkLane 的低基数运行时指标。
    /// <para>
    /// 指标列表：
    /// <list type="bullet">
    /// <item><c>gateway.typing_auth.queued</c>：当前排队等待执行的授权操作数；</item>
    /// <item><c>gateway.typing_auth.inflight</c>：当前正在执行的授权操作数；</item>
    /// <item><c>gateway.typing_auth.rejected</c>：因 Lane 队列满被拒绝的累计提交数；</item>
    /// <item><c>gateway.typing_auth.timeout</c>：因操作超时被取消的累计数。</item>
    /// </list>
    /// </para>
    /// <para>
    /// 授权耗时直方图通过 <see cref="TypingAuthCompleted"/> 实例方法记录（热路径同步 Add），
    /// 而非 ObservableGauge，避免每次 scrape 触发额外计算。
    /// </para>
    /// </summary>
    public void RegisterTypingAuthLaneObservers(
        Func<int> queuedProvider,
        Func<int> inflightProvider,
        Func<long> rejectedProvider,
        Func<long> timeoutProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.typing_auth.queued",
            () => queuedProvider(),
            unit: "{operations}",
            description: "Typing 授权 DomainWorkLane 当前排队待执行的授权操作数。");
        _meter.CreateObservableGauge(
            "gateway.typing_auth.inflight",
            () => inflightProvider(),
            unit: "{operations}",
            description: "Typing 授权 DomainWorkLane 当前正在执行的授权操作数。");
        _meter.CreateObservableCounter(
            "gateway.typing_auth.rejected",
            () => rejectedProvider(),
            unit: "{operations}",
            description: "Typing 授权 DomainWorkLane 因队列满被拒绝的累计提交数。");
        _meter.CreateObservableCounter(
            "gateway.typing_auth.timeout",
            () => timeoutProvider(),
            unit: "{operations}",
            description: "Typing 授权 DomainWorkLane 因操作超时被取消的累计数。");
    }

    /// <summary>
    /// 记录单次 Typing 授权操作耗时。在授权 I/O 完成后（无论成功/拒绝/失败）调用。
    /// </summary>
    /// <param name="durationMs">操作耗时（毫秒）。</param>
    /// <param name="authorized">是否授权通过。通过 tag 区分缓存命中与远程查询耗时分布。</param>
    public void TypingAuthCompleted(double durationMs, bool authorized)
    {
        _typingAuthDuration.Record(
            durationMs,
            new KeyValuePair<string, object?>("outcome", authorized ? "allowed" : "denied"));
    }
}
