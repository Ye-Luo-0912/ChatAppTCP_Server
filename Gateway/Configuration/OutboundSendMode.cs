namespace ChatApp.TcpGateway.Gateway.Configuration;

/// <summary>
/// 出站发送模式：控制每连接是否保留永久 SendLoop Task，或按需唤醒共享 worker 池。
/// <para>
/// 用于 A/B 对照测试，确定在目标负载下哪种模式总体成本更低。
/// </para>
/// </summary>
public enum OutboundSendMode
{
    /// <summary>
    /// 每连接一个永久 SendLoop Task，直接消费出站 Channel。
    /// <para>
    /// 优点：无唤醒延迟，实现简单，Channel 自然背压。
    /// 缺点：空闲连接也保留 1 个 Task（10k 连接 = 10k Task）。
    /// 这是当前默认行为，作为 A/B 对照基线。
    /// </para>
    /// </summary>
    PersistentSendLoop = 0,

    /// <summary>
    /// 无永久 SendLoop Task。入队时通过 CAS 唤醒共享 OutboundPumpCoordinator，
    /// 由全局 worker 池按公平规则轮转 pump。
    /// <para>
    /// 优点：空闲连接 0 出站 Task，仅 1 个挂起 Receive + Lifetime CTS。
    /// 缺点：每次唤醒有一次 ready-queue 调度延迟；活跃聊天场景频繁唤醒可能产生更多分配。
    /// </para>
    /// <para>
    /// 慢消费者隔离：SendTimeout（DeadlineWheel）关闭慢 socket；多 worker 并行；
    /// burst 限制防止单连接独占 worker。
    /// </para>
    /// </summary>
    OnDemandSendPump = 1,

    /// <summary>
    /// 无永久 SendLoop Task，也无共享 worker 池。每连接按需启动自有 async Drain。
    /// <para>
    /// 入队时 CAS Idle→Running 成功后启动 Session 自有 drain Task；
    /// drain 持续消费出站 Channel 直到队列空，然后 CAS Running→Idle 退出。
    /// Socket.SendAsync 的 continuation 天然恢复同一 drain，无需额外调度。
    /// </para>
    /// <para>
    /// 优点：
    /// <list type="bullet">
    /// <item>空闲连接 0 出站 Task（与 OnDemandSendPump 一致）；</item>
    /// <item>慢 Socket 不占用全局逻辑 Worker 名额（每连接独立 drain）；</item>
    /// <item>无 ready queue 调度延迟，无 burst 限制；</item>
    /// <item>同一 Session 天然保持唯一发送所有权（CAS 状态机保证）。</item>
    /// </list>
    /// </para>
    /// <para>
    /// 缺点：活跃聊天场景频繁启动/停止 drain Task 可能产生更多分配
    /// （可通过 drain 内连续消费多帧摊薄）。
    /// </para>
    /// <para>
    /// 慢消费者隔离：SendTimeoutTracker 扫描关闭慢 socket。
    /// </para>
    /// </summary>
    PerSessionDrain = 2
}
