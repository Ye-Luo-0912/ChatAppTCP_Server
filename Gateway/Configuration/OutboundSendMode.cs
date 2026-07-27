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
    OnDemandSendPump = 1
}
