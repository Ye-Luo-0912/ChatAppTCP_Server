using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Gateway.Configuration;

public sealed class TcpGatewayOptions
{
    public const string SectionName = "TcpGateway";

    public string ListenAddress { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 8888;
    public int ListenBacklog { get; set; } = 512;
    public int MaxConnections { get; set; } = 10_000;
    public int ReceiveBufferSize { get; set; } = 4 * 1024;

    /// <summary>
    /// DirectSocket 动态接收缓冲区：新连接的初始缓冲区大小（字节）。
    /// <para>
    /// 心跳/小帧连接使用小缓冲区减少内存占用；检测到连续大帧时自动升级到
    /// <see cref="ReceiveBufferMaxSize"/>，长期空闲后降级回此值。
    /// 仅在 <see cref="InboundTransportMode.DirectSocket"/> 模式下生效。
    /// </para>
    /// </summary>
    public int ReceiveBufferInitialSize { get; set; } = 1024;

    /// <summary>
    /// DirectSocket 动态接收缓冲区：升级后的最大缓冲区大小（字节）。
    /// <para>
    /// 当帧无法容纳在当前缓冲区但可容纳在最大缓冲区时，自动升级。
    /// 超过此大小的帧仍通过单独租用 Payload 缓冲区处理（已有逻辑）。
    /// </para>
    /// </summary>
    public int ReceiveBufferMaxSize { get; set; } = 4 * 1024;

    /// <summary>
    /// DirectSocket 动态接收缓冲区：空闲多少秒后降级到 <see cref="ReceiveBufferInitialSize"/>。
    /// <para>
    /// 长连接在空闲期后释放大缓冲区槽位，减少 ArrayPool 压力。
    /// 设为 <see cref="TimeSpan.Zero"/> 禁用降级。
    /// </para>
    /// </summary>
    public TimeSpan ReceiveBufferDowngradeIdleTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// DirectSocket 帧装配 deadline：不完整 Header 超时后关闭连接（秒）。
    /// <para>
    /// 防御慢速攻击：客户端逐字节发送 Header，消耗连接资源。
    /// 设为 <see cref="TimeSpan.Zero"/> 禁用（仅由 IdleTimeout 兜底）。
    /// </para>
    /// </summary>
    public TimeSpan HeaderAssemblyTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// DirectSocket 帧装配 deadline：不完整 Payload 超时后关闭连接（秒）。
    /// <para>
    /// 防御慢速攻击：客户端逐字节发送 Payload，消耗连接资源。
    /// 设为 <see cref="TimeSpan.Zero"/> 禁用（仅由 IdleTimeout 兜底）。
    /// </para>
    /// </summary>
    public TimeSpan PayloadAssemblyTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 入站读取模式。DirectSocket 通过固定池化缓冲区直接执行增量协议解析；
    /// Pipelines 保留为 A/B 基线与快速回退路径。
    /// </summary>
    public InboundTransportMode InboundTransportMode { get; set; } =
        InboundTransportMode.DirectSocket;

    public long PipePauseWriterThreshold { get; set; } = 160 * 1024;
    public long PipeResumeWriterThreshold { get; set; } = 80 * 1024;
    public int OutboundQueueCapacity { get; set; } = 256;
    public long MaxOutboundQueuedBytes { get; set; } = 256 * 1024;

    /// <summary>
    /// P0-5：出站队列实现模式。
    /// <para>
    /// 默认 <see cref="OutboundQueueMode.BoundedChannel"/>（成熟实现，生产默认）。
    /// 切换为 <see cref="OutboundQueueMode.LazySegmented"/> 后使用自定义 MPSC 队列
    /// （空闲连接零段分配），仅供 A/B 对照；在完整 Transport Matrix 通过前不应作为生产默认。
    /// </para>
    /// </summary>
    public OutboundQueueMode OutboundQueueMode { get; set; } =
        OutboundQueueMode.BoundedChannel;
    public TimeSpan AuthenticationTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(90);
    public TimeSpan HeartbeatScanInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxPacketsPerSecond { get; set; } = 200;

    /// <summary>
    /// 出站发送模式：是否为每连接保留永久 SendLoop Task。
    /// <para>
    /// 默认 <see cref="OutboundSendMode.PersistentSendLoop"/>：作为 A/B 对照基线，
    /// 行为与历史版本一致。切换为 <see cref="OutboundSendMode.OnDemandSendPump"/> 后，
    /// 空闲连接不再保留 SendLoop Task，改由全局共享 worker 池按需 pump。
    /// 切换为 <see cref="OutboundSendMode.PerSessionDrain"/> 后，每连接按需启动自有 async drain，
    /// 慢 Socket 不占用全局 Worker 名额。
    /// </para>
    /// <para>
    /// 切换不影响 wire 协议与出站 FIFO/ephemeral mailbox 语义，仅改变驱动 Task 模型。
    /// 上线前应通过负载测试（10k 空闲 + 活跃聊天混合）验证工作集、alloc/sec、p95/p99。
    /// </para>
    /// </summary>
    public OutboundSendMode OutboundSendMode { get; set; } = OutboundSendMode.PersistentSendLoop;

    /// <summary>
    /// OnDemandSendPump 模式下，共享出站 worker 池的 worker 数量。
    /// <para>
    /// 默认 0：运行时按 <c>Math.Max(2, Environment.ProcessorCount)</c> 推导。
    /// 显式设为正数时覆盖推导值。worker 数过少会导致慢 socket 拖累跨连接公平性；
    /// 过多会增加调度开销。建议与 OrderedWrite/Query 执行器 worker 数对齐。
    /// </para>
    /// </summary>
    public int OnDemandSendWorkerCount { get; set; }

    /// <summary>
    /// OnDemandSendPump 模式下，单个 worker 一次 pump 处理的最大帧数（burst 上限）。
    /// <para>
    /// 默认 16：足够批量化以摊薄调度成本，又不至于让单连接独占 worker 饿死后续会话。
    /// 达到 burst 上限后 worker 将当前会话重新入队 ready queue（公平轮转），再处理下一个会话。
    /// </para>
    /// </summary>
    public int OnDemandSendBurstLimit { get; set; } = 16;

    /// <summary>
    /// 心跳分桶数量。每个 tick 只扫描一个桶，将原本每
    /// <see cref="HeartbeatScanInterval"/> 全量扫描的脉冲打散为 N 次小扫描。
    /// <para>
    /// 默认 30：与默认 30s 扫描间隔配合，每秒扫描一个桶，Redis 负载从锯齿脉冲变为平滑流量。
    /// 设为 1 时退化为全量扫描（兼容旧行为，仅用于对照测试）。
    /// </para>
    /// </summary>
    public int HeartbeatBucketCount { get; set; } = 30;

    /// <summary>
    /// 心跳刷新并发上限（设备租约 + Presence 刷新共享）。
    /// <para>
    /// 默认 32：足够吸收单桶内的刷新任务而不压垮 Redis。
    /// 取代原 HeartbeatCoordinator 中硬编码的 MaxRefreshConcurrency 常量。
    /// </para>
    /// </summary>
    public int HeartbeatRefreshConcurrency { get; set; } = 32;

    /// <summary>
    /// 心跳刷新 jitter 比率（0~1）。每个刷新任务在提交前追加
    /// <c>HeartbeatScanInterval / BucketCount * JitterRatio</c> 范围内的随机延迟，
    /// 避免同桶内任务同步触发 Redis 造成抖动。
    /// <para>
    /// 默认 0.2（±20% 的桶间隔）。设为 0 时禁用 jitter。
    /// </para>
    /// <para>
    /// <b>已弃用</b>：HeartbeatCoordinator 不再消费此值。负载分散改由固定 Worker 池并发数
    /// （<see cref="HeartbeatRefreshConcurrency"/>）+ Channel 背压自然实现。
    /// 保留属性与校验仅为兼容已有配置文件，新配置无需设置。
    /// </para>
    /// </summary>
    public double HeartbeatRefreshJitterRatio { get; set; } = 0.2;

    /// <summary>
    /// 每连接入站帧字节速率上限（含 10 字节包头），按 1 秒滑动窗口计数。
    /// </summary>
    public long MaxInboundBytesPerSecond { get; set; } = 512 * 1024;

    /// <summary>
    /// 单帧 payload 上限；须 ≤ <see cref="PacketProtocol.MaxPayloadSize"/>。
    /// 在反序列化与 NATS 投递前拒绝超限帧。
    /// </summary>
    public int MaxInboundPayloadBytes { get; set; } = PacketProtocol.MaxPayloadSize;

    /// <summary>ChatMessage.attachmentIds 最大元素数（早检 + 语义校验共用）。</summary>
    public int MaxChatAttachments { get; set; } = ChatMessageLimits.MaxAttachments;

    /// <summary>
    /// 同 DeviceIdHash 重复登录时踢掉本机旧会话，并向事件总线发布 SessionRevoked。
    /// </summary>
    public bool ReplaceSameDeviceSession { get; set; } = true;

    /// <summary>
    /// Presence/Typing：本机扇出 + NATS Core ephemeral 跨网关（不进 Outbox）。
    /// 默认关闭；开启前需 Server 侧 PresenceAuthorizeWorker（好友鉴权）与 Redis 全局在线键。
    /// </summary>
    public bool EnableEphemeralPresenceAndTyping { get; set; }

    /// <summary>
    /// Presence ZSET 过期成员清理周期。
    /// <para>
    /// 热路径（SetOnline/SetOffline/Refresh/IsOnline/GetOnlineMany）不做 ZREMRANGEBYSCORE，
    /// 由后台服务按此周期调用 <c>IGlobalPresenceStore.RunMaintenanceAsync</c> 回收崩溃实例残留成员。
    /// 默认 5 分钟；设为 <see cref="TimeSpan.Zero"/> 时禁用维护（仅用于测试）。
    /// </para>
    /// </summary>
    public TimeSpan PresenceMaintenanceInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Realtime 事件消费分区数。
    /// <para>
    /// 按 <c>TargetUserId % PartitionCount</c> 将事件路由到固定分区，每分区单消费者保证
    /// 同一用户的局部顺序；跨分区并行消费提升吞吐。默认 1（串行，向后兼容）；
    /// 建议生产环境设为 4～8，根据 CPU 核数与群聊 fanout 负载调整。
    /// </para>
    /// <para>
    /// 分区数为 1 时保持原有串行消费语义；&gt;1 时启用分区并行消费。
    /// </para>
    /// </summary>
    public int RealtimeEventPartitionCount { get; set; } = 1;

    // 全局内存预算与过载保护配置。

    /// <summary>
    /// 全局出站队列字节预算上限（所有连接出站队列字节总和）。
    /// <para>
    /// 默认 512 MiB。超过时新出站帧被拒绝（优先丢弃 Typing/Presence），防止慢消费者耗尽内存。
    /// </para>
    /// </summary>
    public long GlobalMaxOutboundQueuedBytes { get; set; } = 512 * 1024 * 1024;

    /// <summary>
    /// 全局入站缓冲字节预算上限（所有连接 Socket/Pipe 暂存 + lane 复制/池化 payload 总和）。
    /// <para>
    /// 默认 1 GiB。由 <c>GlobalInboundBudget</c> 在接收/写入 Pipe/复制到调度缓冲区前预留，
    /// 消费或断开时释放；超限时关闭连接以背压，避免配置上限形同虚设。
    /// 单连接 Pipe 暂停阈值仍由 <see cref="PipePauseWriterThreshold"/> 控制。
    /// </para>
    /// </summary>
    public long GlobalMaxInboundBufferedBytes { get; set; } = 1024 * 1024 * 1024;

    /// <summary>
    /// 未认证连接数上限。超过时新连接立即断开，防止认证前资源耗尽。
    /// <para>
    /// 默认为 <see cref="MaxConnections"/> 的 10%，不小于 100。
    /// </para>
    /// </summary>
    public int MaxUnauthenticatedConnections { get; set; } = 1_000;

    /// <summary>
    /// 单 IP 并发连接数上限。超过时该 IP 的新连接被拒绝。
    /// </summary>
    public int MaxConnectionsPerIp { get; set; } = 100;

    /// <summary>
    /// 单 IP 在滑动窗口内的最大认证失败次数。超过时该 IP 的新连接被拒绝。
    /// <para>
    /// 窗口长度为 <see cref="AuthenticationRateWindow"/>。
    /// </para>
    /// </summary>
    public int MaxAuthenticationAttemptsPerIp { get; set; } = 20;

    /// <summary>
    /// 每 IP 认证失败计数滑动窗口长度。
    /// </summary>
    public TimeSpan AuthenticationRateWindow { get; set; } = TimeSpan.FromMinutes(1);

    // 每会话命令调度器容量配置。

    /// <summary>
    /// 每会话 OrderedWrite lane 的有界 Channel 容量。
    /// <para>
    /// Chat/Receipt/Edit/Recall 等写命令在此排队，单消费者保持顺序。
    /// 满时读循环等待（自然背压），级联到 TCP 流控。
    /// 默认 64，足以吸收短暂突发而不占用过多内存。
    /// </para>
    /// </summary>
    public int CommandSchedulerOrderedWriteCapacity { get; set; } = 64;

    /// <summary>
    /// 每会话 Query lane 的有界 Channel 容量。
    /// <para>
    /// History/List/Sync 等查询命令在此排队，与 OrderedWrite 并行处理。
    /// 默认 16，查询响应较慢但单客户端并发查询需求有限。
    /// </para>
    /// </summary>
    public int CommandSchedulerQueryCapacity { get; set; } = 16;

    /// <summary>
    /// 每会话 Ephemeral lane 的有界 Channel 容量。
    /// <para>
    /// Typing 等瞬态命令在此排队，DropOldest 模式只保留最新帧。
    /// 默认 4：Typing 频率受 _typingFanout 协调器限频，小容量足以吸收突发且快速丢弃过期状态。
    /// </para>
    /// </summary>
    public int CommandSchedulerEphemeralCapacity { get; set; } = 4;

    /// <summary>
    /// 使用轻量 ActorRuntime 驱动 Ephemeral 入站命令。关闭时回退到
    /// SessionCommandExecutor，便于灰度和 A/B。
    /// </summary>
    public bool UseActorRuntimeForEphemeralCommands { get; set; }

    /// <summary>
    /// 解析当前 Ephemeral 调度模式。优先使用 <see cref="EphemeralPipelineMode"/>，
    /// 布尔标志保留作为配置入口与 A/B 切换开关。
    /// <para>
    /// 调用方应使用此属性而非直接读取布尔标志，确保 Disabled 模式被正确识别。
    /// </para>
    /// </summary>
    public EphemeralPipelineMode ResolveEphemeralPipelineMode() =>
        UseActorRuntimeForEphemeralCommands
            ? EphemeralPipelineMode.GenericActor
            : EphemeralPipelineMode.Legacy;

    /// <summary>
    /// 启用 Typing 领域 Actor：TCP Read 路径直接解析 TypingNotify 并路由到
    /// LatestOnly Mailbox 的领域 Actor，不创建通用 SessionCommand。
    /// 需要 <see cref="UseActorRuntimeForEphemeralCommands"/> 为 true。
    /// 关闭时 TypingNotify 回退到 EphemeralCommandPipeline 通用路径。
    /// </summary>
    public bool UseTypingActorPipeline { get; set; }

    /// <summary>Ephemeral Actor Shard 数；0 表示按 CPU 自动选择 2 的幂。</summary>
    public int EphemeralActorShardCount { get; set; }

    /// <summary>每个 Ephemeral Actor Shard 的普通 Ingress 容量，必须为 2 的幂。</summary>
    public int EphemeralActorIngressCapacity { get; set; } = 4096;

    /// <summary>Ephemeral Actor 异步 I/O 并发数；0 表示 CPU×2。</summary>
    public int EphemeralActorAsyncConcurrency { get; set; }

    /// <summary>Ephemeral Actor 无消息后的回收时间。</summary>
    public TimeSpan EphemeralActorIdleTimeout { get; set; } =
        TimeSpan.FromSeconds(30);

    /// <summary>单条 Ephemeral Actor 后端操作超时。</summary>
    public TimeSpan EphemeralActorOperationTimeout { get; set; } =
        TimeSpan.FromSeconds(10);

    // 协议握手与连接恢复配置

    /// <summary>
    /// 当前部署接受的最低客户端协议版本。可在协议淘汰期逐步提高，
    /// 但必须位于编译期支持区间
    /// [<see cref="PacketProtocol.MinProtocolVersion"/>,
    /// <see cref="PacketProtocol.CurrentProtocolVersion"/>]。
    /// </summary>
    public ushort MinimumClientProtocolVersion { get; set; } =
        PacketProtocol.MinProtocolVersion;

    /// <summary>
    /// 是否要求客户端在认证前先发送 ClientHello 握手。
    /// <para>
    /// true（默认）：连接建立后客户端必须先发 ClientHello，服务端回 ServerHello。
    /// 未握手直接发 AuthenticationRequest 将被拒绝（ProtocolViolation）。
    /// false：握手可选，兼容旧客户端。
    /// </para>
    /// </summary>
    public bool RequireClientHello { get; set; } = true;

    /// <summary>
    /// 是否启用断线重连（ResumeToken 机制）。
    /// <para>
    /// 开启后认证成功时颁发 ResumeToken，客户端断线后短时间内可凭 Token 恢复会话。
    /// 关闭时 ServerHello.resumeSupported = false，客户端忽略 ResumeToken 字段。
    /// </para>
    /// </summary>
    public bool EnableResume { get; set; } = true;

    /// <summary>
    /// ResumeToken 有效期。客户端断线后须在此时间内重连。
    /// 默认 30 秒：足够客户端检测断线并重连，又不至于过长占用会话资源。
    /// </summary>
    public TimeSpan ResumeTokenTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// P1-C：Resume 路径在 Redis 不可用时的 fail-mode。
    /// <para>
    /// 默认 <see cref="RedisFailMode.FailClosed"/>：TakeOver/代次校验依赖不可用时
    /// 拒绝恢复、回滚本地状态、关闭连接，要求完整认证。Same-device fencing 属于安全不变量。
    /// </para>
    /// <para>
    /// 切换为 <see cref="RedisFailMode.FailOpen"/> 后：跳过 TakeOver/代次校验，
    /// 继续恢复会话。旧 Transport 不被吊销，依赖租约 TTL 自然释放。
    /// 仅用于降级模式，需运维明确评估风险。
    /// </para>
    /// </summary>
    public RedisFailMode ResumeRedisFailMode { get; set; } = RedisFailMode.FailClosed;

    /// <summary>
    /// P1-C：正常 Authentication 路径在 Redis 不可用时的 fail-mode。
    /// <para>
    /// 默认 <see cref="RedisFailMode.FailClosed"/>：与 Resume 路径一致，
    /// TakeOver 依赖不可用时拒绝认证、回滚本地状态、关闭连接。
    /// </para>
    /// <para>
    /// 切换为 <see cref="RedisFailMode.FailOpen"/> 后：TakeOver 失败仅记录日志，
    /// 继续完成认证（best-effort）。旧连接依赖本机 TakeOverSameDevice + 租约 TTL 自然失效。
    /// 此为 P1-C 之前的旧行为，保留以兼容需要最大可用性的部署。
    /// </para>
    /// </summary>
    public RedisFailMode AuthRedisFailMode { get; set; } = RedisFailMode.FailClosed;

    /// <summary>
    /// 服务端实例标识（128 位 GUID 的 32 字符十六进制表示）。
    /// <para>
    /// 配置加载场景传入持久化值；null 或空时启动时自动生成。
    /// 用于 ServerHello.serverDeviceId 和跨网关路由标识。
    /// </para>
    /// </summary>
    public string? ServerDeviceId { get; set; }

    /// <summary>
    /// 优雅停机时发送 GoAway 后等待客户端断开的超时。
    /// <para>
    /// 超时后服务端强制关闭连接。默认 5 秒，平衡客户端重连时间与服务端排空速度。
    /// </para>
    /// </summary>
    public TimeSpan GoAwayDrainTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public bool IsValid() =>
        System.Net.IPAddress.TryParse(ListenAddress, out _) &&
        Port is > 0 and <= ushort.MaxValue &&
        ListenBacklog > 0 &&
        MaxConnections > 0 &&
        ReceiveBufferSize >= 512 &&
        Enum.IsDefined(InboundTransportMode) &&
        PipeResumeWriterThreshold > 0 &&
        PipePauseWriterThreshold > PipeResumeWriterThreshold &&
        OutboundQueueCapacity > 0 &&
        MaxOutboundQueuedBytes >=
        PacketProtocol.HeaderSize + PacketProtocol.MaxPayloadSize &&
        AuthenticationTimeout > TimeSpan.Zero &&
        IdleTimeout > AuthenticationTimeout &&
        HeartbeatScanInterval > TimeSpan.Zero &&
        HeartbeatBucketCount > 0 &&
        HeartbeatRefreshConcurrency > 0 &&
        HeartbeatRefreshJitterRatio is >= 0 and <= 1 &&
        SendTimeout > TimeSpan.Zero &&
        MaxPacketsPerSecond > 0 &&
        MaxInboundBytesPerSecond > 0 &&
        MaxInboundPayloadBytes is > 0 and <= PacketProtocol.MaxPayloadSize &&
        MaxChatAttachments is > 0 and <= ChatMessageLimits.MaxAttachments &&
        GlobalMaxOutboundQueuedBytes > 0 &&
        GlobalMaxInboundBufferedBytes > 0 &&
        MaxUnauthenticatedConnections > 0 &&
        MaxConnectionsPerIp > 0 &&
        MaxAuthenticationAttemptsPerIp > 0 &&
        AuthenticationRateWindow > TimeSpan.Zero &&
        CommandSchedulerOrderedWriteCapacity > 0 &&
        CommandSchedulerQueryCapacity > 0 &&
        CommandSchedulerEphemeralCapacity > 0 &&
        EphemeralActorShardCount >= 0 &&
        (EphemeralActorShardCount == 0 ||
         (EphemeralActorShardCount & (EphemeralActorShardCount - 1)) == 0) &&
        EphemeralActorIngressCapacity > 0 &&
        (EphemeralActorIngressCapacity & (EphemeralActorIngressCapacity - 1)) == 0 &&
        EphemeralActorAsyncConcurrency >= 0 &&
        EphemeralActorIdleTimeout > TimeSpan.Zero &&
        EphemeralActorOperationTimeout > TimeSpan.Zero &&
        OnDemandSendWorkerCount >= 0 &&
        OnDemandSendBurstLimit > 0 &&
        ReceiveBufferInitialSize >= 512 &&
        ReceiveBufferMaxSize >= ReceiveBufferInitialSize &&
        ReceiveBufferDowngradeIdleTimeout >= TimeSpan.Zero &&
        HeaderAssemblyTimeout >= TimeSpan.Zero &&
        PayloadAssemblyTimeout >= TimeSpan.Zero &&
        MinimumClientProtocolVersion >= PacketProtocol.MinProtocolVersion &&
        MinimumClientProtocolVersion <= PacketProtocol.CurrentProtocolVersion &&
        ResumeTokenTtl > TimeSpan.Zero &&
        Enum.IsDefined(ResumeRedisFailMode) &&
        Enum.IsDefined(AuthRedisFailMode) &&
        GoAwayDrainTimeout > TimeSpan.Zero &&
        RealtimeEventPartitionCount > 0;
}
