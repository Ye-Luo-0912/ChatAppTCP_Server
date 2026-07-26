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
    public long PipePauseWriterThreshold { get; set; } = 160 * 1024;
    public long PipeResumeWriterThreshold { get; set; } = 80 * 1024;
    public int OutboundQueueCapacity { get; set; } = 256;
    public long MaxOutboundQueuedBytes { get; set; } = 256 * 1024;
    public TimeSpan AuthenticationTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(90);
    public TimeSpan HeartbeatScanInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxPacketsPerSecond { get; set; } = 200;

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

    // 全局内存预算与过载保护配置。

    /// <summary>
    /// 全局出站队列字节预算上限（所有连接出站队列字节总和）。
    /// <para>
    /// 默认 512 MiB。超过时新出站帧被拒绝（优先丢弃 Typing/Presence），防止慢消费者耗尽内存。
    /// </para>
    /// </summary>
    public long GlobalMaxOutboundQueuedBytes { get; set; } = 512 * 1024 * 1024;

    /// <summary>
    /// 全局入站缓冲字节预算上限（所有连接 Pipe 暂存 + lane 复制/池化 payload 总和）。
    /// <para>
    /// 默认 1 GiB。由 <c>GlobalInboundBudget</c> 在写入 Pipe / 复制到调度缓冲区前预留，
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

    // 协议握手与连接恢复配置

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
        PipeResumeWriterThreshold > 0 &&
        PipePauseWriterThreshold > PipeResumeWriterThreshold &&
        OutboundQueueCapacity > 0 &&
        MaxOutboundQueuedBytes >=
        PacketProtocol.HeaderSize + PacketProtocol.MaxPayloadSize &&
        AuthenticationTimeout > TimeSpan.Zero &&
        IdleTimeout > AuthenticationTimeout &&
        HeartbeatScanInterval > TimeSpan.Zero &&
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
        ResumeTokenTtl > TimeSpan.Zero &&
        GoAwayDrainTimeout > TimeSpan.Zero;
}
