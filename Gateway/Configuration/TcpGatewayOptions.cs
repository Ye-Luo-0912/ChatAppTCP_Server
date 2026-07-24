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
        MaxChatAttachments is > 0 and <= ChatMessageLimits.MaxAttachments;
}
