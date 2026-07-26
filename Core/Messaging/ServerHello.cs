namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 服务端 → 客户端握手响应（PacketCommand.ServerHello = 4）。
/// 服务端在收到 ClientHello 后发送，宣告协商结果与服务端能力。
/// </summary>
public sealed class ServerHello
{
    /// <summary>协商后的协议版本（服务端选择的最大支持版本）。</summary>
    public ushort ProtocolVersion { get; set; } = 1;

    /// <summary>服务端能力位掩码（保留，当前未使用）。</summary>
    public uint FeatureBits { get; set; }

    /// <summary>
    /// 服务端实例标识（128 位 GUID 的 32 字符十六进制表示）。
    /// 用于跨网关路由、客户端亲和性、日志关联。
    /// </summary>
    public string ServerDeviceId { get; set; } = string.Empty;

    /// <summary>服务端当前时间戳（UTC 毫秒）。用于时钟偏移估算。</summary>
    public long ServerTimeMs { get; set; }

    /// <summary>
    /// 心跳间隔（毫秒）。客户端应按此间隔发送 Heartbeat 帧。
    /// 服务端 IdleTimeout 后无任何入站数据将关闭连接。
    /// </summary>
    public int HeartbeatIntervalMs { get; set; }

    /// <summary>
    /// 服务端接受的最大 Payload 大小（字节）。
    /// 客户端发送的任何帧 payload 不得超过此值。
    /// </summary>
    public int MaxPayloadBytes { get; set; }

    /// <summary>
    /// 是否支持断线重连（ResumeToken 机制）。
    /// false 时客户端应每次走完整认证流程。
    /// </summary>
    public bool ResumeSupported { get; set; }

    /// <summary>
    /// 协议格式标识。当前固定为 "json"。
    /// 未来可扩展为 "pb"（Protobuf）等。
    /// </summary>
    public string PayloadFormat { get; set; } = "json";
}
