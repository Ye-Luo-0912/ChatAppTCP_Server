namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 客户端 → 服务端握手请求（PacketCommand.ClientHello = 3）。
/// 连接建立后客户端应首先发送此帧，协商协议版本与能力。
/// </summary>
public sealed class ClientHello
{
    /// <summary>
    /// 客户端期望的协议版本。当前服务端固定为 1。
    /// 服务端不支持时返回 <see cref="ProtocolErrorCode.UnsupportedVersion"/> 错误帧。
    /// </summary>
    public ushort ProtocolVersion { get; set; } = 1;

    /// <summary>
    /// 客户端能力位掩码（<see cref="GatewayFeature"/> flags 的 uint 表示）。
    /// 服务端通过 ServerHello.featureBits 回显协商结果（按位与）。
    /// 当前未强制任何能力位；未来引入命令级能力协商时，
    /// 解析器将根据协商结果决定是否接受特定命令。
    /// </summary>
    public uint FeatureBits { get; set; }

    /// <summary>
    /// 客户端安装标识（128 位 GUID 的 32 字符十六进制表示）。
    /// 用于客户端唯一标识，服务端可结合 AccessToken 的 did 做设备绑定校验。
    /// </summary>
    public string? InstallationId { get; set; }

    /// <summary>
    /// 客户端当前时间戳（UTC 毫秒）。用于时钟偏移估算。
    /// </summary>
    public long ClientTimeMs { get; set; }

    /// <summary>
    /// 断线重连时携带的 ResumeToken。服务端校验通过后跳过完整认证直接恢复会话。
    /// 首次连接或重新登录时为 null。
    /// </summary>
    public string? ResumeToken { get; set; }

    /// <summary>
    /// 客户端支持的 Payload 上限（字节）。服务端不应发送超过此大小的帧。
    /// 0 或省略表示使用服务端默认上限（MaxPayloadSize）。
    /// </summary>
    public int? MaxPayloadBytes { get; set; }
}
