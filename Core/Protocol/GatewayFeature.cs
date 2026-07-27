namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// 网关能力位掩码。客户端通过 <c>ClientHello.featureBits</c> 声明支持的能力，
/// 服务端通过 <c>ServerHello.featureBits</c> 回显协商结果。
/// <para>
/// wire 格式为 <see cref="uint"/>，本枚举仅作为类型化解释；
/// 实际协商仍按位运算（<c>(uint)GatewayFeature.X</c>）。
/// 当前未强制任何能力位；未来引入命令级能力协商时，
/// 解析器将根据协商结果决定是否接受特定命令。
/// </para>
/// </summary>
[Flags]
public enum GatewayFeature : uint
{
    /// <summary>无能力位。客户端未声明任何扩展能力时为 0。</summary>
    None = 0,

    /// <summary>
    /// 支持二进制 payload 格式（Protobuf 等）。当前服务端只支持 JSON，
    /// 此位预留给未来协议升级；服务端不回显此位即表示仅支持 JSON。
    /// </summary>
    BinaryPayload = 1 << 0,

    /// <summary>
    /// 支持压缩帧（payload 经 gzip/zstd 压缩）。当前未实现；
    /// 客户端声明此位后服务端可选择启用压缩回传。
    /// </summary>
    Compression = 1 << 1,

    /// <summary>
    /// 支持 Streaming Chat（流式消息分片）。当前未实现；
    /// 预留给未来大消息流式传输场景。
    /// </summary>
    StreamingChat = 1 << 2,
}

/// <summary>
/// 协议 payload 格式常量。
/// <para>
/// wire 字段 <c>ServerHello.payloadFormat</c> 为字符串以保持向后兼容（JSON 序列化）；
/// 本常量类提供合法值集合与类型化解释。当前服务端固定为 <see cref="Json"/>。
/// 未来引入二进制格式时，<c>payloadFormat</c> 将扩展为 <c>"pb"</c> 等值，
/// 客户端根据 <c>ClientHello.featureBits</c> 协商结果选择格式。
/// </para>
/// </summary>
public static class ProtocolPayloadFormat
{
    /// <summary>JSON payload 格式标识（camelCase JSON，源生成 <c>GatewayJsonSerializerContext</c>）。</summary>
    public const string Json = "json";

    /// <summary>Protobuf payload 格式标识（预留，当前未实现）。</summary>
    public const string Protobuf = "pb";

    /// <summary>判断 payload 格式标识是否合法。</summary>
    public static bool IsValid(string? format) =>
        format is Json or Protobuf;
}
