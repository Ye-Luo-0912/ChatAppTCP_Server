namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// 网关能力位掩码。客户端通过 <c>ClientHello.featureBits</c> 声明支持的能力，
/// 服务端通过 <c>ServerHello.featureBits</c> 回显协商结果。
/// <para>
/// wire 格式为 <see cref="uint"/>，本枚举仅作为类型化解释；
/// 实际协商仍按位运算（<c>(uint)GatewayFeature.X</c>）。
/// 客户端显式协商 <see cref="CommandCapabilities"/> 后，网关才按命令所需能力位
/// 执行严格门控；未声明该位的 v1 客户端保持原有兼容行为。
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

    /// <summary>
    /// 启用命令级能力门控。客户端声明此位后，只能发送其余已协商能力覆盖的扩展命令。
    /// 未声明此位时保持 v1 全命令兼容语义。
    /// </summary>
    CommandCapabilities = 1 << 3,

    /// <summary>支持通过 ClientHello.resumeToken 恢复会话。</summary>
    SessionResume = 1 << 4,

    /// <summary>支持 SyncBootstrap 增量同步。</summary>
    ConversationSync = 1 << 5,

    /// <summary>支持会话偏好设置。</summary>
    ConversationPreferences = 1 << 6,

    /// <summary>支持消息撤回与编辑。</summary>
    MessageMutation = 1 << 7,

    /// <summary>支持输入状态与在线状态查询/订阅。</summary>
    PresenceAndTyping = 1 << 8,

    /// <summary>支持消息 Reaction。</summary>
    MessageReactions = 1 << 9,

    /// <summary>支持群组管理命令。</summary>
    GroupManagement = 1 << 10,

    /// <summary>支持离线推送 Token 注册与注销。</summary>
    PushTokenManagement = 1 << 11,
}

/// <summary>
/// 网关能力集合。集中区分已经实现的能力与仅保留 wire 编号的未来能力。
/// </summary>
public static class GatewayFeatureSet
{
    /// <summary>当前服务端已实现、可在 ServerHello 中协商的能力。</summary>
    public const GatewayFeature Implemented =
        GatewayFeature.CommandCapabilities |
        GatewayFeature.SessionResume |
        GatewayFeature.ConversationSync |
        GatewayFeature.ConversationPreferences |
        GatewayFeature.MessageMutation |
        GatewayFeature.PresenceAndTyping |
        GatewayFeature.MessageReactions |
        GatewayFeature.GroupManagement |
        GatewayFeature.PushTokenManagement;

    /// <summary>协议已经分配的全部能力位，包括尚未实现的预留位。</summary>
    public const GatewayFeature Known =
        GatewayFeature.BinaryPayload |
        GatewayFeature.Compression |
        GatewayFeature.StreamingChat |
        Implemented;

    /// <summary>判断位掩码是否包含指定能力集合。</summary>
    public static bool ContainsAll(uint featureBits, GatewayFeature required)
    {
        var requiredBits = (uint)required;
        return (featureBits & requiredBits) == requiredBits;
    }
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
