namespace ChatApp.TcpGateway.Core.Protocol;

public static class PacketProtocol
{
    public const uint MagicNumber =
        global::ChatApp.Shared.Protocol.Tcp.TcpFrameConstants.MagicNumber;
    public const int CommandOffset =
        global::ChatApp.Shared.Protocol.Tcp.TcpFrameConstants.CommandOffset;
    public const int LengthOffset =
        global::ChatApp.Shared.Protocol.Tcp.TcpFrameConstants.LengthOffset;
    public const int HeaderSize =
        global::ChatApp.Shared.Protocol.Tcp.TcpFrameConstants.HeaderSize;
    public const int MaxPayloadSize = 80 * 1024;

    // 单一协议常量源 —— 分页条数上限与响应字节预算。
    // 后端分页不能只按条数截断，还要按序列化字节数截断，确保响应可装入单帧 TCP Payload。
    public const int HistoryPageMaxItems = 100;
    public const int ConversationListMaxItems = 100;
    public const int SyncMaxConversationsWithHistory = 50;
    public const int SyncMaxHistoryPerConversation = 100;
    public const int SyncMaxWatermarks = 50;

    /// <summary>
    /// 响应序列化字节的软上限：超过此值时按条数+字节数双截断，保留 HasMore/NextCursor。
    /// </summary>
    public const int WireResponseSoftLimit = 64 * 1024;

    /// <summary>
    /// 响应序列化字节的硬上限：等于 <see cref="MaxPayloadSize"/>，单帧绝对不可超过。
    /// </summary>
    public const int WireResponseHardLimit = MaxPayloadSize;

    /// <summary>
    /// 返回指定命令允许的 Payload 上限（字节）。
    /// <para>
    /// 解析包头后立即校验，不等完整 Payload 到达。委托 <see cref="CommandCatalog"/>，
    /// 后者是命令元数据的单一事实源；新增命令只需在 catalog 中登记。
    /// </para>
    /// </summary>
    public static int GetMaxPayloadSize(PacketCommand command) =>
        CommandCatalog.GetMaxPayload(command);

    /// <summary>
    /// 当前协议版本。握手时与服务端协商，客户端必须发送 ≤ 此值的版本。
    /// </summary>
    public const ushort CurrentProtocolVersion =
        global::ChatApp.Shared.Protocol.Tcp.TcpFrameConstants.CurrentProtocolVersion;

    /// <summary>
    /// 服务端最低支持的协议版本。客户端发送低于此值的版本将被拒绝
    /// （返回 <see cref="ProtocolErrorCode.UnsupportedVersion"/> 错误帧）。
    /// <para>
    /// 当前与 <see cref="CurrentProtocolVersion"/> 相等。未来引入不兼容变更时
    /// 可上调此值，强制客户端升级。
    /// </para>
    /// </summary>
    public const ushort MinProtocolVersion = 1;

    /// <summary>
    /// 判断命令是否为握手前命令（允许在未认证状态发送）。
    /// 包含 ClientHello 和 AuthenticationRequest。委托 <see cref="CommandCatalog"/>。
    /// </summary>
    public static bool IsAuthenticationCommand(PacketCommand command) =>
        CommandCatalog.IsPreAuthentication(command);

    /// <summary>
    /// 返回命令的令牌桶消耗权重（包令牌数）。
    /// 昂贵命令消耗更多令牌，限制其突发频率，保护数据库与下游服务。
    /// 字节令牌仍按实际帧字节数消耗，此处仅加权包维度。委托 <see cref="CommandCatalog"/>。
    /// </summary>
    public static int GetCommandCost(PacketCommand command) =>
        CommandCatalog.GetCost(command);

    /// <summary>
    /// 判断命令是否已被弃用。弃用命令仍登记在 catalog 中以保持客户端向后兼容，
    /// 但解析器应拒绝执行并返回 <see cref="ProtocolErrorCode.UnsupportedCommand"/> 错误帧。
    /// 委托 <see cref="CommandCatalog"/>。
    /// </summary>
    public static bool IsDeprecated(PacketCommand command) =>
        CommandCatalog.IsDeprecated(command);
}
