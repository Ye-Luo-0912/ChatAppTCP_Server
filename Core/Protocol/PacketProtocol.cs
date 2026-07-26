namespace ChatApp.TcpGateway.Core.Protocol;

public static class PacketProtocol
{
    public const uint MagicNumber = 0x1A2B3C4D;
    public const int CommandOffset = sizeof(uint);
    public const int LengthOffset = sizeof(uint) + sizeof(ushort);
    public const int HeaderSize = sizeof(uint) + sizeof(ushort) + sizeof(int);
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
    public const ushort CurrentProtocolVersion = 1;

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
}
