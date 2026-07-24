namespace ChatApp.TcpGateway.Core.Protocol;

public static class PacketProtocol
{
    public const uint MagicNumber = 0x1A2B3C4D;
    public const int CommandOffset = sizeof(uint);
    public const int LengthOffset = sizeof(uint) + sizeof(ushort);
    public const int HeaderSize = sizeof(uint) + sizeof(ushort) + sizeof(int);
    public const int MaxPayloadSize = 80 * 1024;

    // P0-6：单一协议常量源 —— 分页条数上限与响应字节预算。
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
    /// P0-5：解析包头后立即校验，不等完整 Payload 到达。
    /// <list type="bullet">
    /// <item>仅客户端可发送的命令返回正数上限；</item>
    /// <item>服务端→客户端命令和未定义命令返回 -1，解析器立即拒绝；</item>
    /// <item>所有正数上限均 ≤ <see cref="MaxPayloadSize"/>。</item>
    /// </list>
    /// </para>
    /// </summary>
    public static int GetMaxPayloadSize(PacketCommand command) => command switch
    {
        // 连接控制
        PacketCommand.Heartbeat => 0,
        PacketCommand.AuthenticationRequest => 4 * 1024,

        // 消息相关
        PacketCommand.ChatMessage => 64 * 1024,
        PacketCommand.MessageReceipt => 1024,
        PacketCommand.MessageHistoryRequest => 4 * 1024,
        PacketCommand.MessageRecallRequest => 1024,
        PacketCommand.MessageEditRequest => 64 * 1024,

        // 会话相关
        PacketCommand.ConversationListRequest => 4 * 1024,
        PacketCommand.ConversationMarkReadRequest => 4 * 1024,
        PacketCommand.ConversationSetPrefsRequest => 4 * 1024,

        // Reaction
        PacketCommand.AddReactionRequest => 1024,
        PacketCommand.RemoveReactionRequest => 1024,

        // 同步
        PacketCommand.SyncBootstrapRequest => 16 * 1024,

        // 群组
        PacketCommand.CreateGroupRequest => 16 * 1024,
        PacketCommand.AddGroupMembersRequest => 16 * 1024,
        PacketCommand.RemoveGroupMemberRequest => 4 * 1024,
        PacketCommand.LeaveGroupRequest => 4 * 1024,
        PacketCommand.ChangeMemberRoleRequest => 4 * 1024,
        PacketCommand.ListGroupMembersRequest => 4 * 1024,

        // Presence / Typing
        PacketCommand.TypingNotify => 512,
        PacketCommand.PresenceQuery => 4 * 1024,
        PacketCommand.PresenceUnwatch => 4 * 1024,

        // 服务端→客户端命令和未定义命令：客户端不允许发送
        _ => -1,
    };

    /// <summary>
    /// 判断命令是否为认证命令（唯一允许在未认证状态发送的命令）。
    /// </summary>
    public static bool IsAuthenticationCommand(PacketCommand command) =>
        command == PacketCommand.AuthenticationRequest;
}
