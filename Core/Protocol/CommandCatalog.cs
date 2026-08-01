namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// 命令传输方向。
/// </summary>
internal enum CommandDirection : byte
{
    /// <summary>客户端 → 服务端。</summary>
    ClientToServer,

    /// <summary>服务端 → 客户端。客户端发送此类命令会被解析器立即拒绝。</summary>
    ServerToClient
}

/// <summary>
/// 命令执行所需的连接阶段。
/// </summary>
internal enum ConnectionPhase : byte
{
    /// <summary>握手前（仅 ClientHello）。</summary>
    PreHandshake,

    /// <summary>握手完成后、认证前（AuthenticationRequest / ResumeRequest）。</summary>
    PreAuthentication,

    /// <summary>已认证。所有业务命令。</summary>
    Authenticated
}

/// <summary>
/// 命令调度 lane 分类。
/// <para>
/// Control 命令（Auth/Heartbeat/PresenceUnwatch）由读循环内联处理；
/// Ephemeral 命令（Typing）通过全局 <c>SessionCommandExecutor</c>（共享 worker 池）异步处理，
/// 避免 Typing 授权器缓存 Miss 时的远程 I/O 阻塞 TCP Read Loop；
/// OrderedWrite/Query 同样通过全局 <c>SessionCommandExecutor</c> 异步处理。
/// </para>
/// </summary>
internal enum CommandLane : byte
{
    /// <summary>Auth/Heartbeat/PresenceUnwatch：读循环内联处理。</summary>
    Inline,

    /// <summary>Chat/Receipt/Edit/Recall 等写操作：保持顺序。</summary>
    OrderedWrite,

    /// <summary>History/List/Sync 等查询：与 OrderedWrite 并行。</summary>
    Query,

    /// <summary>Typing 等瞬态命令：DropOldest，允许丢弃旧帧。</summary>
    Ephemeral
}

/// <summary>
/// 单条命令的全部协议元数据。单一事实源，替代分散在 PacketProtocol 与 TcpGatewayService 的多个 switch。
/// </summary>
internal readonly record struct CommandDescriptor(
    PacketCommand Command,
    CommandDirection Direction,
    ConnectionPhase RequiredPhase,
    CommandLane Lane,
    int MaxPayloadBytes,
    int RateCost)
{
    /// <summary>
    /// 该命令是否已被弃用。弃用命令仍可在 catalog 中登记以保持客户端向后兼容，
    /// 但解析器应拒绝执行并返回 <see cref="ProtocolErrorCode.UnsupportedCommand"/> 错误帧，
    /// 引导客户端迁移到替代命令。默认 false。
    /// <para>
    /// 弃用策略：标记 Deprecated=true 的命令在下一个协议大版本中移除；
    /// 期间服务端只返回错误，不执行任何业务逻辑。
    /// </para>
    /// </summary>
    public bool Deprecated { get; init; }

    /// <summary>
    /// 客户端启用 <see cref="GatewayFeature.CommandCapabilities"/> 后，
    /// 执行该命令前必须协商的扩展能力。默认无要求，保持核心命令兼容。
    /// </summary>
    public GatewayFeature RequiredFeature { get; init; }
}

/// <summary>
/// 命令元数据目录。集中维护所有 <see cref="PacketCommand"/> 的 Direction / Phase / Lane / Payload 上限 / 速率成本。
/// <para>
/// 新增命令时只需在此处追加一行 switch case，<see cref="CommandCatalogTests"/> 中的完整性测试会强制每个枚举值都被列出。
/// </para>
/// </summary>
internal static class CommandCatalog
{
    /// <summary>
    /// 返回指定命令的描述符；未在目录中登记的命令返回 null。
    /// </summary>
    public static CommandDescriptor? TryGetDescriptor(PacketCommand command) => command switch
    {
        // 连接控制
        PacketCommand.Heartbeat => new(
            PacketCommand.Heartbeat, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Inline, 0, 1),
        PacketCommand.AuthenticationRequest => new(
            PacketCommand.AuthenticationRequest, CommandDirection.ClientToServer,
            ConnectionPhase.PreAuthentication, CommandLane.Inline, 4 * 1024, 2),
        PacketCommand.AuthenticationResponse => new(
            PacketCommand.AuthenticationResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ClientHello => new(
            PacketCommand.ClientHello, CommandDirection.ClientToServer,
            ConnectionPhase.PreHandshake, CommandLane.Inline, 4 * 1024, 1),
        PacketCommand.ServerHello => new(
            PacketCommand.ServerHello, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.GoAway => new(
            PacketCommand.GoAway, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        // ResumeRequest 不是独立的 wire 命令——Resume 流程通过 ClientHello.ResumeToken 字段触发
        // （见 SessionControlHandler.TryResumeAsync）。枚举值仅为 catalog 完整性保留。
        // Direction 标记为 ServerToClient：客户端不可将其作为独立帧发送，GetMaxPayload 返回 -1，
        // 解析器立即拒绝。这与"客户端发起 Resume"的语义不矛盾——发起方通过 ClientHello 字段表达意图，
        // 而非发送 ResumeRequest 帧。
        PacketCommand.ResumeRequest => new(
            PacketCommand.ResumeRequest, CommandDirection.ServerToClient,
            ConnectionPhase.PreAuthentication, CommandLane.Inline, -1, 1),
        PacketCommand.ResumeResponse => new(
            PacketCommand.ResumeResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // 消息相关
        PacketCommand.ChatMessage => new(
            PacketCommand.ChatMessage, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 64 * 1024, 4),
        PacketCommand.MessageAcknowledgement => new(
            PacketCommand.MessageAcknowledgement, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MessageReceipt => new(
            PacketCommand.MessageReceipt, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 1024, 1),
        PacketCommand.MessageReceiptAcknowledgement => new(
            PacketCommand.MessageReceiptAcknowledgement, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MessageReceiptUpdated => new(
            PacketCommand.MessageReceiptUpdated, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MessageHistoryRequest => new(
            PacketCommand.MessageHistoryRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Query, 4 * 1024, 4),
        PacketCommand.MessageHistoryPage => new(
            PacketCommand.MessageHistoryPage, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // 会话相关
        PacketCommand.ConversationListRequest => new(
            PacketCommand.ConversationListRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Query, 4 * 1024, 4),
        PacketCommand.ConversationListPage => new(
            PacketCommand.ConversationListPage, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ConversationMarkReadRequest => new(
            PacketCommand.ConversationMarkReadRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4 * 1024, 2),
        PacketCommand.ConversationMarkReadResponse => new(
            PacketCommand.ConversationMarkReadResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ConversationChanged => new(
            PacketCommand.ConversationChanged, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.UnreadCountChanged => new(
            PacketCommand.UnreadCountChanged, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.SyncBootstrapRequest => new(
            PacketCommand.SyncBootstrapRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Query, 16 * 1024, 8)
        {
            RequiredFeature = GatewayFeature.ConversationSync
        },
        PacketCommand.SyncBootstrapResponse => new(
            PacketCommand.SyncBootstrapResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ConversationSetPrefsRequest => new(
            PacketCommand.ConversationSetPrefsRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4 * 1024, 4)
        {
            RequiredFeature = GatewayFeature.ConversationPreferences
        },
        PacketCommand.ConversationSetPrefsResponse => new(
            PacketCommand.ConversationSetPrefsResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // Recall / Edit
        PacketCommand.MessageRecallRequest => new(
            PacketCommand.MessageRecallRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 1024, 2)
        {
            RequiredFeature = GatewayFeature.MessageMutation
        },
        PacketCommand.MessageRecallAck => new(
            PacketCommand.MessageRecallAck, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MessageRecalled => new(
            PacketCommand.MessageRecalled, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MessageEditRequest => new(
            PacketCommand.MessageEditRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 64 * 1024, 2)
        {
            RequiredFeature = GatewayFeature.MessageMutation
        },
        PacketCommand.MessageEditAck => new(
            PacketCommand.MessageEditAck, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MessageEdited => new(
            PacketCommand.MessageEdited, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // Typing / Presence
        PacketCommand.TypingNotify => new(
            PacketCommand.TypingNotify, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Ephemeral, 512, 1)
        {
            RequiredFeature = GatewayFeature.PresenceAndTyping
        },
        PacketCommand.TypingUpdate => new(
            PacketCommand.TypingUpdate, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.PresenceQuery => new(
            PacketCommand.PresenceQuery, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Query, 4 * 1024, 2)
        {
            RequiredFeature = GatewayFeature.PresenceAndTyping
        },
        PacketCommand.PresenceSnapshot => new(
            PacketCommand.PresenceSnapshot, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.PresenceChanged => new(
            PacketCommand.PresenceChanged, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.PresenceUnwatch => new(
            PacketCommand.PresenceUnwatch, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Inline, 4 * 1024, 1)
        {
            RequiredFeature = GatewayFeature.PresenceAndTyping
        },

        // Reaction
        PacketCommand.AddReactionRequest => new(
            PacketCommand.AddReactionRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 1024, 2)
        {
            RequiredFeature = GatewayFeature.MessageReactions
        },
        PacketCommand.AddReactionAck => new(
            PacketCommand.AddReactionAck, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ReactionAdded => new(
            PacketCommand.ReactionAdded, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.RemoveReactionRequest => new(
            PacketCommand.RemoveReactionRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 1024, 2)
        {
            RequiredFeature = GatewayFeature.MessageReactions
        },
        PacketCommand.RemoveReactionAck => new(
            PacketCommand.RemoveReactionAck, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ReactionRemoved => new(
            PacketCommand.ReactionRemoved, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // 群组
        PacketCommand.CreateGroupRequest => new(
            PacketCommand.CreateGroupRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 16 * 1024, 8)
        {
            RequiredFeature = GatewayFeature.GroupManagement
        },
        PacketCommand.CreateGroupResponse => new(
            PacketCommand.CreateGroupResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.AddGroupMembersRequest => new(
            PacketCommand.AddGroupMembersRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 16 * 1024, 8)
        {
            RequiredFeature = GatewayFeature.GroupManagement
        },
        PacketCommand.AddGroupMembersResponse => new(
            PacketCommand.AddGroupMembersResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.RemoveGroupMemberRequest => new(
            PacketCommand.RemoveGroupMemberRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4 * 1024, 4)
        {
            RequiredFeature = GatewayFeature.GroupManagement
        },
        PacketCommand.RemoveGroupMemberResponse => new(
            PacketCommand.RemoveGroupMemberResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.LeaveGroupRequest => new(
            PacketCommand.LeaveGroupRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4 * 1024, 4)
        {
            RequiredFeature = GatewayFeature.GroupManagement
        },
        PacketCommand.LeaveGroupResponse => new(
            PacketCommand.LeaveGroupResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ChangeMemberRoleRequest => new(
            PacketCommand.ChangeMemberRoleRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4 * 1024, 4)
        {
            RequiredFeature = GatewayFeature.GroupManagement
        },
        PacketCommand.ChangeMemberRoleResponse => new(
            PacketCommand.ChangeMemberRoleResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ListGroupMembersRequest => new(
            PacketCommand.ListGroupMembersRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Query, 4 * 1024, 4)
        {
            RequiredFeature = GatewayFeature.GroupManagement
        },
        PacketCommand.ListGroupMembersResponse => new(
            PacketCommand.ListGroupMembersResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MemberJoined => new(
            PacketCommand.MemberJoined, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MemberLeft => new(
            PacketCommand.MemberLeft, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MemberRemoved => new(
            PacketCommand.MemberRemoved, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.RoleChanged => new(
            PacketCommand.RoleChanged, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ConversationRead => new(
            PacketCommand.ConversationRead, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.RelationshipListChanged => new(
            PacketCommand.RelationshipListChanged, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.AttachmentLifecycleChanged => new(
            PacketCommand.AttachmentLifecycleChanged, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.AttachmentFinalizeRequest => new(
            PacketCommand.AttachmentFinalizeRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4096, 2),
        PacketCommand.AttachmentFinalizeResponse => new(
            PacketCommand.AttachmentFinalizeResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.RelationshipCommandRequest => new(
            PacketCommand.RelationshipCommandRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 4096, 2),
        PacketCommand.RelationshipCommandResponse => new(
            PacketCommand.RelationshipCommandResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.RelationshipListRequest => new(
            PacketCommand.RelationshipListRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.Query, 4096, 1),
        PacketCommand.RelationshipListResponse => new(
            PacketCommand.RelationshipListResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.MembersAddedUpdate => new(
            PacketCommand.MembersAddedUpdate, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.ConversationDissolvedUpdate => new(
            PacketCommand.ConversationDissolvedUpdate, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // 离线推送
        PacketCommand.RegisterPushTokenRequest => new(
            PacketCommand.RegisterPushTokenRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 2 * 1024, 2)
        {
            RequiredFeature = GatewayFeature.PushTokenManagement
        },
        PacketCommand.RegisterPushTokenResponse => new(
            PacketCommand.RegisterPushTokenResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.UnregisterPushTokenRequest => new(
            PacketCommand.UnregisterPushTokenRequest, CommandDirection.ClientToServer,
            ConnectionPhase.Authenticated, CommandLane.OrderedWrite, 2 * 1024, 1)
        {
            RequiredFeature = GatewayFeature.PushTokenManagement
        },
        PacketCommand.UnregisterPushTokenResponse => new(
            PacketCommand.UnregisterPushTokenResponse, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        // 协议级
        PacketCommand.HeartbeatAcknowledgement => new(
            PacketCommand.HeartbeatAcknowledgement, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),
        PacketCommand.Error => new(
            PacketCommand.Error, CommandDirection.ServerToClient,
            ConnectionPhase.Authenticated, CommandLane.Inline, -1, 1),

        _ => null
    };

    /// <summary>
    /// 返回命令的调度 lane。未在目录中登记的命令默认 <see cref="CommandLane.OrderedWrite"/>。
    /// </summary>
    public static CommandLane GetLane(PacketCommand command) =>
        TryGetDescriptor(command)?.Lane ?? CommandLane.OrderedWrite;

    /// <summary>
    /// 返回命令允许的 Payload 上限（字节）。
    /// <list type="bullet">
    /// <item>客户端可发送命令返回正数上限；</item>
    /// <item>服务端→客户端命令和未登记命令返回 -1，解析器立即拒绝。</item>
    /// </list>
    /// </summary>
    public static int GetMaxPayload(PacketCommand command) =>
        TryGetDescriptor(command) is { Direction: CommandDirection.ClientToServer } d
            ? d.MaxPayloadBytes
            : -1;

    /// <summary>
    /// 返回命令的令牌桶消耗权重。未登记命令按默认成本 1。
    /// </summary>
    public static int GetCost(PacketCommand command) =>
        TryGetDescriptor(command)?.RateCost ?? 1;

    /// <summary>
    /// 判断命令是否可在未认证状态发送（ClientHello / AuthenticationRequest）。
    /// </summary>
    public static bool IsPreAuthentication(PacketCommand command) =>
        TryGetDescriptor(command)?.RequiredPhase is ConnectionPhase.PreHandshake
            or ConnectionPhase.PreAuthentication;

    /// <summary>
    /// 判断命令是否已被弃用。弃用命令仍登记在 catalog 中以保持客户端向后兼容，
    /// 但解析器应拒绝执行并返回 <see cref="ProtocolErrorCode.UnsupportedCommand"/> 错误帧。
    /// </summary>
    public static bool IsDeprecated(PacketCommand command) =>
        TryGetDescriptor(command)?.Deprecated ?? false;

    /// <summary>
    /// 判断命令是否满足协商能力。未协商 CommandCapabilities 时保持 v1 兼容；
    /// 严格模式下只执行 RequiredFeature 已全部协商的命令。
    /// </summary>
    public static bool IsFeatureAllowed(
        in CommandDescriptor descriptor,
        uint negotiatedFeatureBits) =>
        !GatewayFeatureSet.ContainsAll(
            negotiatedFeatureBits,
            GatewayFeature.CommandCapabilities) ||
        GatewayFeatureSet.ContainsAll(
            negotiatedFeatureBits,
            descriptor.RequiredFeature);
}
