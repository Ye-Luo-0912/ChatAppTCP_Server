using ChatApp.TcpGateway.Core.Protocol;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Architecture;

/// <summary>
/// 命令 Handler 覆盖完整性检查。
/// 强制每个 ClientToServer 的 PacketCommand 都有明确的处理路径，
/// 防止新增命令时漏注册 handler 导致连接被误关闭（ProtocolViolation）。
/// <para>
/// 当前架构采用三级处理：
/// 1. Inline 命令（ClientHello / AuthenticationRequest / Heartbeat）由 SessionControlHandler / TcpGatewayService 直接处理；
/// 2. PresenceUnwatch 虽为 Inline 但走 CommandDispatcher.PresenceCommandsHandler；
/// 3. 其余 ClientToServer 命令由 CommandDispatcher 路由到对应 handler。
/// </para>
/// <para>
/// 本测试通过枚举所有 C2S 命令并校验它们落在已知集合中，确保新增命令时开发者必须同时更新此处的预期集合，
/// 否则测试失败提示漏处理。
/// </para>
/// </summary>
public sealed class CommandHandlerCoverageTests
{
    /// <summary>
    /// 已知由 CommandDispatcher 显式路由的 C2S 命令集合。
    /// 新增命令到 dispatcher 的 switch 时，必须同步追加到此集合。
    /// </summary>
    private static readonly HashSet<PacketCommand> DispatcherRoutedCommands = new()
    {
        // Push
        PacketCommand.RegisterPushTokenRequest,
        PacketCommand.UnregisterPushTokenRequest,
        // Reactions
        PacketCommand.AddReactionRequest,
        PacketCommand.RemoveReactionRequest,
        // Messaging
        PacketCommand.ChatMessage,
        PacketCommand.MessageReceipt,
        PacketCommand.MessageEditRequest,
        PacketCommand.MessageRecallRequest,
        // Queries
        PacketCommand.MessageHistoryRequest,
        PacketCommand.ConversationListRequest,
        PacketCommand.SyncBootstrapRequest,
        // Conversation Prefs
        PacketCommand.ConversationMarkReadRequest,
        PacketCommand.ConversationSetPrefsRequest,
        // Groups
        PacketCommand.CreateGroupRequest,
        PacketCommand.AddGroupMembersRequest,
        PacketCommand.RemoveGroupMemberRequest,
        PacketCommand.LeaveGroupRequest,
        PacketCommand.ChangeMemberRoleRequest,
        PacketCommand.ListGroupMembersRequest,
        PacketCommand.MessageReadReceiptQueryRequest,
        PacketCommand.DissolveGroupRequest,
        // Typing
        PacketCommand.TypingNotify,
        // Presence
        PacketCommand.PresenceQuery,
        PacketCommand.PresenceUnwatch,
        // 主线四：附件与关系命令
        PacketCommand.AttachmentFinalizeRequest,
        PacketCommand.AttachmentDownloadAuthorizeRequest,
        PacketCommand.RelationshipCommandRequest,
        PacketCommand.RelationshipListRequest,
    };

    /// <summary>
    /// Inline 命令：由 SessionControlHandler / TcpGatewayService 直接处理，不走 CommandDispatcher。
    /// 协议不变量：ClientHello / AuthenticationRequest / Heartbeat 必须是 Inline。
    /// </summary>
    private static readonly HashSet<PacketCommand> InlineHandledCommands = new()
    {
        PacketCommand.ClientHello,
        PacketCommand.AuthenticationRequest,
        PacketCommand.Heartbeat,
    };

    /// <summary>
    /// 每个客户端可发送命令必须有明确处理路径：
    /// 要么在 <see cref="InlineHandledCommands"/> 中（Inline 命令直接处理），
    /// 要么在 <see cref="DispatcherRoutedCommands"/> 中（CommandDispatcher 路由）。
    /// 未落在任一集合的命令会导致 TcpGatewayService default 分支直接 ProtocolViolation 关闭连接，
    /// 这是潜在的协议回归。
    /// </summary>
    [Fact]
    public void Every_ClientToServer_Command_Has_Known_Handler()
    {
        var unhandled = new List<PacketCommand>();

        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            if (descriptor is null || descriptor.Value.Direction != CommandDirection.ClientToServer)
                continue;

            if (InlineHandledCommands.Contains(command))
                continue;
            if (DispatcherRoutedCommands.Contains(command))
                continue;

            unhandled.Add(command);
        }

        Assert.Empty(unhandled);
    }

    /// <summary>
    /// Inline 与 Dispatcher 集合不应有交集：同一命令不应同时被两条路径声明处理，
    /// 否则会出现处理顺序歧义。
    /// </summary>
    [Fact]
    public void Inline_And_Dispatcher_Sets_Are_Disjoint()
    {
        var intersection = InlineHandledCommands
            .Intersect(DispatcherRoutedCommands)
            .ToList();

        Assert.Empty(intersection);
    }

    /// <summary>
    /// 协议不变量：ClientHello / AuthenticationRequest / Heartbeat 必须是 Inline lane。
    /// 此约束已在 CommandCatalogTests 中校验，这里再次确认集合定义与 catalog 一致，
    /// 防止本测试的预期集合与 catalog 实际 lane 分配脱节。
    /// </summary>
    [Fact]
    public void InlineHandledCommands_Are_Catalogued_As_Inline()
    {
        foreach (var command in InlineHandledCommands)
        {
            Assert.Equal(
                CommandLane.Inline,
                CommandCatalog.GetLane(command));
        }
    }
}
