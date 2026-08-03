using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Commands.Attachments;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Commands.Relationships;

namespace ChatApp.TcpGateway.Gateway.Dispatching;

/// <summary>
/// 命令分发器。基于手写 switch 将已注册命令路由到对应 <see cref="ICommandHandler"/>。
/// <para>
/// 设计约束（来自 AGENTS.md / 既有架构决策）：
/// <list type="bullet">
/// <item>不引入 MediatR、反射扫描、运行时 Attribute 查找或 Dictionary 驱动的通用框架；</item>
/// <item>新增命令只需在此 switch 追加一行 + 注册 handler，编译期完整性由 <see cref="CommandCatalog"/> 与测试保障；</item>
/// <item>未注册命令返回 <c>false</c>，调用方（TcpGatewayService）继续走原 ProcessPacketAsync switch，便于增量迁移。</item>
/// </list>
/// </para>
/// </summary>
internal sealed class CommandDispatcher
{
    private readonly PushTokenCommandHandler _pushTokenHandler;
    private readonly ReactionCommandHandler _reactionHandler;
    private readonly MessagingCommandHandler _messagingHandler;
    private readonly HistoryQueryCommandHandler _historyQueryHandler;
    private readonly ConversationPrefsCommandHandler _conversationPrefsHandler;
    private readonly GroupCommandHandler _groupHandler;
    private readonly TypingCommandHandler _typingHandler;
    private readonly PresenceCommandHandler _presenceHandler;
    private readonly AttachmentCommandHandler _attachmentHandler;
    private readonly RelationshipCommandHandler _relationshipHandler;

    public CommandDispatcher(
        PushTokenCommandHandler pushTokenHandler,
        ReactionCommandHandler reactionHandler,
        MessagingCommandHandler messagingHandler,
        HistoryQueryCommandHandler historyQueryHandler,
        ConversationPrefsCommandHandler conversationPrefsHandler,
        GroupCommandHandler groupHandler,
        TypingCommandHandler typingHandler,
        PresenceCommandHandler presenceHandler,
        AttachmentCommandHandler attachmentHandler,
        RelationshipCommandHandler relationshipHandler)
    {
        _pushTokenHandler = pushTokenHandler;
        _reactionHandler = reactionHandler;
        _messagingHandler = messagingHandler;
        _historyQueryHandler = historyQueryHandler;
        _conversationPrefsHandler = conversationPrefsHandler;
        _groupHandler = groupHandler;
        _typingHandler = typingHandler;
        _presenceHandler = presenceHandler;
        _attachmentHandler = attachmentHandler;
        _relationshipHandler = relationshipHandler;
    }

    /// <summary>
    /// 尝试将命令分发给已注册的 handler。
    /// </summary>
    /// <returns>
    /// <c>true</c> 表示该命令已被 handler 接管（无论成功与否）；<c>false</c> 表示无对应 handler，调用方需走原 switch。
    /// </returns>
    public ValueTask<bool> TryDispatchAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        // Push
        PacketCommand.RegisterPushTokenRequest
        or PacketCommand.UnregisterPushTokenRequest =>
            InvokeAsync(_pushTokenHandler, frame, context, cancellationToken),

        // Reactions
        PacketCommand.AddReactionRequest
        or PacketCommand.RemoveReactionRequest =>
            InvokeAsync(_reactionHandler, frame, context, cancellationToken),

        // Messaging (Chat / Receipt / Edit / Recall)
        PacketCommand.ChatMessage
        or PacketCommand.MessageReceipt
        or PacketCommand.MessageEditRequest
        or PacketCommand.MessageRecallRequest =>
            InvokeAsync(_messagingHandler, frame, context, cancellationToken),

        // Queries (History / ConversationList / SyncBootstrap)
        PacketCommand.MessageHistoryRequest
        or PacketCommand.ConversationListRequest
        or PacketCommand.SyncBootstrapRequest =>
            InvokeAsync(_historyQueryHandler, frame, context, cancellationToken),

        // Conversation Prefs (MarkRead / SetPrefs)
        PacketCommand.ConversationMarkReadRequest
        or PacketCommand.ConversationSetPrefsRequest =>
            InvokeAsync(_conversationPrefsHandler, frame, context, cancellationToken),

        // Groups
        PacketCommand.CreateGroupRequest
        or PacketCommand.AddGroupMembersRequest
        or PacketCommand.RemoveGroupMemberRequest
        or PacketCommand.LeaveGroupRequest
        or PacketCommand.ChangeMemberRoleRequest
        or PacketCommand.ListGroupMembersRequest
        or PacketCommand.MessageReadReceiptQueryRequest =>
            InvokeAsync(_groupHandler, frame, context, cancellationToken),

        // Typing
        PacketCommand.TypingNotify =>
            InvokeAsync(_typingHandler, frame, context, cancellationToken),

        // Presence
        PacketCommand.PresenceQuery
        or PacketCommand.PresenceUnwatch =>
            InvokeAsync(_presenceHandler, frame, context, cancellationToken),

        // Attachments (主线四 / P1-3)
        PacketCommand.AttachmentFinalizeRequest
        or PacketCommand.AttachmentDownloadAuthorizeRequest =>
            InvokeAsync(_attachmentHandler, frame, context, cancellationToken),

        // Relationships (主线四)
        PacketCommand.RelationshipCommandRequest
        or PacketCommand.RelationshipListRequest =>
            InvokeAsync(_relationshipHandler, frame, context, cancellationToken),

        _ => new ValueTask<bool>(false)
    };

    private static async ValueTask<bool> InvokeAsync(
        ICommandHandler handler,
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken)
    {
        await handler
            .ExecuteAsync(frame, context, cancellationToken)
            .ConfigureAwait(false);
        return true;
    }
}
