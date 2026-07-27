using System.Buffers;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Commands.Presence;

/// <summary>
/// 输入状态（TypingNotify）命令处理器。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="TypingFanoutCoordinator"/>、
/// <see cref="IDirectConversationAuthorizer"/>，不再依赖 service 私有字段。行为与原内联
/// handler 完全等价（校验顺序、错误码、metric 与日志事件）。
/// </para>
/// </summary>
internal sealed class TypingCommandHandler : ICommandHandler
{
    private readonly IPayloadCodec<TypingNotify> _typingNotifyCodec;
    private readonly TypingFanoutCoordinator _typingFanout;
    private readonly IDirectConversationAuthorizer? _directConversationAuthorizer;
    private readonly TcpGatewayOptions _options;

    public TypingCommandHandler(
        IPayloadCodec<TypingNotify> typingNotifyCodec,
        TypingFanoutCoordinator typingFanout,
        IDirectConversationAuthorizer? directConversationAuthorizer,
        IOptions<TcpGatewayOptions> options,
        ILogger<TypingCommandHandler> logger)
    {
        _typingNotifyCodec = typingNotifyCodec;
        _typingFanout = typingFanout;
        _directConversationAuthorizer = directConversationAuthorizer;
        _options = options.Value;
        // logger 接受以保持 DI 注册一致性；原 handler 无日志路径，故不持有。
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.TypingNotify => HandleTypingNotifyAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    /// <summary>
    /// 输入状态：本机 UserSessionRegistry 扇出。多网关需后续 ephemeral NATS。
    /// 默认关闭（<see cref="TcpGatewayOptions.EnableEphemeralPresenceAndTyping"/>）。
    /// 要求 ConversationId 与双方用户匹配（私聊成员校验）。
    /// </summary>
    private async ValueTask HandleTypingNotifyAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var notify = _typingNotifyCodec.Deserialize(payload);
        if (notify is null || string.IsNullOrWhiteSpace(notify.ConversationId))
            return;

        // 从 conversationId 推导 TargetUserId，忽略客户端提交的 TargetUserId。
        if (!TryResolveDirectConversationTarget(
                notify.ConversationId,
                session.UserId,
                out var conversationId,
                out var targetUserId))
        {
            return;
        }

        // 授权校验：检查双方是否好友或同属一会话，且未被拉黑。
        // 授权器未注入（测试场景）时跳过校验，回退到仅会话 ID 解析行为。
        if (_directConversationAuthorizer is not null)
        {
            var allowed = await _directConversationAuthorizer
                .AuthorizeAsync(session.UserId, targetUserId, cancellationToken)
                .ConfigureAwait(false);
            if (!allowed)
                return;
        }

        // 发射路径由协调器统一管理。TryAccept 内部决定是否发射：
        // 限频命中、全局/单用户槽位超限、无活跃 typing 的 isTyping=false 均不发射。
        _typingFanout.TryAccept(
            session.UserId,
            targetUserId,
            conversationId,
            notify.IsTyping);
    }

    /// <summary>
    /// 从 conversationId 解析私聊会话的另一方用户 Id。
    /// <para>
    /// 以 conversationId 为权威源，忽略客户端提交的 TargetUserId：
    /// <list type="bullet">
    /// <item>解析 dm:lo:hi 格式，校验 sender（session.UserId）必须是会话成员。</item>
    /// <item>target 为会话另一方，防止客户端伪造 TargetUserId 向任意用户发送 Typing。</item>
    /// </list>
    /// 后续可在此处插入 membership/block 缓存查询，检查会话存在性、成员关系、拉黑状态。
    /// </para>
    /// </summary>
    private static bool TryResolveDirectConversationTarget(
        string? conversationId,
        long senderUserId,
        out string normalizedId,
        out long targetUserId)
    {
        normalizedId = string.Empty;
        targetUserId = 0;

        if (string.IsNullOrWhiteSpace(conversationId) || senderUserId <= 0)
            return false;

        var trimmed = conversationId.Trim();
        if (!Realtime.Abstractions.Conversations.ConversationId.TryParseDirect(
                trimmed,
                out var userLo,
                out var userHi))
        {
            return false;
        }

        // sender 必须是会话成员。
        if (senderUserId != userLo && senderUserId != userHi)
            return false;

        // target 为另一方。
        targetUserId = senderUserId == userLo ? userHi : userLo;
        normalizedId = trimmed;
        return true;
    }
}
