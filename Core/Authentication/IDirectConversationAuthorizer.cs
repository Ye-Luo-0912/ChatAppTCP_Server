namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 私聊会话授权器。校验发送方是否有权向目标用户发送 Typing 等瞬态通知。
/// 复用 Presence 授权路径（好友关系 OR 同属一会话），并叠加拉黑检查。
/// </summary>
public interface IDirectConversationAuthorizer
{
    /// <summary>
    /// 校验发送方是否可向目标用户发送私聊瞬态通知。
    /// </summary>
    /// <param name="senderUserId">发送方用户 Id。</param>
    /// <param name="targetUserId">目标用户 Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>true 表示允许；false 表示拒绝（非好友/被拉黑/无共同会话）。</returns>
    ValueTask<bool> AuthorizeAsync(
        long senderUserId,
        long targetUserId,
        CancellationToken cancellationToken);
}
