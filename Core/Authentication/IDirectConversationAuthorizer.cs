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

    /// <summary>
    /// 失效指定方向的授权缓存。关系变更（拉黑、解除好友）后调用，
    /// 确保缓存窗口内不会继续允许已禁止的 Typing/Presence 通知。
    /// </summary>
    /// <param name="senderUserId">发送方用户 Id。</param>
    /// <param name="targetUserId">目标用户 Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <remarks>
    /// 默认实现为 no-op（无缓存的实现器无需关心）。带缓存的实现器应清除
    /// (sender, target) 方向的缓存条目；调用方需双向调用以清除两个方向。
    /// </remarks>
    ValueTask InvalidateAsync(
        long senderUserId,
        long targetUserId,
        CancellationToken cancellationToken)
        => ValueTask.CompletedTask;
}
