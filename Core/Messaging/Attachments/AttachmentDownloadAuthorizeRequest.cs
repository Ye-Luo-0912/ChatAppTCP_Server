namespace ChatApp.TcpGateway.Core.Messaging.Attachments;

/// <summary>
/// P1-3：附件下载授权请求（C2S）。
/// <para>
/// 客户端请求为指定附件签发短时有效的签名下载 URL。Gateway 转发到
/// Realtime 侧调用对象存储签发 URL，并将结果映射为
/// <see cref="AttachmentDownloadAuthorizeResponse"/> 返回客户端。
/// </para>
/// </summary>
public sealed class AttachmentDownloadAuthorizeRequest
{
    public required string RequestId { get; init; }

    /// <summary>附件 Id（由 HTTP API 在 Initiate 阶段签发）。</summary>
    public required string AttachmentId { get; init; }

    /// <summary>可选：附件所属会话 Id，辅助 Realtime 侧权限校验。</summary>
    public string? ConversationId { get; init; }
}