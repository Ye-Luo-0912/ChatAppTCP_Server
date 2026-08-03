namespace ChatApp.TcpGateway.Core.Messaging.Attachments;

/// <summary>
/// P1-3：附件下载授权响应（S2C）。
/// <para>
/// 成功时返回带签名的短时下载 URL / 令牌及其过期时间（unix 毫秒）；
/// 失败时返回错误码与错误信息。
/// </para>
/// </summary>
public sealed class AttachmentDownloadAuthorizeResponse
{
    public required string RequestId { get; init; }

    public required bool Succeeded { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>附件 Id。</summary>
    public string? AttachmentId { get; init; }

    /// <summary>签发的短时有效下载 URL（成功时）。</summary>
    public string? DownloadUrl { get; init; }

    /// <summary>签名令牌（若 URL 需携带令牌鉴权）。</summary>
    public string? DownloadToken { get; init; }

    /// <summary>下载 URL 过期时间（unix 毫秒）。</summary>
    public long? ExpiresAtMs { get; init; }
}