namespace ChatApp.TcpGateway.Core.Messaging.Attachments;

/// <summary>
/// 主线四：附件上传完成确认请求（C2S）。
/// <para>
/// 客户端完成分片上传后发送此命令，触发 Realtime 侧 Ticketed→Uploaded 状态转换。
/// Realtime 侧校验上传完整性后返回 <see cref="AttachmentFinalizeResponse"/>。
/// </para>
/// </summary>
public sealed class AttachmentFinalizeRequest
{
    public required string RequestId { get; init; }

    /// <summary>附件 Id（由 HTTP API 在 Initiate 阶段签发）。</summary>
    public required string AttachmentId { get; init; }

    /// <summary>上传大小（字节），用于服务端校验完整性。</summary>
    public long SizeBytes { get; init; }

    /// <summary>可选：内容哈希（SHA-256 hex），用于服务端去重校验。</summary>
    public string? ContentHash { get; init; }
}
