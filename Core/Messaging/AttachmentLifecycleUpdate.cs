namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 附件生命周期变更下行。目标为上传者本人（per-user notification）。
/// </summary>
public sealed class AttachmentLifecycleUpdate
{
    public required string AttachmentId { get; init; }

    /// <summary>新状态，取值见 <see cref="AttachmentWireStatus"/>。</summary>
    public short Status { get; init; }

    /// <summary>状态变更时间（毫秒）。</summary>
    public long OccurredAtMs { get; init; }

    /// <summary>当 Status=Rejected 时，拒绝原因代码（如 "virus"、"policy"）；其他状态可空。</summary>
    public string? RejectReason { get; init; }

    /// <summary>当 Status=Available 或 ThumbnailUpdated 时下发，鉴权下载 hint（非公网 URL）。</summary>
    public string? ThumbnailApiHint { get; init; }

    /// <summary>当 Status=Available 或 ThumbnailUpdated 时可下发新的下载令牌。</summary>
    public string? DownloadToken { get; init; }
}
