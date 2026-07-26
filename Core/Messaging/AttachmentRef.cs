namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// TCP 线协议附件引用。客户端经 DownloadApiHint 走鉴权下载，不含永久公网 URL。
/// </summary>
public sealed class AttachmentRef
{
    public const int CurrentVersion = 1;

    public int RefVersion { get; set; } = CurrentVersion;

    public required string AttachmentId { get; set; }

    public string? FileName { get; set; }

    public required string ContentType { get; set; }

    public long SizeBytes { get; set; }

    /// <summary>0=Scanning，1=Available。</summary>
    public short Status { get; set; }

    /// <summary>通常为 attachmentId → GET /api/attachments/{id}/download。</summary>
    public string? DownloadApiHint { get; set; }

    public string? DownloadToken { get; set; }

    public string? ThumbnailApiHint { get; set; }
}

public enum AttachmentWireStatus : short
{
    Scanning = 0,
    Available = 1,
    UploadConfirmed = 2,
    Rejected = 3,
    Expired = 4,
    ThumbnailUpdated = 5
}
