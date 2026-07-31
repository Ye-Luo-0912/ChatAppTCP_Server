namespace ChatApp.TcpGateway.Core.Messaging.Attachments;

/// <summary>
/// 主线四：附件上传完成确认响应（S2C）。
/// </summary>
public sealed class AttachmentFinalizeResponse
{
    public required string RequestId { get; init; }

    public required bool Succeeded { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>附件 Id。</summary>
    public string? AttachmentId { get; init; }

    /// <summary>确认后的状态（UploadConfirmed=2 或 Rejected=3）。</summary>
    public short? Status { get; init; }
}
