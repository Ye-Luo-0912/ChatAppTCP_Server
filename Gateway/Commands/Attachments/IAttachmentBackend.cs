using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Commands.Attachments;

/// <summary>
/// 附件后端端口：抽象 RealtimeServices 侧附件生命周期操作。
/// <para>
/// 主线四：当前为 stub 实现（<see cref="StubAttachmentBackend"/>），
/// 返回 <c>attachment_service_unavailable</c>。待 sibling 仓库
/// <c>IRealtimeMessageBus</c> 新增 <c>FinalizeAttachmentUploadAsync</c> 后，
/// 替换为调用 bus 的适配实现即可。
/// </para>
/// </summary>
internal interface IAttachmentBackend
{
    /// <summary>
    /// 确认附件上传完成，触发 Realtime 侧 Ticketed→Uploaded 状态转换。
    /// </summary>
    Task<AttachmentFinalizeBackendResult> FinalizeUploadAsync(
        string requestId,
        long actorUserId,
        string attachmentId,
        long sizeBytes,
        string? contentHash,
        string? actorSessionId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 附件上传确认后端结果。
/// </summary>
internal sealed record AttachmentFinalizeBackendResult(
    string RequestId,
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    string? AttachmentId,
    /// <summary>确认后的状态（AttachmentStatus 数值：Uploaded=4 / Rejected=6 等）。</summary>
    short? Status)
{
    public static AttachmentFinalizeBackendResult Success(
        string requestId, string attachmentId, short status) =>
        new(requestId, true, null, null, attachmentId, status);

    public static AttachmentFinalizeBackendResult Failed(
        string requestId, string errorCode, string errorMessage) =>
        new(requestId, false, errorCode, errorMessage, null, null);
}

/// <summary>
/// 占位实现：RealtimeServices 侧附件 finalize 后端尚未接入。
/// 返回 <c>attachment_service_unavailable</c>，客户端可稍后重试。
/// </summary>
internal sealed class StubAttachmentBackend : IAttachmentBackend
{
    private readonly ILogger<StubAttachmentBackend> _logger;

    public StubAttachmentBackend(ILogger<StubAttachmentBackend> logger)
    {
        _logger = logger;
    }

    public Task<AttachmentFinalizeBackendResult> FinalizeUploadAsync(
        string requestId,
        long actorUserId,
        string attachmentId,
        long sizeBytes,
        string? contentHash,
        string? actorSessionId,
        CancellationToken cancellationToken = default)
    {
        _logger.AttachmentBackendUnavailable(requestId, attachmentId, actorUserId);
        return Task.FromResult(AttachmentFinalizeBackendResult.Failed(
            requestId,
            "attachment_service_unavailable",
            "附件上传确认服务暂未配置。"));
    }
}
