using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Commands.Attachments;

/// <summary>
/// 附件后端端口：抽象 RealtimeServices 侧附件生命周期操作。
/// <para>
/// 主线四：生产路径由 <see cref="RealtimeAttachmentBackend"/> 实现，经
/// <c>IRealtimeMessageBus.FinalizeAttachmentUploadAsync</c> 转发到 Realtime 侧
/// 触发 Ticketed→Uploaded 状态转换。<see cref="StubAttachmentBackend"/> 仅保留供
/// 单测注入（不依赖 NATS 总线）。
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

    /// <summary>
    /// 为附件签发短时有效的签名下载 URL / 令牌。
    /// </summary>
    Task<AttachmentDownloadAuthorizeBackendResult> AuthorizeDownloadAsync(
        string requestId,
        long actorUserId,
        string attachmentId,
        string? conversationId,
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
/// 附件下载授权后端结果。
/// </summary>
internal sealed record AttachmentDownloadAuthorizeBackendResult(
    string RequestId,
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    string? AttachmentId,
    string? DownloadUrl,
    string? DownloadToken,
    long? ExpiresAtMs)
{
    public static AttachmentDownloadAuthorizeBackendResult Success(
        string requestId,
        string attachmentId,
        string downloadUrl,
        string? downloadToken,
        long? expiresAtMs) =>
        new(requestId, true, null, null, attachmentId, downloadUrl, downloadToken, expiresAtMs);

    public static AttachmentDownloadAuthorizeBackendResult Failed(
        string requestId, string errorCode, string errorMessage) =>
        new(requestId, false, errorCode, errorMessage, null, null, null, null);
}

/// <summary>
/// 占位实现：RealtimeServices 侧附件 finalize 后端尚未接入。
/// 返回 <c>attachment_service_unavailable</c>，客户端可稍后重试。
/// <para>仅用于单测注入；生产路径注册 <see cref="RealtimeAttachmentBackend"/>。</para>
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

    public Task<AttachmentDownloadAuthorizeBackendResult> AuthorizeDownloadAsync(
        string requestId,
        long actorUserId,
        string attachmentId,
        string? conversationId,
        string? actorSessionId,
        CancellationToken cancellationToken = default)
    {
        _logger.AttachmentBackendUnavailable(requestId, attachmentId, actorUserId);
        return Task.FromResult(AttachmentDownloadAuthorizeBackendResult.Failed(
            requestId,
            "attachment_service_unavailable",
            "附件下载授权服务暂未配置。"));
    }
}

/// <summary>
/// 生产适配实现：经 <see cref="IRealtimeMessageBus.FinalizeAttachmentUploadAsync"/>
/// 将附件确认命令转发到 RealtimeServices，触发 Ticketed→Uploaded 状态转换。
/// <para>
/// 总线异常（含 <see cref="RealtimeServerBusyException"/>、NATS 超时等）不做吞咽，
/// 直接向 <see cref="AttachmentCommandHandler"/> 抛出，由其 catch-all 统一映射为
/// <c>attachment_service_unavailable</c> 响应。这与其他 Realtime 命令处理器
/// （<see cref="GroupCommandHandler"/> / <c>MessagingCommandHandler</c>）的异常约定一致。
/// </para>
/// </summary>
internal sealed class RealtimeAttachmentBackend : IAttachmentBackend
{
    private readonly IRealtimeMessageBus _messageBus;

    public RealtimeAttachmentBackend(IRealtimeMessageBus messageBus)
    {
        _messageBus = messageBus;
    }

    public async Task<AttachmentFinalizeBackendResult> FinalizeUploadAsync(
        string requestId,
        long actorUserId,
        string attachmentId,
        long sizeBytes,
        string? contentHash,
        string? actorSessionId,
        CancellationToken cancellationToken = default)
    {
        var command = new AttachmentFinalizeCommand
        {
            RequestId = requestId,
            ActorUserId = actorUserId,
            AttachmentId = attachmentId,
            SizeBytes = sizeBytes,
            ContentHash = contentHash,
            ActorSessionId = actorSessionId,
        };

        var result = await _messageBus
            .FinalizeAttachmentUploadAsync(command, cancellationToken)
            .ConfigureAwait(false);

        return new AttachmentFinalizeBackendResult(
            result.RequestId,
            result.Succeeded,
            result.ErrorCode,
            result.ErrorMessage,
            result.AttachmentId,
            result.Status);
    }

    public async Task<AttachmentDownloadAuthorizeBackendResult> AuthorizeDownloadAsync(
        string requestId,
        long actorUserId,
        string attachmentId,
        string? conversationId,
        string? actorSessionId,
        CancellationToken cancellationToken = default)
    {
        var command = new AttachmentDownloadAuthorizeCommand
        {
            RequestId = requestId,
            ActorUserId = actorUserId,
            AttachmentId = attachmentId,
            ConversationId = conversationId,
            ActorSessionId = actorSessionId,
        };

        var result = await _messageBus
            .AuthorizeAttachmentDownloadAsync(command, cancellationToken)
            .ConfigureAwait(false);

        return new AttachmentDownloadAuthorizeBackendResult(
            result.RequestId,
            result.Succeeded,
            result.ErrorCode,
            result.ErrorMessage,
            result.AttachmentId,
            result.DownloadUrl,
            result.DownloadToken,
            result.ExpiresAtMs);
    }
}
