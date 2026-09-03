using System.Buffers;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using ChatApp.TcpGateway.Gateway.Serialization;

namespace ChatApp.TcpGateway.Gateway.Commands.Attachments;

/// <summary>
/// 附件相关命令处理器（AttachmentFinalizeRequest）。
/// <para>
/// 主线四：客户端完成分片上传后发送确认命令，Gateway 转发到 <see cref="IAttachmentBackend"/>
/// 触发 Realtime 侧 Ticketed→Uploaded 状态转换，并将结果映射为
/// <see cref="AttachmentFinalizeResponse"/> 返回客户端。
/// </para>
/// <para>
/// 校验顺序、错误码与 metric 事件遵循 <see cref="PushTokenCommandHandler"/> 既有约定。
/// </para>
/// </summary>
internal sealed class AttachmentCommandHandler : ICommandHandler
{
    private const int MaxRequestIdLength = 64;
    private const int MaxAttachmentIdLength = 128;
    private const int MaxContentHashLength = 128; // SHA-256 hex = 64，留余量

    private readonly IAttachmentBackend _backend;
    private readonly IPayloadCodec<AttachmentFinalizeRequest> _requestCodec;
    private readonly IPayloadCodec<AttachmentFinalizeResponse> _responseCodec;
    private readonly IPayloadCodec<AttachmentDownloadAuthorizeRequest> _downloadAuthorizeRequestCodec;
    private readonly IPayloadCodec<AttachmentDownloadAuthorizeResponse> _downloadAuthorizeResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<AttachmentCommandHandler> _logger;

    public AttachmentCommandHandler(
        IAttachmentBackend backend,
        IPayloadCodec<AttachmentFinalizeRequest> requestCodec,
        IPayloadCodec<AttachmentFinalizeResponse> responseCodec,
        IPayloadCodec<AttachmentDownloadAuthorizeRequest> downloadAuthorizeRequestCodec,
        IPayloadCodec<AttachmentDownloadAuthorizeResponse> downloadAuthorizeResponseCodec,
        GatewayMetrics metrics,
        ILogger<AttachmentCommandHandler> logger)
    {
        _backend = backend;
        _requestCodec = requestCodec;
        _responseCodec = responseCodec;
        _downloadAuthorizeRequestCodec = downloadAuthorizeRequestCodec;
        _downloadAuthorizeResponseCodec = downloadAuthorizeResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.AttachmentFinalizeRequest => HandleFinalizeAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.AttachmentDownloadAuthorizeRequest => HandleDownloadAuthorizeAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    private async ValueTask HandleFinalizeAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            session,
            PacketCommand.AttachmentFinalizeRequest,
            _requestCodec,
            payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;

        // 廉价结构校验：RequestId / AttachmentId 长度、SizeBytes 非负、ContentHash 长度。
        if (requestId.Length > MaxRequestIdLength
            || string.IsNullOrWhiteSpace(request.AttachmentId)
            || request.AttachmentId.Length > MaxAttachmentIdLength
            || request.SizeBytes < 0
            || (request.ContentHash is { Length: > MaxContentHashLength }))
        {
            SendResponse(
                session,
                new AttachmentFinalizeResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_attachment_request",
                    ErrorMessage = "附件确认请求参数无效。"
                });
            return;
        }

        try
        {
            var result = await _backend
                .FinalizeUploadAsync(
                    requestId,
                    session.UserId,
                    request.AttachmentId,
                    request.SizeBytes,
                    request.ContentHash,
                    session.SessionId,
                    cancellationToken)
                .ConfigureAwait(false);

            SendResponse(
                session,
                new AttachmentFinalizeResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    AttachmentId = result.AttachmentId,
                    Status = result.Status
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.AttachmentFinalizeRequest);
            _logger.CommandFailed(
                PacketCommand.AttachmentFinalizeRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendResponse(
                session,
                new AttachmentFinalizeResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "attachment_service_unavailable",
                    ErrorMessage = "附件上传确认服务暂时不可用。"
                });
        }
    }

    private void SendResponse(
        TcpClientSession session,
        AttachmentFinalizeResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.AttachmentFinalizeResponse,
            _responseCodec,
            session,
            response);
        session.TryQueue(frame);
    }

    private async ValueTask HandleDownloadAuthorizeAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            session,
            PacketCommand.AttachmentDownloadAuthorizeRequest,
            _downloadAuthorizeRequestCodec,
            payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;

        // 廉价结构校验：RequestId / AttachmentId 长度。
        if (requestId.Length > MaxRequestIdLength
            || string.IsNullOrWhiteSpace(request.AttachmentId)
            || request.AttachmentId.Length > MaxAttachmentIdLength)
        {
            SendDownloadAuthorizeResponse(
                session,
                new AttachmentDownloadAuthorizeResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_attachment_request",
                    ErrorMessage = "附件下载授权请求参数无效。"
                });
            return;
        }

        try
        {
            var result = await _backend
                .AuthorizeDownloadAsync(
                    requestId,
                    session.UserId,
                    request.AttachmentId,
                    request.ConversationId,
                    session.SessionId,
                    cancellationToken)
                .ConfigureAwait(false);

            SendDownloadAuthorizeResponse(
                session,
                new AttachmentDownloadAuthorizeResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    AttachmentId = result.AttachmentId,
                    DownloadUrl = result.DownloadUrl,
                    DownloadToken = result.DownloadToken,
                    ExpiresAtMs = result.ExpiresAtMs
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.AttachmentDownloadAuthorizeRequest);
            _logger.CommandFailed(
                PacketCommand.AttachmentDownloadAuthorizeRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendDownloadAuthorizeResponse(
                session,
                new AttachmentDownloadAuthorizeResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "attachment_service_unavailable",
                    ErrorMessage = "附件下载授权服务暂时不可用。"
                });
        }
    }

    private void SendDownloadAuthorizeResponse(
        TcpClientSession session,
        AttachmentDownloadAuthorizeResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.AttachmentDownloadAuthorizeResponse,
            _downloadAuthorizeResponseCodec,
            session,
            response);
        session.TryQueue(frame);
    }
}
