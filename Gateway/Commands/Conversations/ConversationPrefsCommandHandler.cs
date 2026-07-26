using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using System.Buffers;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeConversationMarkReadCommand =
    ChatApp.Realtime.Abstractions.Conversations.ConversationMarkReadCommand;
using RealtimeConversationSetPrefsCommand =
    ChatApp.Realtime.Abstractions.Conversations.ConversationSetPrefsCommand;

namespace ChatApp.TcpGateway.Gateway.Commands.Conversations;

/// <summary>
/// 会话偏好相关命令处理器（ConversationMarkReadRequest / ConversationSetPrefsRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec 与 <see cref="IRealtimeMessageBus"/> 端口，
/// 不再依赖 service 的私有字段。行为与原内联 handler 完全等价
/// （校验顺序、错误码、metric 与日志事件）。
/// </para>
/// </summary>
internal sealed class ConversationPrefsCommandHandler : ICommandHandler
{
    private const int MaxRequestIdLength = 64;
    private const int MaxConversationIdLength = 64;
    private const int MaxMessageIdLength = 64;

    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<ConversationMarkReadRequest> _markReadRequestCodec;
    private readonly IPayloadCodec<ConversationMarkReadResponse> _markReadResponseCodec;
    private readonly IPayloadCodec<ConversationSetPrefsRequest> _setPrefsRequestCodec;
    private readonly IPayloadCodec<ConversationSetPrefsResponse> _setPrefsResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<ConversationPrefsCommandHandler> _logger;

    public ConversationPrefsCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<ConversationMarkReadRequest> markReadRequestCodec,
        IPayloadCodec<ConversationMarkReadResponse> markReadResponseCodec,
        IPayloadCodec<ConversationSetPrefsRequest> setPrefsRequestCodec,
        IPayloadCodec<ConversationSetPrefsResponse> setPrefsResponseCodec,
        GatewayMetrics metrics,
        ILogger<ConversationPrefsCommandHandler> logger)
    {
        _messageBus = messageBus;
        _markReadRequestCodec = markReadRequestCodec;
        _markReadResponseCodec = markReadResponseCodec;
        _setPrefsRequestCodec = setPrefsRequestCodec;
        _setPrefsResponseCodec = setPrefsResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        ICommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.ConversationMarkReadRequest => HandleMarkReadAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.ConversationSetPrefsRequest => HandleSetPrefsAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    /// <summary>
    /// 标记会话已读。ReadAtMs 与 ReadMessageId 必须同时提供或同时缺省；
    /// 任一非空时形成游标，由 Realtime 端去重并返回最新未读数与最后已读位置。
    /// </summary>
    private async ValueTask HandleMarkReadAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _markReadRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        var hasCursorTime = request.ReadAtMs.HasValue;
        var hasCursorMessage = !string.IsNullOrWhiteSpace(request.ReadMessageId);
        if (requestId.Length > MaxRequestIdLength
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.ConversationId.Length > MaxConversationIdLength
            || hasCursorTime != hasCursorMessage
            || request.ReadAtMs is <= 0
            || request.ReadMessageId?.Length > MaxMessageIdLength)
        {
            SendConversationMarkReadResponse(
                session,
                new ConversationMarkReadResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_conversation_mark_read_request",
                    ErrorMessage = "会话已读请求参数无效。"
                });
            return;
        }

        var command = new RealtimeConversationMarkReadCommand
        {
            RequestId = requestId,
            UserId = session.UserId,
            ConversationId = request.ConversationId,
            ReadAtMs = request.ReadAtMs,
            ReadMessageId = request.ReadMessageId
        };

        try
        {
            var result = await _messageBus
                .MarkConversationReadAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendConversationMarkReadResponse(
                session,
                new ConversationMarkReadResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    UnreadCount = result.UnreadCount,
                    LastReadMessageId = result.LastReadMessageId,
                    LastReadAtMs = result.LastReadAtMs,
                    Changed = result.Changed
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.ConversationMarkReadRequest);
            _logger.CommandFailed(
                PacketCommand.ConversationMarkReadRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendConversationMarkReadResponse(
                session,
                new ConversationMarkReadResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "conversation_mark_read_unavailable",
                    ErrorMessage = "会话已读服务暂时不可用，请稍后重试。"
                });
        }
    }

    /// <summary>
    /// 设置会话偏好（置顶 / 免打扰）。Pinned 与 Muted 至少传一个；
    /// MutedUntilMs 为非正时视为校验失败，避免持久化非法免打扰截止时间。
    /// </summary>
    private async ValueTask HandleSetPrefsAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _setPrefsRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > MaxRequestIdLength
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.ConversationId.Length > MaxConversationIdLength
            || (request.Pinned is null && request.Muted is null)
            || request.MutedUntilMs is <= 0)
        {
            SendConversationSetPrefsResponse(
                session,
                new ConversationSetPrefsResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_conversation_set_prefs_request",
                    ErrorMessage = "会话偏好请求参数无效。"
                });
            return;
        }

        var command = new RealtimeConversationSetPrefsCommand
        {
            RequestId = requestId,
            UserId = session.UserId,
            ConversationId = request.ConversationId,
            Pinned = request.Pinned,
            Muted = request.Muted,
            MutedUntilMs = request.MutedUntilMs
        };

        try
        {
            var result = await _messageBus
                .SetConversationPrefsAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendConversationSetPrefsResponse(
                session,
                new ConversationSetPrefsResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    IsPinned = result.IsPinned,
                    IsMuted = result.IsMuted,
                    MutedUntilMs = result.MutedUntilMs,
                    Changed = result.Changed
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.ConversationSetPrefsRequest);
            _logger.CommandFailed(
                PacketCommand.ConversationSetPrefsRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendConversationSetPrefsResponse(
                session,
                new ConversationSetPrefsResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "conversation_set_prefs_unavailable",
                    ErrorMessage = "会话偏好服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendConversationMarkReadResponse(
        TcpClientSession session,
        ConversationMarkReadResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.ConversationMarkReadResponse,
            _markReadResponseCodec,
            response);
        session.TryQueue(frame);
    }

    private void SendConversationSetPrefsResponse(
        TcpClientSession session,
        ConversationSetPrefsResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.ConversationSetPrefsResponse,
            _setPrefsResponseCodec,
            response);
        session.TryQueue(frame);
    }
}
