using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using System.Buffers;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeMessageReactionAction =
    ChatApp.Realtime.Abstractions.Messaging.MessageReactionAction;
using RealtimeMessageReactionCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageReactionCommand;

namespace ChatApp.TcpGateway.Gateway.Commands.Reactions;

/// <summary>
/// 消息反应相关命令处理器（AddReactionRequest / RemoveReactionRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="IRealtimeMessageBus"/>、
/// <see cref="TimeProvider"/>，不再依赖 service 私有字段。行为与原内联 handler 完全等价
/// （校验顺序、错误码、metric 与日志事件）。
/// </para>
/// </summary>
internal sealed class ReactionCommandHandler : ICommandHandler
{
    private const int MaxRequestIdLength = 64;
    private const int MaxMessageIdLength = 64;
    private const int MaxEmojiLength = 32;

    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<AddReactionRequest> _addRequestCodec;
    private readonly IPayloadCodec<AddReactionAcknowledgement> _addAckCodec;
    private readonly IPayloadCodec<RemoveReactionRequest> _removeRequestCodec;
    private readonly IPayloadCodec<RemoveReactionAcknowledgement> _removeAckCodec;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ReactionCommandHandler> _logger;

    public ReactionCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<AddReactionRequest> addRequestCodec,
        IPayloadCodec<AddReactionAcknowledgement> addAckCodec,
        IPayloadCodec<RemoveReactionRequest> removeRequestCodec,
        IPayloadCodec<RemoveReactionAcknowledgement> removeAckCodec,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<ReactionCommandHandler> logger)
    {
        _messageBus = messageBus;
        _addRequestCodec = addRequestCodec;
        _addAckCodec = addAckCodec;
        _removeRequestCodec = removeRequestCodec;
        _removeAckCodec = removeAckCodec;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.AddReactionRequest => HandleAddAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.RemoveReactionRequest => HandleRemoveAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    private async ValueTask HandleAddAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _addRequestCodec.Deserialize(payload);
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
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > MaxMessageIdLength
            || string.IsNullOrWhiteSpace(request.Emoji)
            || request.Emoji.Trim().Length > MaxEmojiLength)
        {
            SendAddAck(
                session,
                new AddReactionAcknowledgement
                {
                    RequestId = requestId.Length <= MaxRequestIdLength ? requestId : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_add_reaction_request",
                    ErrorMessage = "添加反应请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageReactionCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            Emoji = request.Emoji.Trim(),
            Action = RealtimeMessageReactionAction.Add,
            ActorUserId = session.UserId,
            ActorSessionId = session.SessionId ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .ReactToMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendAddAck(
                session,
                new AddReactionAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Emoji = result.Emoji,
                    OccurredAtMs = result.OccurredAtMs,
                    EmojiCount = result.EmojiCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.AddReactionRequest);
            _logger.CommandFailed(
                PacketCommand.AddReactionRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendAddAck(
                session,
                new AddReactionAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_reaction_unavailable",
                    ErrorMessage = "消息反应服务暂时不可用，请稍后重试。"
                });
        }
    }

    private async ValueTask HandleRemoveAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _removeRequestCodec.Deserialize(payload);
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
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > MaxMessageIdLength
            || string.IsNullOrWhiteSpace(request.Emoji)
            || request.Emoji.Trim().Length > MaxEmojiLength)
        {
            SendRemoveAck(
                session,
                new RemoveReactionAcknowledgement
                {
                    RequestId = requestId.Length <= MaxRequestIdLength ? requestId : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_remove_reaction_request",
                    ErrorMessage = "移除反应请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageReactionCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            Emoji = request.Emoji.Trim(),
            Action = RealtimeMessageReactionAction.Remove,
            ActorUserId = session.UserId,
            ActorSessionId = session.SessionId ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .ReactToMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendRemoveAck(
                session,
                new RemoveReactionAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Emoji = result.Emoji,
                    OccurredAtMs = result.OccurredAtMs,
                    EmojiCount = result.EmojiCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.RemoveReactionRequest);
            _logger.CommandFailed(
                PacketCommand.RemoveReactionRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendRemoveAck(
                session,
                new RemoveReactionAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_reaction_unavailable",
                    ErrorMessage = "消息反应服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendAddAck(
        TcpClientSession session,
        AddReactionAcknowledgement response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.AddReactionAck,
            _addAckCodec,
            response);
        session.TryQueue(frame);
    }

    private void SendRemoveAck(
        TcpClientSession session,
        RemoveReactionAcknowledgement response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.RemoveReactionAck,
            _removeAckCodec,
            response);
        session.TryQueue(frame);
    }
}
