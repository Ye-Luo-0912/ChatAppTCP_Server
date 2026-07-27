using System.Buffers;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using RealtimeMessageRecallCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageRecallCommand;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// MessageRecallRequest 命令处理部分（partial）。
/// 包含撤回请求校验、RecallMessageAsync 调用与 ACK。
/// </summary>
internal sealed partial class MessagingCommandHandler
{
    private async ValueTask HandleMessageRecallRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageRecallRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > 64)
        {
            SendMessageRecallAcknowledgement(
                session,
                new MessageRecallAcknowledgement
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_message_recall_request",
                    ErrorMessage = "消息撤回请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageRecallCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            SenderUserId = session.UserId,
            SenderSessionId = session.SessionId
                ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .RecallMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendMessageRecallAcknowledgement(
                session,
                new MessageRecallAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    RecalledAtMs = result.RecalledAtMs
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.MessageRecallRequest);
            _logger.CommandFailed(
                PacketCommand.MessageRecallRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendMessageRecallAcknowledgement(
                session,
                new MessageRecallAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_recall_unavailable",
                    ErrorMessage = "消息撤回服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendMessageRecallAcknowledgement(
        TcpClientSession session,
        MessageRecallAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageRecallAck,
            _messageRecallAcknowledgementCodec,
            response);
        session.TryQueue(outboundFrame);
    }
}
