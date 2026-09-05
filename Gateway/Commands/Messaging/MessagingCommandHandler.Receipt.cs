using System.Buffers;
using System.Security.Cryptography;
using System.Text;
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
using ChatApp.TcpGateway.Gateway.Serialization;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// MessageReceipt 命令处理部分（partial）。
/// 包含回执请求校验、发布、ACK 与 ReceiptCommandId 生成。
/// </summary>
internal sealed partial class MessagingCommandHandler
{
    private async ValueTask HandleMessageReceiptAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession receiver,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            receiver,
            PacketCommand.MessageReceipt,
            _messageReceiptRequestCodec,
            payload);
        if (request is null ||
            string.IsNullOrWhiteSpace(request.MessageId) ||
            request.MessageId.Length > 64 ||
            !Enum.IsDefined(request.State))
        {
            _metrics.ProtocolError();
            receiver.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var receiptType = (MessageReceiptType)(byte)request.State;
        var commandId = CreateReceiptCommandId(
            receiver.UserId,
            request.MessageId,
            receiptType);
        var command = new MessageReceiptCommand
        {
            CommandId = commandId,
            MessageId = request.MessageId,
            ReceiverUserId = receiver.UserId,
            ReceiverSessionId = receiver.SessionId
                ?? $"tcp-{receiver.ConnectionId}",
            ReceiptType = receiptType,
            OccurredAtMs = _timeProvider
                .GetUtcNow()
                .ToUnixTimeMilliseconds()
        };

        try
        {
            await _messageBus
                .PublishMessageReceiptAsync(command, cancellationToken)
                .ConfigureAwait(false);
            _metrics.ReceiptPublished();
            SendMessageReceiptAcknowledgement(
                receiver,
                command,
                accepted: true);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.ReceiptPublishFailed();
            _metrics.CommandFailed(PacketCommand.MessageReceipt);
            _logger.CommandFailed(
                PacketCommand.MessageReceipt,
                receiver.ConnectionId,
                commandId,
                exception);
            SendMessageReceiptAcknowledgement(
                receiver,
                command,
                accepted: false,
                errorCode: "message_bus_unavailable",
                errorMessage: "消息服务暂时不可用，请重试相同回执。");
        }
    }

    private void SendMessageReceiptAcknowledgement(
        TcpClientSession session,
        MessageReceiptCommand command,
        bool accepted,
        string? errorCode = null,
        string? errorMessage = null)
    {
        var acknowledgement = new MessageReceiptAcknowledgement
        {
            CommandId = command.CommandId,
            MessageId = command.MessageId,
            State = (MessageReceiptState)(byte)command.ReceiptType,
            Accepted = accepted,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            AcknowledgedUtc = _timeProvider.GetUtcNow().UtcDateTime
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageReceiptAcknowledgement,
            _messageReceiptAcknowledgementCodec,
            session,
            acknowledgement);
        session.TryQueue(outboundFrame);
    }

    // 20（long）+ 1（':'）+ messageId 最大 UTF8 字节数 + 1（':'）+ 3（byte）+ 余量。
    private const int ReceiptCommandIdScratchBytes =
        20 + 1 + (64 * 3) + 1 + 3 + 16;

    private static string CreateReceiptCommandId(
        long receiverUserId,
        string messageId,
        MessageReceiptType receiptType)
    {
        var maxIdBytes = Encoding.UTF8.GetMaxByteCount(messageId.Length);
        if (20 + 1 + maxIdBytes + 1 + 3 > ReceiptCommandIdScratchBytes)
            return CreateReceiptCommandIdSlow(receiverUserId, messageId, receiptType);

        Span<byte> scratch = stackalloc byte[ReceiptCommandIdScratchBytes];
        var written = 0;
        receiverUserId.TryFormat(scratch, out var idLen);
        written += idLen;
        scratch[written++] = (byte)':';
        written += Encoding.UTF8.GetBytes(messageId, scratch[written..]);
        scratch[written++] = (byte)':';
        ((byte)receiptType).TryFormat(scratch[written..], out var typeLen);
        written += typeLen;

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(scratch[..written], hash);
        return Convert.ToHexStringLower(hash);
    }

    private static string CreateReceiptCommandIdSlow(
        long receiverUserId,
        string messageId,
        MessageReceiptType receiptType)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{receiverUserId}:{messageId}:{(byte)receiptType}");
        return Convert.ToHexStringLower(
            SHA256.HashData(source));
    }
}
