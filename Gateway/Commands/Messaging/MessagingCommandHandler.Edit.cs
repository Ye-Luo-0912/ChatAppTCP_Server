using System.Buffers;
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
using RealtimeMessageEditCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageEditCommand;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// MessageEditRequest 命令处理部分（partial）。
/// 包含编辑请求校验（含控制字符检查）、EditMessageAsync 调用与 ACK。
/// </summary>
internal sealed partial class MessagingCommandHandler
{
    private async ValueTask HandleMessageEditRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            session,
            PacketCommand.MessageEditRequest,
            _messageEditRequestCodec,
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
        // Content 是 required string，但 JSON 仍可显式传入 "content": null。
        // System.Text.Json 默认不强制非空注解（需 RespectNullableAnnotations），
        // null 会被反序列化为 null，后续 request.Content.Length 抛 NullReferenceException，
        // 被 SessionRuntime 当作传输层错误关闭会话。
        // 此处补齐：null/空白检查 + 控制字符策略 + 字符长度 + UTF-8 字节长度。
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > 64
            || string.IsNullOrWhiteSpace(request.Content)
            || ContainsDisallowedControlChars(request.Content)
            || request.Content.Length > 65_536
            || Encoding.UTF8.GetByteCount(request.Content) > 65_536)
        {
            SendMessageEditAcknowledgement(
                session,
                new MessageEditAcknowledgement
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_message_edit_request",
                    ErrorMessage = "消息编辑请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageEditCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            Content = request.Content,
            SenderUserId = session.UserId,
            SenderSessionId = session.SessionId
                ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .EditMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendMessageEditAcknowledgement(
                session,
                new MessageEditAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Content = result.Content,
                    EditVersion = result.EditVersion,
                    EditedAtMs = result.EditedAtMs
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.MessageEditRequest);
            _logger.CommandFailed(
                PacketCommand.MessageEditRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendMessageEditAcknowledgement(
                session,
                new MessageEditAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_edit_unavailable",
                    ErrorMessage = "消息编辑服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendMessageEditAcknowledgement(
        TcpClientSession session,
        MessageEditAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageEditAck,
            _messageEditAcknowledgementCodec,
            session,
            response);
        session.TryQueue(outboundFrame);
    }

    /// <summary>
    /// 检查字符串是否包含不允许的控制字符。
    /// 允许 \t (U+0009)、\n (U+000A)、\r (U+000D)；
    /// 拒绝其他 C0 控制字符 (U+0000–U+0008, U+000B–U+000C, U+000E–U+001F)、
    /// DEL (U+007F) 和 C1 控制字符 (U+0080–U+009F)，
    /// 防止通过 JSON 字符串注入二进制数据。
    /// </summary>
    private static bool ContainsDisallowedControlChars(string content)
    {
        foreach (var c in content)
        {
            if (c < 0x20 && c is not '\t' and not '\n' and not '\r')
                return true;
            if (c is >= (char)0x7F and <= (char)0x9F)
                return true;
        }
        return false;
    }
}
