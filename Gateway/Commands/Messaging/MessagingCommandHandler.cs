using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RealtimeMessageRecallCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageRecallCommand;
using RealtimeMessageEditCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageEditCommand;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// 消息类命令处理器（ChatMessage / MessageReceipt / MessageRecallRequest / MessageEditRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="IRealtimeMessageBus"/>、
/// <see cref="TimeProvider"/> 与 <see cref="TcpGatewayOptions"/>，不再依赖 service 私有字段。
/// 行为与原内联 handler 完全等价（校验顺序、错误码、metric 与日志事件）。
/// </para>
/// </summary>
internal sealed class MessagingCommandHandler : ICommandHandler
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<ChatMessage> _chatMessageCodec;
    private readonly IPayloadCodec<MessageAcknowledgement> _messageAcknowledgementCodec;
    private readonly IPayloadCodec<MessageReceiptRequest> _messageReceiptRequestCodec;
    private readonly IPayloadCodec<MessageReceiptAcknowledgement> _messageReceiptAcknowledgementCodec;
    private readonly IPayloadCodec<MessageRecallRequest> _messageRecallRequestCodec;
    private readonly IPayloadCodec<MessageRecallAcknowledgement> _messageRecallAcknowledgementCodec;
    private readonly IPayloadCodec<MessageEditRequest> _messageEditRequestCodec;
    private readonly IPayloadCodec<MessageEditAcknowledgement> _messageEditAcknowledgementCodec;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MessagingCommandHandler> _logger;
    private readonly TcpGatewayOptions _options;

    public MessagingCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageAcknowledgement> messageAcknowledgementCodec,
        IPayloadCodec<MessageReceiptRequest> messageReceiptRequestCodec,
        IPayloadCodec<MessageReceiptAcknowledgement> messageReceiptAcknowledgementCodec,
        IPayloadCodec<MessageRecallRequest> messageRecallRequestCodec,
        IPayloadCodec<MessageRecallAcknowledgement> messageRecallAcknowledgementCodec,
        IPayloadCodec<MessageEditRequest> messageEditRequestCodec,
        IPayloadCodec<MessageEditAcknowledgement> messageEditAcknowledgementCodec,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<MessagingCommandHandler> logger,
        IOptions<TcpGatewayOptions> options)
    {
        _messageBus = messageBus;
        _chatMessageCodec = chatMessageCodec;
        _messageAcknowledgementCodec = messageAcknowledgementCodec;
        _messageReceiptRequestCodec = messageReceiptRequestCodec;
        _messageReceiptAcknowledgementCodec = messageReceiptAcknowledgementCodec;
        _messageRecallRequestCodec = messageRecallRequestCodec;
        _messageRecallAcknowledgementCodec = messageRecallAcknowledgementCodec;
        _messageEditRequestCodec = messageEditRequestCodec;
        _messageEditAcknowledgementCodec = messageEditAcknowledgementCodec;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
        _options = options.Value;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        ICommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.ChatMessage => HandleChatMessageAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.MessageReceipt => HandleMessageReceiptAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.MessageRecallRequest => HandleMessageRecallRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.MessageEditRequest => HandleMessageEditRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    private async ValueTask HandleChatMessageAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession sender,
        CancellationToken cancellationToken)
    {
        if (!InboundPayloadEarlyValidator.TryValidateChatMessage(
                payload,
                _options.MaxChatAttachments,
                ChatMessageLimits.MaxAttachmentIdLength,
                out var earlyErrorCode,
                out var earlyErrorMessage))
        {
            _metrics.ProtocolError();
            SendMessageAcknowledgement(
                sender,
                clientMessageId: string.Empty,
                commandId: string.Empty,
                accepted: false,
                errorCode: earlyErrorCode,
                errorMessage: earlyErrorMessage,
                closeAfterSend: SessionCloseReason.ProtocolViolation);
            return;
        }

        var message = _chatMessageCodec.Deserialize(payload);
        var hasAttachments = message?.AttachmentIds is { Count: > 0 };
        var hasReply = !string.IsNullOrWhiteSpace(message?.ReplyToMessageId);
        var hasForward = !string.IsNullOrWhiteSpace(message?.ForwardedFromMessageId);
        var isGroup = !string.IsNullOrWhiteSpace(message?.ConversationId)
                      && Realtime.Abstractions.Conversations.ConversationId.IsGroup(
                          message!.ConversationId);
        if (message is null ||
            (!isGroup && message.TargetUserId <= 0) ||
            (isGroup && message.ConversationId!.Length > 64) ||
            (string.IsNullOrWhiteSpace(message.Content) && !hasAttachments) ||
            message.MessageId?.Length > ChatMessageLimits.MaxClientMessageIdLength ||
            (message.AttachmentIds is { Count: > 0 } &&
             message.AttachmentIds.Count > _options.MaxChatAttachments) ||
            (message.AttachmentIds?.Any(static id =>
                string.IsNullOrWhiteSpace(id) ||
                id.Length > ChatMessageLimits.MaxAttachmentIdLength) == true) ||
            (hasReply && hasForward) ||
            (hasReply && (message.ReplyToMessageId!.Length >
                          ChatMessageLimits.MaxReplyToMessageIdLength
                          || message.ReplyToSenderUserId is null or <= 0)) ||
            (!hasReply && (message.ReplyToSenderUserId is not null
                           || !string.IsNullOrWhiteSpace(message.ReplyToPreview))) ||
            (hasForward && (message.ForwardedFromMessageId!.Length >
                            ChatMessageLimits.MaxForwardedFromMessageIdLength
                            || message.ForwardedFromSenderUserId is null or <= 0)) ||
            (!hasForward && (message.ForwardedFromSenderUserId is not null
                             || !string.IsNullOrWhiteSpace(message.ForwardedFromPreview))))
        {
            _metrics.ProtocolError();
            var rejectedMessageId = message?.MessageId is { Length: > 0 and <= ChatMessageLimits.MaxClientMessageIdLength }
                ? message.MessageId
                : string.Empty;
            SendMessageAcknowledgement(
                sender,
                clientMessageId: rejectedMessageId,
                commandId: string.Empty,
                accepted: false,
                errorCode: "invalid_message",
                errorMessage: "聊天消息参数无效。",
                closeAfterSend: SessionCloseReason.ProtocolViolation);
            return;
        }

        var clientMessageId = string.IsNullOrWhiteSpace(message.MessageId)
            ? Guid.CreateVersion7().ToString("N")
            : message.MessageId;
        var commandId = CreateCommandId(
            sender.UserId,
            clientMessageId);
        var command = new IncomingMessageCommand
        {
            CommandId = commandId,
            ClientMessageId = clientMessageId,
            SenderUserId = sender.UserId,
            SenderSessionId = sender.SessionId
                ?? $"tcp-{sender.ConnectionId}",
            ReceiverUserId = isGroup ? 0 : message.TargetUserId,
            ConversationId = isGroup ? message.ConversationId!.Trim() : null,
            Content = message.Content ?? string.Empty,
            AttachmentIds = message.AttachmentIds,
            ReplyToMessageId = string.IsNullOrWhiteSpace(message.ReplyToMessageId)
                ? null
                : message.ReplyToMessageId.Trim(),
            ReplyToSenderUserId = message.ReplyToSenderUserId,
            ReplyToPreview = string.IsNullOrWhiteSpace(message.ReplyToPreview)
                ? null
                : TruncateReplyPreview(message.ReplyToPreview),
            ForwardedFromMessageId = string.IsNullOrWhiteSpace(message.ForwardedFromMessageId)
                ? null
                : message.ForwardedFromMessageId.Trim(),
            ForwardedFromSenderUserId = message.ForwardedFromSenderUserId,
            ForwardedFromPreview = string.IsNullOrWhiteSpace(message.ForwardedFromPreview)
                ? null
                : TruncateForwardedPreview(message.ForwardedFromPreview),
            MentionedUserIds = NormalizeMentionedUserIds(message.MentionedUserIds, isGroup, sender.UserId),
            MentionedRoles = NormalizeMentionedRoles(message.MentionedRoles, isGroup),
            ReceivedAtMs = _timeProvider
                .GetUtcNow()
                .ToUnixTimeMilliseconds()
        };

        try
        {
            await _messageBus
                .PublishIncomingMessageAsync(
                    command,
                    cancellationToken)
                .ConfigureAwait(false);
            _metrics.MessagePublished();

            SendMessageAcknowledgement(
                sender,
                clientMessageId,
                commandId,
                accepted: true);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.MessagePublishFailed();
            _metrics.CommandFailed(PacketCommand.ChatMessage);
            _logger.CommandFailed(
                PacketCommand.ChatMessage,
                sender.ConnectionId,
                commandId,
                exception);

            SendMessageAcknowledgement(
                sender,
                clientMessageId,
                commandId,
                accepted: false,
                errorCode: "message_bus_unavailable",
                errorMessage: "消息服务暂时不可用，请使用相同 ClientMessageId 重试。");
        }
    }

    private async ValueTask HandleMessageReceiptAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession receiver,
        CancellationToken cancellationToken)
    {
        var request = _messageReceiptRequestCodec.Deserialize(payload);
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

    private async ValueTask HandleMessageEditRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageEditRequestCodec.Deserialize(payload);
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
            || request.MessageId.Length > 64
            || request.Content.Length > 65_536)
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

    private void SendMessageAcknowledgement(
        TcpClientSession session,
        string clientMessageId,
        string commandId,
        bool accepted,
        string? errorCode = null,
        string? errorMessage = null,
        SessionCloseReason? closeAfterSend = null)
    {
        var acknowledgement = new MessageAcknowledgement
        {
            ClientMessageId = clientMessageId,
            CommandId = commandId,
            Accepted = accepted,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            AcknowledgedUtc = _timeProvider.GetUtcNow().UtcDateTime
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageAcknowledgement,
            _messageAcknowledgementCodec,
            acknowledgement);
        if (!session.TryQueue(outboundFrame, closeAfterSend) &&
            closeAfterSend is { } reason)
        {
            session.Close(reason);
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
            acknowledgement);
        session.TryQueue(outboundFrame);
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

    private void SendMessageEditAcknowledgement(
        TcpClientSession session,
        MessageEditAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageEditAck,
            _messageEditAcknowledgementCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private static string TruncateReplyPreview(string preview)
    {
        var trimmed = preview.Trim();
        return trimmed.Length <= ChatMessageLimits.MaxReplyPreviewLength
            ? trimmed
            : trimmed[..ChatMessageLimits.MaxReplyPreviewLength];
    }

    private static string TruncateForwardedPreview(string preview)
    {
        var trimmed = preview.Trim();
        return trimmed.Length <= ChatMessageLimits.MaxForwardedFromPreviewLength
            ? trimmed
            : trimmed[..ChatMessageLimits.MaxForwardedFromPreviewLength];
    }

    /// <summary>
    /// 规整 @ 用户 Id 列表：非群聊返回 null；去重、去自提及非正 Id；超额截断。
    /// </summary>
    internal static List<long>? NormalizeMentionedUserIds(
        IReadOnlyList<long>? raw,
        bool isGroup,
        long senderUserId)
    {
        if (!isGroup || raw is null || raw.Count == 0)
            return null;

        var seen = new HashSet<long>();
        var result = new List<long>(Math.Min(raw.Count, ChatMessageLimits.MaxMentionedUserIds));
        foreach (var id in raw)
        {
            if (id <= 0 || id == senderUserId)
                continue;
            if (seen.Add(id))
                result.Add(id);
            if (result.Count >= ChatMessageLimits.MaxMentionedUserIds)
                break;
        }

        return result.Count == 0 ? null : result;
    }

    /// <summary>
    /// 规整 @ 角色列表：非群聊返回 null；去空白项与重复项；按长度与数量上限截断。
    /// </summary>
    internal static List<string>? NormalizeMentionedRoles(
        IReadOnlyList<string>? raw,
        bool isGroup)
    {
        if (!isGroup || raw is null || raw.Count == 0)
            return null;

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>(Math.Min(raw.Count, ChatMessageLimits.MaxMentionedRoles));
        foreach (var role in raw)
        {
            if (string.IsNullOrWhiteSpace(role))
                continue;
            var trimmed = role.Trim();
            if (trimmed.Length > ChatMessageLimits.MaxMentionedRoleLength)
                trimmed = trimmed[..ChatMessageLimits.MaxMentionedRoleLength];
            if (seen.Add(trimmed))
                result.Add(trimmed);
            if (result.Count >= ChatMessageLimits.MaxMentionedRoles)
                break;
        }

        return result.Count == 0 ? null : result;
    }

    // 20（long 含符号最大位数）+ 1（':'）+ clientMessageId 最大 UTF8 字节数 + 余量。
    private const int CommandIdScratchBytes =
        20 + 1 + (ChatMessageLimits.MaxClientMessageIdLength * 3) + 16;

    private static string CreateCommandId(
        long senderUserId,
        string clientMessageId)
    {
        var maxIdBytes = Encoding.UTF8.GetMaxByteCount(clientMessageId.Length);
        if (20 + 1 + maxIdBytes > CommandIdScratchBytes)
            return CreateCommandIdSlow(senderUserId, clientMessageId);

        Span<byte> scratch = stackalloc byte[CommandIdScratchBytes];
        var written = 0;
        senderUserId.TryFormat(scratch, out var idLen);
        written += idLen;
        scratch[written++] = (byte)':';
        written += Encoding.UTF8.GetBytes(clientMessageId, scratch[written..]);

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(scratch[..written], hash);
        return Convert.ToHexStringLower(hash);
    }

    private static string CreateCommandIdSlow(
        long senderUserId,
        string clientMessageId)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(
            SHA256.HashData(source));
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
