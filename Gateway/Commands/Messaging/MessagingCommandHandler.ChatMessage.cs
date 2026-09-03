using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ChatApp.TcpGateway.Gateway.Serialization;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// ChatMessage 命令处理部分（partial）。
/// 包含 ChatMessage 校验、发布、ACK 与辅助方法（CommandId 生成、@ 列表规整、preview 截断）。
/// </summary>
internal sealed partial class MessagingCommandHandler
{
    private async ValueTask HandleChatMessageAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession sender,
        CancellationToken cancellationToken)
    {
        // 早检是 JSON 结构扫描（Utf8JsonReader），只对 JSON 会话有意义；
        // 二进制会话的 ChatMessage 走 schema 解码 + 下方同一套业务校验，跳过 JSON 早检。
        if (sender.NegotiatedPayloadFormat == PayloadFormat.Json &&
            !InboundPayloadEarlyValidator.TryValidateChatMessage(
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

        var message = SessionPayload.Deserialize(
            sender,
            PacketCommand.ChatMessage,
            _chatMessageCodec,
            payload);
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

        // VOICE-MSG-2：把消息里出现的附件元数据快照带上（AttachmentIds 对齐）。
        // 历史消息经附件注册表回查构建，语音 6 字段需经此快照持久化到注册表。
        var attachmentMetadata = MapUplinkAttachmentMetadata(
            message.AttachmentIds,
            message.Attachments);

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
            Attachments = attachmentMetadata,
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
            session,
            acknowledgement);
        if (!session.TryQueue(outboundFrame, closeAfterSend) &&
            closeAfterSend is { } reason)
        {
            session.Close(reason);
        }
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

    /// <summary>
    /// VOICE-MSG-2：提取上行 ChatMessage.Attachments 中与本消息 AttachmentIds 对齐的元数据快照。
    /// 只保留消息里出现的附件（按 attachment_id 匹配）；上行未携带引用或引用与 ids 无交集时
    /// 返回 null（仅 id 上行的旧客户端路径不受影响）。元数据由 Realtime 绑定链路持久化到附件注册表，
    /// 语音 6 字段经注册表在历史回查时带出。
    /// </summary>
    internal static IReadOnlyList<AttachmentRef>? MapUplinkAttachmentMetadata(
        IReadOnlyList<string>? attachmentIds,
        IReadOnlyList<AttachmentRef>? uplinkAttachments)
    {
        if (uplinkAttachments is not { Count: > 0 })
            return null;

        List<AttachmentRef>? metadata = null;
        if (attachmentIds is { Count: > 0 })
        {
            foreach (var reference in uplinkAttachments)
            {
                if (reference?.AttachmentId is not { Length: > 0 } referenceId
                    || !attachmentIds.Contains(referenceId))
                {
                    continue;
                }

                metadata ??= new List<AttachmentRef>(uplinkAttachments.Count);
                metadata.Add(reference);
            }
        }

        return metadata is { Count: > 0 } ? metadata : null;
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
}
