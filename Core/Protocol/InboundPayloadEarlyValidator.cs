using System.Buffers;
using System.Text.Json;

namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// 在完整 JSON 反序列化与 NATS 投递之前，对 ChatMessage 载荷做廉价结构校验。
/// </summary>
public static class InboundPayloadEarlyValidator
{
    public const string PayloadTooLargeCode = "payload_too_large";
    public const string TooManyAttachmentsCode = "too_many_attachments";
    public const string InvalidAttachmentIdCode = "invalid_attachment_id";
    public const string InvalidJsonCode = "invalid_message_json";
    public const string EmptyMessageCode = "empty_message";

    public static bool IsPayloadWithinLimit(long payloadLength, int maxPayloadBytes) =>
        payloadLength >= 0 && payloadLength <= maxPayloadBytes;

    /// <summary>
    /// 扫描 ChatMessage JSON：附件数量/Id 长度。不分配业务对象。
    /// </summary>
    public static bool TryValidateChatMessage(
        ReadOnlySequence<byte> payload,
        int maxAttachments,
        int maxAttachmentIdLength,
        out string errorCode,
        out string errorMessage)
    {
        if (payload.IsEmpty)
        {
            errorCode = EmptyMessageCode;
            errorMessage = "聊天消息载荷为空。";
            return false;
        }

        try
        {
            var reader = new Utf8JsonReader(payload, isFinalBlock: true, state: default);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                errorCode = InvalidJsonCode;
                errorMessage = "聊天消息必须是 JSON 对象。";
                return false;
            }

            while (reader.Read())
            {
                if (reader.TokenType != JsonTokenType.PropertyName ||
                    reader.CurrentDepth != 1)
                {
                    continue;
                }

                if (!reader.ValueTextEquals("attachmentIds"u8))
                {
                    continue;
                }

                if (!reader.Read())
                {
                    errorCode = InvalidJsonCode;
                    errorMessage = "attachmentIds 字段不完整。";
                    return false;
                }

                if (reader.TokenType == JsonTokenType.Null)
                {
                    continue;
                }

                if (reader.TokenType != JsonTokenType.StartArray)
                {
                    errorCode = InvalidJsonCode;
                    errorMessage = "attachmentIds 必须是数组。";
                    return false;
                }

                var attachmentCount = 0;
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndArray)
                    {
                        break;
                    }

                    if (reader.TokenType is JsonTokenType.Comment)
                    {
                        continue;
                    }

                    if (reader.TokenType == JsonTokenType.Null)
                    {
                        errorCode = InvalidAttachmentIdCode;
                        errorMessage = "附件 Id 不能为空。";
                        return false;
                    }

                    if (reader.TokenType != JsonTokenType.String)
                    {
                        errorCode = InvalidJsonCode;
                        errorMessage = "attachmentIds 元素必须是字符串。";
                        return false;
                    }

                    attachmentCount++;
                    if (attachmentCount > maxAttachments)
                    {
                        errorCode = TooManyAttachmentsCode;
                        errorMessage = $"附件数量超过上限 {maxAttachments}。";
                        return false;
                    }

                    var idLength = reader.HasValueSequence
                        ? reader.ValueSequence.Length
                        : reader.ValueSpan.Length;
                    if (idLength == 0 || idLength > maxAttachmentIdLength)
                    {
                        errorCode = InvalidAttachmentIdCode;
                        errorMessage =
                            $"附件 Id 长度必须在 1..{maxAttachmentIdLength}。";
                        return false;
                    }
                }
            }

            errorCode = string.Empty;
            errorMessage = string.Empty;
            return true;
        }
        catch (JsonException)
        {
            errorCode = InvalidJsonCode;
            errorMessage = "聊天消息 JSON 无效。";
            return false;
        }
    }
}
