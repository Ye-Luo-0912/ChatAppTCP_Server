using ChatApp.TcpGateway.Core.Messaging;
using SharedAddReactionAcknowledgement = ChatApp.Shared.Protocol.Tcp.AddReactionAcknowledgement;
using SharedAddReactionRequest = ChatApp.Shared.Protocol.Tcp.AddReactionRequest;
using SharedAuthenticationRequest = ChatApp.Shared.Protocol.Tcp.AuthenticationRequest;
using SharedAuthenticationResponse = ChatApp.Shared.Protocol.Tcp.AuthenticationResponse;
using SharedChatMessage = ChatApp.Shared.Protocol.Tcp.ChatMessage;
using SharedMessageAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageAcknowledgement;
using SharedMessageEditAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageEditAcknowledgement;
using SharedMessageEditedUpdate = ChatApp.Shared.Protocol.Tcp.MessageEditedUpdate;
using SharedMessageEditRequest = ChatApp.Shared.Protocol.Tcp.MessageEditRequest;
using SharedMessageReceipt = ChatApp.Shared.Protocol.Tcp.MessageReceipt;
using SharedMessageReceiptAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageReceiptAcknowledgement;
using SharedMessageReceiptUpdated = ChatApp.Shared.Protocol.Tcp.MessageReceiptUpdated;
using SharedMessageRecallAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageRecallAcknowledgement;
using SharedMessageRecalledUpdate = ChatApp.Shared.Protocol.Tcp.MessageRecalledUpdate;
using SharedMessageRecallRequest = ChatApp.Shared.Protocol.Tcp.MessageRecallRequest;
using SharedReactionAddedUpdate = ChatApp.Shared.Protocol.Tcp.ReactionAddedUpdate;
using SharedReactionRemovedUpdate = ChatApp.Shared.Protocol.Tcp.ReactionRemovedUpdate;
using SharedRemoveReactionAcknowledgement = ChatApp.Shared.Protocol.Tcp.RemoveReactionAcknowledgement;
using SharedRemoveReactionRequest = ChatApp.Shared.Protocol.Tcp.RemoveReactionRequest;

namespace ChatApp.TcpGateway.Gateway.Serialization;

/// <summary>
/// 认证 / 聊天消息 / 回执 / 编辑 / 撤回 / 反应的本地 ↔ 共享映射。
/// </summary>
internal static partial class BinaryPayloadMapper
{
    // ──────────── 认证 ────────────

    private static SharedAuthenticationRequest ToShared(AuthenticationRequest request) => new()
    {
        AccessToken = request.AccessToken,
        DeviceIdHash = request.DeviceIdHash
    };

    private static AuthenticationRequest ToLocal(SharedAuthenticationRequest request) => new()
    {
        AccessToken = request.AccessToken,
        DeviceIdHash = request.DeviceIdHash
    };

    private static SharedAuthenticationResponse ToShared(AuthenticationResponse response) => new()
    {
        Success = response.Success,
        UserId = response.UserId,
        ErrorMessage = response.ErrorMessage,
        SessionId = response.SessionId,
        DeviceIdHash = response.DeviceIdHash,
        DeviceId = response.DeviceId,
        ResumeToken = response.ResumeToken
    };

    private static AuthenticationResponse ToLocal(SharedAuthenticationResponse response) => new()
    {
        Success = response.Success,
        UserId = response.UserId,
        ErrorMessage = response.ErrorMessage,
        SessionId = response.SessionId,
        DeviceIdHash = response.DeviceIdHash,
        DeviceId = response.DeviceId,
        ResumeToken = response.ResumeToken
    };

    // ──────────── 聊天消息 ────────────

    /// <summary>
    /// 本地 SentUtc(DateTime) ↔ 共享 SentAtMs(Unix ms)。
    /// 上行幂等键：本地把 clientMessageId 放在 MessageId（JSON 契约），二进制规范按
    /// ClientMessageId 承载，故两侧各自空缺时用对方回填（与客户端映射规则一致）。
    /// </summary>
    private static SharedChatMessage ToShared(ChatMessage message) => new()
    {
        ClientMessageId = message.ClientMessageId ?? message.MessageId,
        MessageId = message.MessageId ?? message.ClientMessageId ?? string.Empty,
        ConversationId = message.ConversationId,
        TargetUserId = message.TargetUserId,
        SenderUserId = message.SenderUserId,
        Content = message.Content ?? string.Empty,
        SentAtMs = ToUnixMs(message.SentUtc),
        AttachmentIds = message.AttachmentIds,
        Attachments = MapAttachmentRefs(message.Attachments),
        ReplyToMessageId = message.ReplyToMessageId,
        ReplyToSenderUserId = message.ReplyToSenderUserId,
        ReplyToPreview = message.ReplyToPreview,
        ForwardedFromMessageId = message.ForwardedFromMessageId,
        ForwardedFromSenderUserId = message.ForwardedFromSenderUserId,
        ForwardedFromPreview = message.ForwardedFromPreview,
        MentionedUserIds = message.MentionedUserIds,
        MentionedRoles = message.MentionedRoles
    };

    private static ChatMessage ToLocal(SharedChatMessage message) => new()
    {
        ClientMessageId = message.ClientMessageId,
        MessageId = string.IsNullOrEmpty(message.MessageId)
            ? message.ClientMessageId
            : message.MessageId,
        ConversationId = message.ConversationId,
        TargetUserId = message.TargetUserId,
        SenderUserId = message.SenderUserId,
        Content = message.Content,
        SentUtc = FromUnixMs(message.SentAtMs),
        AttachmentIds = message.AttachmentIds,
        Attachments = MapAttachmentRefs(message.Attachments),
        ReplyToMessageId = message.ReplyToMessageId,
        ReplyToSenderUserId = message.ReplyToSenderUserId,
        ReplyToPreview = message.ReplyToPreview,
        ForwardedFromMessageId = message.ForwardedFromMessageId,
        ForwardedFromSenderUserId = message.ForwardedFromSenderUserId,
        ForwardedFromPreview = message.ForwardedFromPreview,
        MentionedUserIds = message.MentionedUserIds,
        MentionedRoles = message.MentionedRoles
    };

    // ──────────── 消息 ACK ────────────

    private static SharedMessageAcknowledgement ToShared(MessageAcknowledgement ack) => new()
    {
        ClientMessageId = ack.ClientMessageId,
        CommandId = ack.CommandId,
        Accepted = ack.Accepted,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        AcknowledgedAtMs = ToUnixMs(ack.AcknowledgedUtc)
    };

    private static MessageAcknowledgement ToLocal(SharedMessageAcknowledgement ack) => new()
    {
        ClientMessageId = ack.ClientMessageId,
        CommandId = ack.CommandId,
        Accepted = ack.Accepted,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        AcknowledgedUtc = FromUnixMs(ack.AcknowledgedAtMs)
    };

    // ──────────── 回执 ────────────
    // 本地回执是"逐消息状态"（MessageId + Delivered/Read），共享 schema 是"已读水位"
    // （LastReadMessageId/LastReadAtMs/ReaderUserId）。网关把水位发布为 Read 回执，
    // 因此只有 Read 语义能在两种形状间无损往返；Delivered 无法承载（见下）。

    /// <summary>
    /// 网关从不编码 MessageReceipt 上行（C2S-only），本映射仅为契约对称与 round-trip 测试存在；
    /// 本地 State 在共享水位 schema 中无对应，恒按 Read 水位语义承载。
    /// </summary>
    private static SharedMessageReceipt ToShared(MessageReceiptRequest request) => new()
    {
        LastReadMessageId = request.MessageId
    };

    /// <summary>共享水位 → 逐消息形状：LastReadMessageId 即被已读的消息，状态恒 Read；
    /// RequestId/ConversationId/LastReadAtMs/ReceiverUserId 本地形状不承载，丢弃。</summary>
    private static MessageReceiptRequest ToLocal(SharedMessageReceipt receipt) => new()
    {
        MessageId = receipt.LastReadMessageId,
        State = MessageReceiptState.Read
    };

    /// <summary>
    /// 本地 ACK 的 MessageId/State/AcknowledgedUtc 在共享 schema（RequestId/Accepted/错误码）中
    /// 无对应字段，丢弃；CommandId 即共享侧的 RequestId（幂等键回显）。
    /// </summary>
    private static SharedMessageReceiptAcknowledgement ToShared(MessageReceiptAcknowledgement ack) => new()
    {
        RequestId = ack.CommandId,
        Accepted = ack.Accepted,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage
    };

    private static MessageReceiptAcknowledgement ToLocal(SharedMessageReceiptAcknowledgement ack) => new()
    {
        CommandId = ack.RequestId,
        // 共享 ACK 不区分回执类型；占位回填首个合法枚举值，消费端只匹配 RequestId/Accepted。
        State = MessageReceiptState.Delivered,
        Accepted = ack.Accepted,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage
    };

    /// <summary>
    /// MessageReceiptUpdated fanout：共享 schema 只有"已读水位"语义。
    /// State=Read 映射为水位推进；State=Delivered 在共享 schema 中无任何承载方式，
    /// 且把它伪装成已读水位会让对端错误标记消息已读——宁可选编码为空事件（客户端按
    /// 缺 ConversationId 忽略），绝不伪造已读状态。
    /// </summary>
    private static SharedMessageReceiptUpdated ToShared(MessageReceiptUpdate update) =>
        update.State == MessageReceiptState.Read
            ? new SharedMessageReceiptUpdated
            {
                LastReadMessageId = update.MessageId,
                ReaderUserId = update.ReceiverUserId,
                LastReadAtMs = ToUnixMs(update.OccurredUtc)
            }
            : new SharedMessageReceiptUpdated();

    private static MessageReceiptUpdate ToLocal(SharedMessageReceiptUpdated update) => new()
    {
        MessageId = update.LastReadMessageId,
        ReceiverUserId = update.ReaderUserId ?? 0,
        // 水位推进即已读；共享 schema 无 Delivered 概念。
        State = MessageReceiptState.Read,
        OccurredUtc = update.LastReadAtMs is { } atMs ? FromUnixMs(atMs) : default
    };

    // ──────────── 编辑 / 撤回 ────────────

    private static SharedMessageEditRequest ToShared(MessageEditRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId,
        Content = request.Content
    };

    private static MessageEditRequest ToLocal(SharedMessageEditRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId,
        Content = request.Content
    };

    private static SharedMessageEditAcknowledgement ToShared(MessageEditAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        Content = ack.Content,
        EditVersion = ack.EditVersion,
        EditedAtMs = ack.EditedAtMs
    };

    private static MessageEditAcknowledgement ToLocal(SharedMessageEditAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        Content = ack.Content,
        EditVersion = ack.EditVersion,
        EditedAtMs = ack.EditedAtMs
    };

    private static SharedMessageEditedUpdate ToShared(MessageEditedUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        SenderUserId = update.SenderUserId,
        ReceiverUserId = update.ReceiverUserId,
        Content = update.Content,
        EditVersion = update.EditVersion,
        EditedAtMs = update.EditedAtMs
    };

    private static MessageEditedUpdate ToLocal(SharedMessageEditedUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        SenderUserId = update.SenderUserId,
        ReceiverUserId = update.ReceiverUserId,
        Content = update.Content,
        EditVersion = update.EditVersion,
        EditedAtMs = update.EditedAtMs
    };

    private static SharedMessageRecallRequest ToShared(MessageRecallRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId
    };

    private static MessageRecallRequest ToLocal(SharedMessageRecallRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId
    };

    private static SharedMessageRecallAcknowledgement ToShared(MessageRecallAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        RecalledAtMs = ack.RecalledAtMs
    };

    private static MessageRecallAcknowledgement ToLocal(SharedMessageRecallAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        RecalledAtMs = ack.RecalledAtMs
    };

    private static SharedMessageRecalledUpdate ToShared(MessageRecalledUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        SenderUserId = update.SenderUserId,
        ReceiverUserId = update.ReceiverUserId,
        RecalledAtMs = update.RecalledAtMs
    };

    private static MessageRecalledUpdate ToLocal(SharedMessageRecalledUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        SenderUserId = update.SenderUserId,
        ReceiverUserId = update.ReceiverUserId,
        RecalledAtMs = update.RecalledAtMs
    };

    // ──────────── 反应 ────────────

    private static SharedAddReactionRequest ToShared(AddReactionRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId,
        Emoji = request.Emoji
    };

    private static AddReactionRequest ToLocal(SharedAddReactionRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId,
        Emoji = request.Emoji
    };

    private static SharedRemoveReactionRequest ToShared(RemoveReactionRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId,
        Emoji = request.Emoji
    };

    private static RemoveReactionRequest ToLocal(SharedRemoveReactionRequest request) => new()
    {
        RequestId = request.RequestId,
        MessageId = request.MessageId,
        Emoji = request.Emoji
    };

    /// <summary>Add/Remove 反应 ACK 两侧字段完全同名同语义，仅类型不同。</summary>
    private static SharedAddReactionAcknowledgement ToShared(AddReactionAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        Emoji = ack.Emoji,
        OccurredAtMs = ack.OccurredAtMs,
        EmojiCount = ack.EmojiCount
    };

    private static AddReactionAcknowledgement ToLocal(SharedAddReactionAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        Emoji = ack.Emoji,
        OccurredAtMs = ack.OccurredAtMs,
        EmojiCount = ack.EmojiCount
    };

    private static SharedRemoveReactionAcknowledgement ToShared(RemoveReactionAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        Emoji = ack.Emoji,
        OccurredAtMs = ack.OccurredAtMs,
        EmojiCount = ack.EmojiCount
    };

    private static RemoveReactionAcknowledgement ToLocal(SharedRemoveReactionAcknowledgement ack) => new()
    {
        RequestId = ack.RequestId,
        MessageId = ack.MessageId,
        Succeeded = ack.Succeeded,
        ErrorCode = ack.ErrorCode,
        ErrorMessage = ack.ErrorMessage,
        ConversationId = ack.ConversationId,
        Emoji = ack.Emoji,
        OccurredAtMs = ack.OccurredAtMs,
        EmojiCount = ack.EmojiCount
    };

    private static SharedReactionAddedUpdate ToShared(ReactionAddedUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        ReactorUserId = update.ReactorUserId,
        MessageSenderUserId = update.MessageSenderUserId,
        MessageReceiverUserId = update.MessageReceiverUserId,
        Emoji = update.Emoji,
        EmojiCount = update.EmojiCount,
        OccurredAtMs = update.OccurredAtMs
    };

    private static ReactionAddedUpdate ToLocal(SharedReactionAddedUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        ReactorUserId = update.ReactorUserId,
        MessageSenderUserId = update.MessageSenderUserId,
        MessageReceiverUserId = update.MessageReceiverUserId,
        Emoji = update.Emoji,
        EmojiCount = update.EmojiCount,
        OccurredAtMs = update.OccurredAtMs
    };

    private static SharedReactionRemovedUpdate ToShared(ReactionRemovedUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        ReactorUserId = update.ReactorUserId,
        MessageSenderUserId = update.MessageSenderUserId,
        MessageReceiverUserId = update.MessageReceiverUserId,
        Emoji = update.Emoji,
        EmojiCount = update.EmojiCount,
        OccurredAtMs = update.OccurredAtMs
    };

    private static ReactionRemovedUpdate ToLocal(SharedReactionRemovedUpdate update) => new()
    {
        MessageId = update.MessageId,
        ConversationId = update.ConversationId,
        ReactorUserId = update.ReactorUserId,
        MessageSenderUserId = update.MessageSenderUserId,
        MessageReceiverUserId = update.MessageReceiverUserId,
        Emoji = update.Emoji,
        EmojiCount = update.EmojiCount,
        OccurredAtMs = update.OccurredAtMs
    };
}
