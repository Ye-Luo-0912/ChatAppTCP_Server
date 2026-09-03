using ChatApp.Binary.Core;
using ChatApp.Shared.Protocol.Tcp.Binary.Schemas;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Gateway.Serialization;
using Xunit;
// Realtime 契约与共享协议类型（MessageHistory*/SyncBootstrap*/ProtocolErrorFrame/GoAway/
// TcpRelationshipList* 等）已由项目级 global using 提供正确绑定。
using PushPlatform = ChatApp.Realtime.Abstractions.Push.PushPlatform;
using SharedConversationListPage = ChatApp.Shared.Protocol.Tcp.ConversationListPage;
using SharedMessageReceipt = ChatApp.Shared.Protocol.Tcp.MessageReceipt;
using SharedMessageReceiptUpdated = ChatApp.Shared.Protocol.Tcp.MessageReceiptUpdated;
using TcpCallCommandRequest = ChatApp.Shared.Protocol.Tcp.TcpCallCommandRequest;
using TcpCallCommandResponse = ChatApp.Shared.Protocol.Tcp.TcpCallCommandResponse;
using TcpCallSignal = ChatApp.Shared.Protocol.Tcp.TcpCallSignal;
using TcpConversationListCursor = ChatApp.Shared.Protocol.Tcp.TcpConversationListCursor;
using TcpConversationListItem = ChatApp.Shared.Protocol.Tcp.TcpConversationListItem;
using TcpPresenceChanged = ChatApp.Shared.Protocol.Tcp.TcpPresenceChanged;
using TcpPushPlatform = ChatApp.Shared.Protocol.Tcp.TcpPushPlatform;
using TcpCallState = ChatApp.Shared.Protocol.Tcp.TcpCallState;

namespace ChatApp.TcpGateway.Tests.Serialization;

/// <summary>
/// 网关本地 DTO ↔ chatapp-bin-v1 共享规范 DTO 映射的 round-trip 契约测试：
/// 本地 DTO → ToShared → TcpBinaryWireEncoder 编码 → TcpBinaryWireCodec 按命令解码
/// → ToLocal → 逐字段相等。wire 类型即共享类型的命令直接验证 ToShared/ToLocal 恒等。
/// </summary>
public sealed class BinaryPayloadMapperTests
{
    private static readonly DateTime SampleUtc =
        new(2026, 8, 30, 12, 34, 56, DateTimeKind.Utc);

    private static byte[] EncodeShared(object shared)
    {
        var buffer = new byte[BinaryLimits.Default.MaxMessageBytes];
        var result = TcpBinaryWireEncoder.TryEncode(shared, buffer, BinaryLimits.Default);
        Assert.Equal(TcpBinaryWireEncodeStatus.Encoded, result.Status);
        return buffer.AsSpan(0, result.Written).ToArray();
    }

    private static object DecodeShared(PacketCommand command, byte[] payload)
    {
        var decode = TcpBinaryWireCodec.TryDecode(command, payload, BinaryLimits.Default);
        Assert.Equal(TcpBinaryWireStatus.Decoded, decode.Status);
        return decode.Value!;
    }

    /// <summary>完整往返：本地 → 共享 → 二进制 → 共享 → 本地。</summary>
    private static TLocal RoundTrip<TLocal>(PacketCommand command, object localDto)
        where TLocal : class =>
        BinaryPayloadMapper.ToLocal<TLocal>(
            command,
            DecodeShared(command, EncodeShared(BinaryPayloadMapper.ToShared(command, localDto))))!;

    // ──────────── 认证 ────────────

    [Fact]
    public void AuthenticationRequest_RoundTrips()
    {
        var local = new AuthenticationRequest
        {
            AccessToken = "token-abc",
            DeviceIdHash = 0x1234_5678_9ABC_DEF0
        };

        var back = RoundTrip<AuthenticationRequest>(PacketCommand.AuthenticationRequest, local);

        Assert.Equal("token-abc", back.AccessToken);
        Assert.Equal(0x1234_5678_9ABC_DEF0u, back.DeviceIdHash);
    }

    [Fact]
    public void AuthenticationResponse_RoundTripsAllFields()
    {
        var local = new AuthenticationResponse
        {
            Success = true,
            UserId = 42,
            ErrorMessage = null,
            SessionId = "session-1",
            DeviceIdHash = 99,
            DeviceId = "device-1",
            ResumeToken = "resume-1"
        };

        var back = RoundTrip<AuthenticationResponse>(PacketCommand.AuthenticationResponse, local);

        Assert.True(back.Success);
        Assert.Equal(42, back.UserId);
        Assert.Equal("session-1", back.SessionId);
        Assert.Equal(99u, back.DeviceIdHash);
        Assert.Equal("device-1", back.DeviceId);
        Assert.Equal("resume-1", back.ResumeToken);
    }

    [Fact]
    public void AuthenticationResponse_FailureRoundTrips()
    {
        var local = new AuthenticationResponse
        {
            Success = false,
            ErrorMessage = "denied"
        };

        var back = RoundTrip<AuthenticationResponse>(PacketCommand.AuthenticationResponse, local);

        Assert.False(back.Success);
        Assert.Equal("denied", back.ErrorMessage);
        Assert.Equal(0, back.UserId);
    }

    // ──────────── 聊天消息 ────────────

    [Fact]
    public void ChatMessage_RoundTripsAllFieldsIncludingVoiceAttachments()
    {
        var local = new ChatMessage
        {
            ClientMessageId = "client-1",
            MessageId = "client-1",
            ConversationId = "conv-1",
            TargetUserId = 1001,
            SenderUserId = 2002,
            Content = "hello binary",
            SentUtc = SampleUtc,
            AttachmentIds = ["att-1", "att-2"],
            Attachments =
            [
                new AttachmentRef
                {
                    AttachmentId = "att-1",
                    ContentType = "image/png",
                    FileName = "pic.png",
                    SizeBytes = 1234,
                    Status = AttachmentWireStatus.Available,
                    DownloadApiHint = "att-1",
                    DownloadToken = "tok-1",
                    ThumbnailApiHint = "thumb-1"
                },
                new AttachmentRef
                {
                    AttachmentId = "att-2",
                    ContentType = "audio/ogg",
                    SizeBytes = 555,
                    Status = AttachmentWireStatus.Scanning,
                    IsVoice = true,
                    VoiceCodec = "opus",
                    VoiceContainer = "ogg",
                    VoiceDurationMs = 4321,
                    VoiceSampleRateHz = 48000,
                    VoiceChannels = 1,
                    VoiceWaveformPeaks = [1, 32, 200, 255, 64]
                }
            ],
            ReplyToMessageId = "reply-1",
            ReplyToSenderUserId = 3003,
            ReplyToPreview = "prev",
            MentionedUserIds = [4004],
            MentionedRoles = ["all"]
        };

        var back = RoundTrip<ChatMessage>(PacketCommand.ChatMessage, local);

        Assert.Equal("client-1", back.ClientMessageId);
        Assert.Equal("client-1", back.MessageId);
        Assert.Equal("conv-1", back.ConversationId);
        Assert.Equal(1001, back.TargetUserId);
        Assert.Equal(2002, back.SenderUserId);
        Assert.Equal("hello binary", back.Content);
        Assert.Equal(SampleUtc, back.SentUtc);
        Assert.Equal(["att-1", "att-2"], back.AttachmentIds);
        Assert.Equal("reply-1", back.ReplyToMessageId);
        Assert.Equal(3003, back.ReplyToSenderUserId);
        Assert.Equal("prev", back.ReplyToPreview);
        Assert.Equal([4004], back.MentionedUserIds);
        Assert.Equal(["all"], back.MentionedRoles);

        Assert.NotNull(back.Attachments);
        Assert.Equal(2, back.Attachments!.Count);
        var plain = back.Attachments[0];
        Assert.Equal("att-1", plain.AttachmentId);
        Assert.Equal("image/png", plain.ContentType);
        Assert.Equal("pic.png", plain.FileName);
        Assert.Equal(1234, plain.SizeBytes);
        Assert.Equal(AttachmentWireStatus.Available, plain.Status);
        Assert.Equal("att-1", plain.DownloadApiHint);
        Assert.Equal("tok-1", plain.DownloadToken);
        Assert.Equal("thumb-1", plain.ThumbnailApiHint);
        Assert.False(plain.IsVoice);
        var voice = back.Attachments[1];
        Assert.True(voice.IsVoice);
        Assert.Equal("opus", voice.VoiceCodec);
        Assert.Equal("ogg", voice.VoiceContainer);
        Assert.Equal(4321, voice.VoiceDurationMs);
        Assert.Equal(48000, voice.VoiceSampleRateHz);
        Assert.Equal((short)1, voice.VoiceChannels);
        // VOICE-MSG-2 waveform：波形峰值经二进制 wire（TcpAttachmentRef field 16）往返保真。
        Assert.Equal(new byte[] { 1, 32, 200, 255, 64 }, voice.VoiceWaveformPeaks);
        Assert.Null(plain.VoiceWaveformPeaks);
    }

    // ──────────── 消息 ACK / 回执 / 编辑 / 撤回 ────────────

    [Fact]
    public void MessageAcknowledgement_RoundTrips()
    {
        var local = new MessageAcknowledgement
        {
            ClientMessageId = "cm-1",
            CommandId = "cmd-1",
            Accepted = true,
            ErrorCode = null,
            ErrorMessage = null,
            AcknowledgedUtc = SampleUtc
        };

        var back = RoundTrip<MessageAcknowledgement>(PacketCommand.MessageAcknowledgement, local);

        Assert.Equal("cm-1", back.ClientMessageId);
        Assert.Equal("cmd-1", back.CommandId);
        Assert.True(back.Accepted);
        Assert.Equal(SampleUtc, back.AcknowledgedUtc);
    }

    [Fact]
    public void MessageReceiptRequest_WatermarkRoundTripsAsRead()
    {
        // 本地逐消息形状 ↔ 共享已读水位：水位消息即被已读消息，状态恒 Read。
        var local = new MessageReceiptRequest
        {
            MessageId = "msg-1",
            State = MessageReceiptState.Read
        };

        var back = RoundTrip<MessageReceiptRequest>(PacketCommand.MessageReceipt, local);

        Assert.Equal("msg-1", back.MessageId);
        Assert.Equal(MessageReceiptState.Read, back.State);
    }

    [Fact]
    public void MessageReceiptRequest_ToSharedCarriesWatermarkOnly()
    {
        var shared = (SharedMessageReceipt)BinaryPayloadMapper.ToShared(
            PacketCommand.MessageReceipt,
            new MessageReceiptRequest { MessageId = "msg-1", State = MessageReceiptState.Read });

        Assert.Equal("msg-1", shared.LastReadMessageId);
        Assert.Null(shared.RequestId);
        Assert.Null(shared.ConversationId);
        Assert.Null(shared.LastReadAtMs);
        Assert.Null(shared.ReaderUserId);
    }

    [Fact]
    public void MessageReceiptAcknowledgement_RoundTripsRequestIdAndOutcome()
    {
        var local = new MessageReceiptAcknowledgement
        {
            CommandId = "cmd-9",
            MessageId = "msg-9",
            State = MessageReceiptState.Read,
            Accepted = false,
            ErrorCode = "message_bus_unavailable",
            ErrorMessage = "服务暂时不可用",
            AcknowledgedUtc = SampleUtc
        };

        var back = RoundTrip<MessageReceiptAcknowledgement>(
            PacketCommand.MessageReceiptAcknowledgement, local);

        // 共享 ACK schema 只承载 RequestId/Accepted/错误码；MessageId/State/AcknowledgedUtc 被丢弃，
        // State 回填 Delivered 占位。
        Assert.Equal("cmd-9", back.CommandId);
        Assert.False(back.Accepted);
        Assert.Equal("message_bus_unavailable", back.ErrorCode);
        Assert.Equal("服务暂时不可用", back.ErrorMessage);
        Assert.Null(back.MessageId);
        Assert.Equal(MessageReceiptState.Delivered, back.State);
    }

    [Fact]
    public void MessageReceiptUpdated_ReadStateRoundTripsAsWatermark()
    {
        var local = new MessageReceiptUpdate
        {
            MessageId = "msg-2",
            ReceiverUserId = 77,
            State = MessageReceiptState.Read,
            OccurredUtc = SampleUtc
        };

        var back = RoundTrip<MessageReceiptUpdate>(PacketCommand.MessageReceiptUpdated, local);

        Assert.Equal("msg-2", back.MessageId);
        Assert.Equal(77, back.ReceiverUserId);
        Assert.Equal(MessageReceiptState.Read, back.State);
        Assert.Equal(SampleUtc, back.OccurredUtc);
    }

    [Fact]
    public void MessageReceiptUpdated_DeliveredEncodesAsEmptyEvent()
    {
        // 共享 schema 只有已读水位语义；Delivered 不能伪装成已读，编码为空事件由客户端忽略。
        var shared = (SharedMessageReceiptUpdated)BinaryPayloadMapper.ToShared(
            PacketCommand.MessageReceiptUpdated,
            new MessageReceiptUpdate
            {
                MessageId = "msg-3",
                ReceiverUserId = 88,
                State = MessageReceiptState.Delivered,
                OccurredUtc = SampleUtc
            });

        var payload = EncodeShared(shared);
        Assert.Empty(payload);
    }

    [Fact]
    public void MessageEditRequestAndAck_RoundTrip()
    {
        var request = new MessageEditRequest
        {
            RequestId = "req-e1",
            MessageId = "msg-e1",
            Content = "edited text"
        };
        var backRequest = RoundTrip<MessageEditRequest>(PacketCommand.MessageEditRequest, request);
        Assert.Equal("req-e1", backRequest.RequestId);
        Assert.Equal("msg-e1", backRequest.MessageId);
        Assert.Equal("edited text", backRequest.Content);

        var ack = new MessageEditAcknowledgement
        {
            RequestId = "req-e1",
            MessageId = "msg-e1",
            Succeeded = true,
            ErrorCode = null,
            ErrorMessage = null,
            ConversationId = "conv-1",
            Content = "edited text",
            EditVersion = 3,
            EditedAtMs = 1725015296000
        };
        var backAck = RoundTrip<MessageEditAcknowledgement>(PacketCommand.MessageEditAck, ack);
        Assert.Equal("req-e1", backAck.RequestId);
        Assert.True(backAck.Succeeded);
        Assert.Equal("conv-1", backAck.ConversationId);
        Assert.Equal("edited text", backAck.Content);
        Assert.Equal(3, backAck.EditVersion);
        Assert.Equal(1725015296000, backAck.EditedAtMs);
    }

    [Fact]
    public void MessageEditedUpdate_RoundTrips()
    {
        var local = new MessageEditedUpdate
        {
            MessageId = "msg-e2",
            ConversationId = "conv-2",
            SenderUserId = 11,
            ReceiverUserId = 22,
            Content = "new content",
            EditVersion = 2,
            EditedAtMs = 1725015296123
        };

        var back = RoundTrip<MessageEditedUpdate>(PacketCommand.MessageEdited, local);

        Assert.Equal("msg-e2", back.MessageId);
        Assert.Equal("conv-2", back.ConversationId);
        Assert.Equal(11, back.SenderUserId);
        Assert.Equal(22, back.ReceiverUserId);
        Assert.Equal("new content", back.Content);
        Assert.Equal(2, back.EditVersion);
        Assert.Equal(1725015296123, back.EditedAtMs);
    }

    [Fact]
    public void MessageRecallRequestAckAndUpdate_RoundTrip()
    {
        var request = new MessageRecallRequest { RequestId = "req-r1", MessageId = "msg-r1" };
        var backRequest = RoundTrip<MessageRecallRequest>(PacketCommand.MessageRecallRequest, request);
        Assert.Equal("req-r1", backRequest.RequestId);
        Assert.Equal("msg-r1", backRequest.MessageId);

        var ack = new MessageRecallAcknowledgement
        {
            RequestId = "req-r1",
            MessageId = "msg-r1",
            Succeeded = true,
            ConversationId = "conv-1",
            RecalledAtMs = 1725015296999
        };
        var backAck = RoundTrip<MessageRecallAcknowledgement>(PacketCommand.MessageRecallAck, ack);
        Assert.Equal("req-r1", backAck.RequestId);
        Assert.True(backAck.Succeeded);
        Assert.Equal("conv-1", backAck.ConversationId);
        Assert.Equal(1725015296999, backAck.RecalledAtMs);

        var update = new MessageRecalledUpdate
        {
            MessageId = "msg-r1",
            ConversationId = "conv-1",
            SenderUserId = 11,
            ReceiverUserId = 22,
            RecalledAtMs = 1725015296999
        };
        var backUpdate = RoundTrip<MessageRecalledUpdate>(PacketCommand.MessageRecalled, update);
        Assert.Equal("msg-r1", backUpdate.MessageId);
        Assert.Equal(11, backUpdate.SenderUserId);
        Assert.Equal(22, backUpdate.ReceiverUserId);
        Assert.Equal(1725015296999, backUpdate.RecalledAtMs);
    }

    // ──────────── 反应 ────────────

    [Fact]
    public void ReactionRequestsAndAcks_RoundTrip()
    {
        var add = new AddReactionRequest { RequestId = "req-a1", MessageId = "msg-a1", Emoji = "👍" };
        var backAdd = RoundTrip<AddReactionRequest>(PacketCommand.AddReactionRequest, add);
        Assert.Equal("req-a1", backAdd.RequestId);
        Assert.Equal("msg-a1", backAdd.MessageId);
        Assert.Equal("👍", backAdd.Emoji);

        var addAck = new AddReactionAcknowledgement
        {
            RequestId = "req-a1",
            MessageId = "msg-a1",
            Succeeded = true,
            ConversationId = "conv-1",
            Emoji = "👍",
            OccurredAtMs = 1725015296000,
            EmojiCount = 5
        };
        var backAddAck = RoundTrip<AddReactionAcknowledgement>(PacketCommand.AddReactionAck, addAck);
        Assert.Equal("conv-1", backAddAck.ConversationId);
        Assert.Equal("👍", backAddAck.Emoji);
        Assert.Equal(1725015296000, backAddAck.OccurredAtMs);
        Assert.Equal(5, backAddAck.EmojiCount);

        var remove = new RemoveReactionRequest { RequestId = "req-a2", MessageId = "msg-a1", Emoji = "👍" };
        var backRemove = RoundTrip<RemoveReactionRequest>(PacketCommand.RemoveReactionRequest, remove);
        Assert.Equal("req-a2", backRemove.RequestId);
        Assert.Equal("msg-a1", backRemove.MessageId);
        Assert.Equal("👍", backRemove.Emoji);

        var removeAck = new RemoveReactionAcknowledgement
        {
            RequestId = "req-a2",
            MessageId = "msg-a1",
            Succeeded = false,
            ErrorCode = "reaction_not_found",
            ErrorMessage = "反应不存在",
            Emoji = "👍"
        };
        var backRemoveAck = RoundTrip<RemoveReactionAcknowledgement>(
            PacketCommand.RemoveReactionAck, removeAck);
        Assert.False(backRemoveAck.Succeeded);
        Assert.Equal("reaction_not_found", backRemoveAck.ErrorCode);
        Assert.Equal("反应不存在", backRemoveAck.ErrorMessage);
    }

    [Fact]
    public void ReactionUpdates_RoundTrip()
    {
        var added = new ReactionAddedUpdate
        {
            MessageId = "msg-a1",
            ConversationId = "conv-1",
            ReactorUserId = 31,
            MessageSenderUserId = 32,
            MessageReceiverUserId = 33,
            Emoji = "🎉",
            EmojiCount = 7,
            OccurredAtMs = 1725015296456
        };
        var backAdded = RoundTrip<ReactionAddedUpdate>(PacketCommand.ReactionAdded, added);
        Assert.Equal("msg-a1", backAdded.MessageId);
        Assert.Equal("conv-1", backAdded.ConversationId);
        Assert.Equal(31, backAdded.ReactorUserId);
        Assert.Equal(32, backAdded.MessageSenderUserId);
        Assert.Equal(33, backAdded.MessageReceiverUserId);
        Assert.Equal("🎉", backAdded.Emoji);
        Assert.Equal(7, backAdded.EmojiCount);
        Assert.Equal(1725015296456, backAdded.OccurredAtMs);

        var removed = new ReactionRemovedUpdate
        {
            MessageId = "msg-a1",
            ConversationId = "conv-1",
            ReactorUserId = 31,
            MessageSenderUserId = 32,
            MessageReceiverUserId = 33,
            Emoji = "🎉",
            EmojiCount = 6,
            OccurredAtMs = 1725015296789
        };
        var backRemoved = RoundTrip<ReactionRemovedUpdate>(PacketCommand.ReactionRemoved, removed);
        Assert.Equal(6, backRemoved.EmojiCount);
        Assert.Equal(1725015296789, backRemoved.OccurredAtMs);
    }

    // ──────────── 会话列表 / 已读 / 偏好 ────────────

    [Fact]
    public void ConversationListRequestAndPage_RoundTrip()
    {
        var request = new ConversationListRequest
        {
            RequestId = "req-cl",
            BeforeIsPinned = true,
            BeforePinnedAtMs = 1725015296000,
            BeforeLastMessageAtMs = 1725015295000,
            BeforeConversationId = "conv-0",
            Limit = 25
        };
        var backRequest = RoundTrip<ConversationListRequest>(PacketCommand.ConversationListRequest, request);
        Assert.Equal("req-cl", backRequest.RequestId);
        Assert.True(backRequest.BeforeIsPinned);
        Assert.Equal(1725015296000, backRequest.BeforePinnedAtMs);
        Assert.Equal(1725015295000, backRequest.BeforeLastMessageAtMs);
        Assert.Equal("conv-0", backRequest.BeforeConversationId);
        Assert.Equal(25, backRequest.Limit);

        var response = new ConversationListResponse
        {
            RequestId = "req-cl",
            Succeeded = true,
            ErrorCode = null,
            ErrorMessage = null,
            Items =
            [
                new ConversationListItem
                {
                    ConversationId = "conv-1",
                    Type = ConversationType.Group,
                    PeerUserId = null,
                    Title = "群聊",
                    LastMessageId = "m1",
                    LastMessagePreview = "preview",
                    LastMessageAtMs = 1725015296000,
                    LastSenderUserId = 11,
                    UnreadCount = 3,
                    LastReadMessageId = "m0",
                    LastReadAtMs = 1725015290000,
                    IsPinned = true,
                    PinnedAtMs = 1725015280000,
                    IsMuted = false,
                    MutedUntilMs = null
                },
                new ConversationListItem
                {
                    ConversationId = "conv-2",
                    Type = ConversationType.Direct,
                    PeerUserId = 42,
                    UnreadCount = 0,
                    IsMuted = true,
                    MutedUntilMs = 1725016000000
                }
            ],
            NextCursor = new ConversationListCursor(true, 1725015280000, 1725015296000, "conv-1"),
            HasMore = true
        };
        var backResponse = RoundTrip<ConversationListResponse>(PacketCommand.ConversationListPage, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal(2, backResponse.Items.Count);
        var first = backResponse.Items[0];
        Assert.Equal("conv-1", first.ConversationId);
        Assert.Equal(ConversationType.Group, first.Type);
        Assert.Equal("群聊", first.Title);
        Assert.Equal("m1", first.LastMessageId);
        Assert.Equal("preview", first.LastMessagePreview);
        Assert.Equal(1725015296000, first.LastMessageAtMs);
        Assert.Equal(11, first.LastSenderUserId);
        Assert.Equal(3, first.UnreadCount);
        Assert.Equal("m0", first.LastReadMessageId);
        Assert.Equal(1725015290000, first.LastReadAtMs);
        Assert.True(first.IsPinned);
        Assert.Equal(1725015280000, first.PinnedAtMs);
        Assert.False(first.IsMuted);
        Assert.Null(first.MutedUntilMs);
        var second = backResponse.Items[1];
        Assert.Equal("conv-2", second.ConversationId);
        Assert.Equal(ConversationType.Direct, second.Type);
        Assert.Equal(42, second.PeerUserId);
        Assert.True(second.IsMuted);
        Assert.Equal(1725016000000, second.MutedUntilMs);
        Assert.NotNull(backResponse.NextCursor);
        Assert.True(backResponse.NextCursor!.IsPinned);
        Assert.Equal(1725015280000, backResponse.NextCursor.PinnedAtMs);
        Assert.Equal("conv-1", backResponse.NextCursor.ConversationId);
        Assert.True(backResponse.HasMore);
    }

    [Fact]
    public void ConversationMarkReadRequestAndResponse_RoundTrip()
    {
        var request = new ConversationMarkReadRequest
        {
            RequestId = "req-mr",
            ConversationId = "conv-1",
            ReadAtMs = 1725015296000,
            ReadMessageId = "m9"
        };
        var backRequest = RoundTrip<ConversationMarkReadRequest>(PacketCommand.ConversationMarkReadRequest, request);
        Assert.Equal("req-mr", backRequest.RequestId);
        Assert.Equal("conv-1", backRequest.ConversationId);
        Assert.Equal(1725015296000, backRequest.ReadAtMs);
        Assert.Equal("m9", backRequest.ReadMessageId);

        var response = new ConversationMarkReadResponse
        {
            RequestId = "req-mr",
            Succeeded = true,
            ConversationId = "conv-1",
            UnreadCount = 0,
            LastReadMessageId = "m9",
            LastReadAtMs = 1725015296000,
            Changed = true
        };
        var backResponse = RoundTrip<ConversationMarkReadResponse>(PacketCommand.ConversationMarkReadResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal("conv-1", backResponse.ConversationId);
        Assert.Equal(0, backResponse.UnreadCount);
        Assert.Equal("m9", backResponse.LastReadMessageId);
        Assert.Equal(1725015296000, backResponse.LastReadAtMs);
        Assert.True(backResponse.Changed);
    }

    [Fact]
    public void ConversationSetPrefsRequestAndResponse_RoundTrip()
    {
        var request = new ConversationSetPrefsRequest
        {
            RequestId = "req-sp",
            ConversationId = "conv-1",
            Pinned = true,
            Muted = true,
            MutedUntilMs = 1725016000000
        };
        var backRequest = RoundTrip<ConversationSetPrefsRequest>(PacketCommand.ConversationSetPrefsRequest, request);
        Assert.Equal("req-sp", backRequest.RequestId);
        Assert.Equal("conv-1", backRequest.ConversationId);
        Assert.True(backRequest.Pinned);
        Assert.True(backRequest.Muted);
        Assert.Equal(1725016000000, backRequest.MutedUntilMs);

        var response = new ConversationSetPrefsResponse
        {
            RequestId = "req-sp",
            Succeeded = true,
            ConversationId = "conv-1",
            IsPinned = true,
            IsMuted = true,
            MutedUntilMs = 1725016000000,
            Changed = true
        };
        var backResponse = RoundTrip<ConversationSetPrefsResponse>(PacketCommand.ConversationSetPrefsResponse, response);
        Assert.True(backResponse.IsPinned);
        Assert.True(backResponse.IsMuted);
        Assert.Equal(1725016000000, backResponse.MutedUntilMs);
        Assert.True(backResponse.Changed);
    }

    [Fact]
    public void ConversationEvents_RoundTrip()
    {
        var changed = new ConversationChanged
        {
            ConversationId = "conv-1",
            Type = ConversationType.Direct,
            PeerUserId = 42,
            Title = null,
            LastMessageId = "m2",
            LastMessagePreview = "hello",
            LastMessageAtMs = 1725015296000,
            LastSenderUserId = 42,
            IsPinned = false,
            IsMuted = true,
            MutedUntilMs = 1725016000000
        };
        var backChanged = RoundTrip<ConversationChanged>(PacketCommand.ConversationChanged, changed);
        Assert.Equal("conv-1", backChanged.ConversationId);
        Assert.Equal(ConversationType.Direct, backChanged.Type);
        Assert.Equal(42, backChanged.PeerUserId);
        Assert.Equal("m2", backChanged.LastMessageId);
        Assert.Equal("hello", backChanged.LastMessagePreview);
        Assert.Equal(1725015296000, backChanged.LastMessageAtMs);
        Assert.Equal(42, backChanged.LastSenderUserId);
        Assert.False(backChanged.IsPinned);
        Assert.True(backChanged.IsMuted);
        Assert.Equal(1725016000000, backChanged.MutedUntilMs);

        var unread = new UnreadCountChanged
        {
            ConversationId = "conv-1",
            UnreadCount = 12,
            LastReadMessageId = "m3",
            LastReadAtMs = 1725015296000
        };
        var backUnread = RoundTrip<UnreadCountChanged>(PacketCommand.UnreadCountChanged, unread);
        Assert.Equal("conv-1", backUnread.ConversationId);
        Assert.Equal(12, backUnread.UnreadCount);
        Assert.Equal("m3", backUnread.LastReadMessageId);
        Assert.Equal(1725015296000, backUnread.LastReadAtMs);

        var read = new ConversationReadUpdate
        {
            ConversationId = "conv-1",
            ReaderUserId = 42,
            LastReadMessageId = "m4",
            LastReadAtMs = 1725015296000
        };
        var backRead = RoundTrip<ConversationReadUpdate>(PacketCommand.ConversationRead, read);
        Assert.Equal("conv-1", backRead.ConversationId);
        Assert.Equal(42, backRead.ReaderUserId);
        Assert.Equal("m4", backRead.LastReadMessageId);
        Assert.Equal(1725015296000, backRead.LastReadAtMs);
    }

    [Fact]
    public void MessageReadReceiptQueryRequestAndResponse_RoundTrip()
    {
        var request = new MessageReadReceiptQueryRequest
        {
            RequestId = "req-rr",
            ConversationId = "conv-g",
            MessageId = "m5",
            Cursor = 99,
            PageSize = 50
        };
        var backRequest = RoundTrip<MessageReadReceiptQueryRequest>(
            PacketCommand.MessageReadReceiptQueryRequest, request);
        Assert.Equal("req-rr", backRequest.RequestId);
        Assert.Equal("conv-g", backRequest.ConversationId);
        Assert.Equal("m5", backRequest.MessageId);
        Assert.Equal(99, backRequest.Cursor);
        Assert.Equal(50, backRequest.PageSize);

        var response = new MessageReadReceiptQueryResponse
        {
            RequestId = "req-rr",
            Succeeded = true,
            ConversationId = "conv-g",
            ReadCount = 2,
            TotalMemberCount = 8,
            IsSmallGroup = true,
            Readers =
            [
                new MessageReadReceiptItem { UserId = 71, ReadAtMs = 1725015296000 },
                new MessageReadReceiptItem { UserId = 72, ReadAtMs = 1725015297000 }
            ],
            NextCursor = 72,
            HasMore = false
        };
        var backResponse = RoundTrip<MessageReadReceiptQueryResponse>(
            PacketCommand.MessageReadReceiptQueryResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal(2, backResponse.ReadCount);
        Assert.Equal(8, backResponse.TotalMemberCount);
        Assert.True(backResponse.IsSmallGroup);
        Assert.NotNull(backResponse.Readers);
        Assert.Equal(2, backResponse.Readers!.Count);
        Assert.Equal(71, backResponse.Readers[0].UserId);
        Assert.Equal(1725015296000, backResponse.Readers[0].ReadAtMs);
        Assert.Equal(72, backResponse.Readers[1].UserId);
        Assert.Equal(1725015297000, backResponse.Readers[1].ReadAtMs);
        Assert.Equal(72, backResponse.NextCursor);
        Assert.False(backResponse.HasMore);
    }

    // ──────────── 群组命令 ────────────

    [Fact]
    public void CreateGroupRequestAndResponse_RoundTrip()
    {
        var request = new CreateGroupRequest
        {
            RequestId = "req-cg",
            Title = "新群",
            MemberUserIds = [1, 2, 3]
        };
        var backRequest = RoundTrip<CreateGroupRequest>(PacketCommand.CreateGroupRequest, request);
        Assert.Equal("req-cg", backRequest.RequestId);
        Assert.Equal("新群", backRequest.Title);
        Assert.Equal([1L, 2, 3], backRequest.MemberUserIds);

        var response = new CreateGroupResponse
        {
            RequestId = "req-cg",
            Succeeded = true,
            ConversationId = "conv-g",
            Title = "新群",
            Members =
            [
                new ConversationMemberItem { UserId = 1, Role = ConversationMemberRole.Owner, JoinedAtMs = 100 },
                new ConversationMemberItem { UserId = 2, Role = ConversationMemberRole.Member, JoinedAtMs = 200 }
            ]
        };
        var backResponse = RoundTrip<CreateGroupResponse>(PacketCommand.CreateGroupResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal("conv-g", backResponse.ConversationId);
        Assert.Equal("新群", backResponse.Title);
        Assert.NotNull(backResponse.Members);
        Assert.Equal(2, backResponse.Members!.Count);
        Assert.Equal(1, backResponse.Members[0].UserId);
        Assert.Equal(ConversationMemberRole.Owner, backResponse.Members[0].Role);
        Assert.Equal(100, backResponse.Members[0].JoinedAtMs);
        Assert.Equal(ConversationMemberRole.Member, backResponse.Members[1].Role);
    }

    [Fact]
    public void GroupMembershipRequestsAndResponses_RoundTrip()
    {
        var addRequest = new AddGroupMembersRequest
        {
            RequestId = "req-ag",
            ConversationId = "conv-g",
            MemberUserIds = [4, 5]
        };
        var backAddRequest = RoundTrip<AddGroupMembersRequest>(PacketCommand.AddGroupMembersRequest, addRequest);
        Assert.Equal("req-ag", backAddRequest.RequestId);
        Assert.Equal("conv-g", backAddRequest.ConversationId);
        Assert.Equal([4L, 5], backAddRequest.MemberUserIds);

        var addResponse = new AddGroupMembersResponse
        {
            RequestId = "req-ag",
            Succeeded = true,
            ConversationId = "conv-g",
            Members = [new ConversationMemberItem { UserId = 4, Role = ConversationMemberRole.Admin, JoinedAtMs = 300 }]
        };
        var backAddResponse = RoundTrip<AddGroupMembersResponse>(PacketCommand.AddGroupMembersResponse, addResponse);
        Assert.True(backAddResponse.Succeeded);
        Assert.Equal(ConversationMemberRole.Admin, backAddResponse.Members![0].Role);

        var removeRequest = new RemoveGroupMemberRequest
        {
            RequestId = "req-rg",
            ConversationId = "conv-g",
            TargetUserId = 5
        };
        var backRemoveRequest = RoundTrip<RemoveGroupMemberRequest>(
            PacketCommand.RemoveGroupMemberRequest, removeRequest);
        Assert.Equal("req-rg", backRemoveRequest.RequestId);
        Assert.Equal(5, backRemoveRequest.TargetUserId);

        var removeResponse = new RemoveGroupMemberResponse
        {
            RequestId = "req-rg",
            Succeeded = true,
            ConversationId = "conv-g"
        };
        var backRemoveResponse = RoundTrip<RemoveGroupMemberResponse>(
            PacketCommand.RemoveGroupMemberResponse, removeResponse);
        Assert.True(backRemoveResponse.Succeeded);
        Assert.Equal("conv-g", backRemoveResponse.ConversationId);
    }

    [Fact]
    public void LeaveDissolveAndRoleRequests_RoundTrip()
    {
        var leave = new LeaveGroupRequest { RequestId = "req-lg", ConversationId = "conv-g" };
        var backLeave = RoundTrip<LeaveGroupRequest>(PacketCommand.LeaveGroupRequest, leave);
        Assert.Equal("req-lg", backLeave.RequestId);
        Assert.Equal("conv-g", backLeave.ConversationId);

        var leaveResponse = new LeaveGroupResponse
        {
            RequestId = "req-lg",
            Succeeded = false,
            ErrorCode = "group_unavailable",
            ErrorMessage = "群服务暂时不可用。",
            ConversationId = "conv-g"
        };
        var backLeaveResponse = RoundTrip<LeaveGroupResponse>(PacketCommand.LeaveGroupResponse, leaveResponse);
        Assert.False(backLeaveResponse.Succeeded);
        Assert.Equal("group_unavailable", backLeaveResponse.ErrorCode);
        Assert.Equal("群服务暂时不可用。", backLeaveResponse.ErrorMessage);

        var dissolve = new DissolveGroupRequest { RequestId = "req-dg", ConversationId = "conv-g" };
        var backDissolve = RoundTrip<DissolveGroupRequest>(PacketCommand.DissolveGroupRequest, dissolve);
        Assert.Equal("req-dg", backDissolve.RequestId);
        Assert.Equal("conv-g", backDissolve.ConversationId);

        var dissolveResponse = new DissolveGroupResponse
        {
            RequestId = "req-dg",
            Succeeded = true,
            ConversationId = "conv-g"
        };
        var backDissolveResponse = RoundTrip<DissolveGroupResponse>(PacketCommand.DissolveGroupResponse, dissolveResponse);
        Assert.True(backDissolveResponse.Succeeded);

        var role = new ChangeMemberRoleRequest
        {
            RequestId = "req-cr",
            ConversationId = "conv-g",
            TargetUserId = 6,
            NewRole = ConversationMemberRole.Admin
        };
        var backRole = RoundTrip<ChangeMemberRoleRequest>(PacketCommand.ChangeMemberRoleRequest, role);
        Assert.Equal("req-cr", backRole.RequestId);
        Assert.Equal(6, backRole.TargetUserId);
        Assert.Equal(ConversationMemberRole.Admin, backRole.NewRole);

        var roleResponse = new ChangeMemberRoleResponse
        {
            RequestId = "req-cr",
            Succeeded = true,
            ConversationId = "conv-g"
        };
        var backRoleResponse = RoundTrip<ChangeMemberRoleResponse>(PacketCommand.ChangeMemberRoleResponse, roleResponse);
        Assert.True(backRoleResponse.Succeeded);
    }

    [Fact]
    public void ListGroupMembersRequestAndResponse_RoundTrip()
    {
        var request = new ListGroupMembersRequest
        {
            RequestId = "req-lm",
            ConversationId = "conv-g",
            PageSize = 100,
            Cursor = "cursor-1"
        };
        var backRequest = RoundTrip<ListGroupMembersRequest>(PacketCommand.ListGroupMembersRequest, request);
        Assert.Equal("req-lm", backRequest.RequestId);
        Assert.Equal("conv-g", backRequest.ConversationId);
        Assert.Equal(100, backRequest.PageSize);
        Assert.Equal("cursor-1", backRequest.Cursor);

        var response = new ListGroupMembersResponse
        {
            RequestId = "req-lm",
            Succeeded = true,
            ConversationId = "conv-g",
            Members = [new ConversationMemberItem { UserId = 9, Role = ConversationMemberRole.Member, JoinedAtMs = 900 }],
            NextCursor = "cursor-2",
            HasMore = true
        };
        var backResponse = RoundTrip<ListGroupMembersResponse>(PacketCommand.ListGroupMembersResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal(9, backResponse.Members![0].UserId);
        Assert.Equal("cursor-2", backResponse.NextCursor);
        Assert.True(backResponse.HasMore);
    }

    [Fact]
    public void GroupMemberUpdates_RoundTrip()
    {
        var joined = new MemberJoinedUpdate
        {
            ConversationId = "conv-g",
            UserId = 7,
            Role = ConversationMemberRole.Member,
            ActorUserId = 1,
            Title = "群聊",
            OccurredAtMs = 1725015296000
        };
        var backJoined = RoundTrip<MemberJoinedUpdate>(PacketCommand.MemberJoined, joined);
        Assert.Equal("conv-g", backJoined.ConversationId);
        Assert.Equal(7, backJoined.UserId);
        Assert.Equal(ConversationMemberRole.Member, backJoined.Role);
        Assert.Equal(1, backJoined.ActorUserId);
        Assert.Equal("群聊", backJoined.Title);
        Assert.Equal(1725015296000, backJoined.OccurredAtMs);

        var left = new MemberLeftUpdate { ConversationId = "conv-g", UserId = 7, OccurredAtMs = 1725015296001 };
        var backLeft = RoundTrip<MemberLeftUpdate>(PacketCommand.MemberLeft, left);
        Assert.Equal(7, backLeft.UserId);
        Assert.Equal(1725015296001, backLeft.OccurredAtMs);

        var removed = new MemberRemovedUpdate
        {
            ConversationId = "conv-g",
            UserId = 7,
            ActorUserId = 1,
            OccurredAtMs = 1725015296002
        };
        var backRemoved = RoundTrip<MemberRemovedUpdate>(PacketCommand.MemberRemoved, removed);
        Assert.Equal(7, backRemoved.UserId);
        Assert.Equal(1, backRemoved.ActorUserId);
        Assert.Equal(1725015296002, backRemoved.OccurredAtMs);

        var roleChanged = new RoleChangedUpdate
        {
            ConversationId = "conv-g",
            UserId = 7,
            NewRole = ConversationMemberRole.Admin,
            PreviousRole = ConversationMemberRole.Member,
            ActorUserId = 1,
            OccurredAtMs = 1725015296003
        };
        var backRoleChanged = RoundTrip<RoleChangedUpdate>(PacketCommand.RoleChanged, roleChanged);
        Assert.Equal(7, backRoleChanged.UserId);
        Assert.Equal(ConversationMemberRole.Admin, backRoleChanged.NewRole);
        Assert.Equal(ConversationMemberRole.Member, backRoleChanged.PreviousRole);
        Assert.Equal(1, backRoleChanged.ActorUserId);
        Assert.Equal(1725015296003, backRoleChanged.OccurredAtMs);

        var membersAdded = new MembersAddedUpdate
        {
            ConversationId = "conv-g",
            AddedUserIds = [8, 9, 10],
            ActorUserId = 1,
            Title = "群聊",
            OccurredAtMs = 1725015296004
        };
        var backMembersAdded = RoundTrip<MembersAddedUpdate>(PacketCommand.MembersAddedUpdate, membersAdded);
        Assert.Equal([8L, 9, 10], backMembersAdded.AddedUserIds);
        Assert.Equal(1, backMembersAdded.ActorUserId);
        Assert.Equal(1725015296004, backMembersAdded.OccurredAtMs);

        var dissolved = new ConversationDissolvedUpdate
        {
            ConversationId = "conv-g",
            ActorUserId = 1,
            OccurredAtMs = 1725015296005
        };
        var backDissolved = RoundTrip<ConversationDissolvedUpdate>(
            PacketCommand.ConversationDissolvedUpdate, dissolved);
        Assert.Equal("conv-g", backDissolved.ConversationId);
        Assert.Equal(1, backDissolved.ActorUserId);
        Assert.Equal(1725015296005, backDissolved.OccurredAtMs);
    }

    // ──────────── 关系 ────────────

    [Fact]
    public void RelationshipCommandRequestAndResponse_RoundTrip()
    {
        var request = new RelationshipCommandRequest
        {
            RequestId = "req-rel",
            Operation = RelationshipOperation.SendFriendRequest,
            TargetUserId = 55,
            Message = "交个朋友",
            RequestIdToRespond = null
        };
        var backRequest = RoundTrip<RelationshipCommandRequest>(PacketCommand.RelationshipCommandRequest, request);
        Assert.Equal("req-rel", backRequest.RequestId);
        Assert.Equal(RelationshipOperation.SendFriendRequest, backRequest.Operation);
        Assert.Equal(55, backRequest.TargetUserId);
        Assert.Equal("交个朋友", backRequest.Message);
        Assert.Null(backRequest.RequestIdToRespond);

        var response = new RelationshipCommandResponse
        {
            RequestId = "req-rel",
            Succeeded = true,
            ErrorCode = null,
            ErrorMessage = null,
            Operation = RelationshipOperation.AcceptFriendRequest,
            TargetUserId = 55,
            ResourceId = "friendship-1"
        };
        var backResponse = RoundTrip<RelationshipCommandResponse>(PacketCommand.RelationshipCommandResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal(RelationshipOperation.AcceptFriendRequest, backResponse.Operation);
        Assert.Equal(55, backResponse.TargetUserId);
        Assert.Equal("friendship-1", backResponse.ResourceId);
    }

    [Fact]
    public void RelationshipListChangedUpdate_RoundTrips()
    {
        var local = new RelationshipListChangedUpdate
        {
            Resource = "friend-request",
            Action = "Pending",
            ResourceId = "request-1",
            ActorUserId = 66,
            Message = "加个好友",
            OccurredAtMs = 1725015296000
        };

        var back = RoundTrip<RelationshipListChangedUpdate>(PacketCommand.RelationshipListChanged, local);

        Assert.Equal("friend-request", back.Resource);
        Assert.Equal("Pending", back.Action);
        Assert.Equal("request-1", back.ResourceId);
        Assert.Equal(66, back.ActorUserId);
        Assert.Equal("加个好友", back.Message);
        Assert.Equal(1725015296000, back.OccurredAtMs);
    }

    // ──────────── 在线 / 输入 / 推送 ────────────

    [Fact]
    public void TypingNotify_RoundTripsDroppingTargetUserId()
    {
        // 共享 schema 要求 TargetUserId，但网关以 ConversationId 为权威源推导目标，字段被丢弃。
        var local = new TypingNotify
        {
            ConversationId = "dm:1:2",
            IsTyping = true
        };

        var back = RoundTrip<TypingNotify>(PacketCommand.TypingNotify, local);

        Assert.Equal("dm:1:2", back.ConversationId);
        Assert.True(back.IsTyping);
    }

    [Fact]
    public void TypingUpdate_RoundTrips()
    {
        var local = new TypingUpdate
        {
            SenderUserId = 1,
            ConversationId = "dm:1:2",
            IsTyping = false
        };

        var back = RoundTrip<TypingUpdate>(PacketCommand.TypingUpdate, local);

        Assert.Equal(1, back.SenderUserId);
        Assert.Equal("dm:1:2", back.ConversationId);
        Assert.False(back.IsTyping);
    }

    [Fact]
    public void PresenceQueryAndUnwatch_RoundTrip()
    {
        var query = new PresenceQueryRequest
        {
            RequestId = "req-pq",
            UserIds = [101, 102]
        };
        var backQuery = RoundTrip<PresenceQueryRequest>(PacketCommand.PresenceQuery, query);
        Assert.Equal("req-pq", backQuery.RequestId);
        Assert.Equal([101L, 102], backQuery.UserIds);

        var unwatch = new PresenceUnwatchRequest { UserIds = [101] };
        var backUnwatch = RoundTrip<PresenceUnwatchRequest>(PacketCommand.PresenceUnwatch, unwatch);
        Assert.Equal([101L], backUnwatch.UserIds);
    }

    [Fact]
    public void PresenceSnapshot_RoundTripsItems()
    {
        var local = new PresenceSnapshotResponse
        {
            RequestId = "req-ps",
            Items =
            [
                new PresenceSnapshotItem { UserId = 101, IsOnline = true },
                new PresenceSnapshotItem { UserId = 102, IsOnline = false }
            ]
        };

        var back = RoundTrip<PresenceSnapshotResponse>(PacketCommand.PresenceSnapshot, local);

        Assert.Equal("req-ps", back.RequestId);
        Assert.Equal(2, back.Items.Count);
        Assert.Equal(101, back.Items[0].UserId);
        Assert.True(back.Items[0].IsOnline);
        Assert.Equal(102, back.Items[1].UserId);
        Assert.False(back.Items[1].IsOnline);
    }

    [Fact]
    public void PresenceChanged_RoundTrips()
    {
        var local = new PresenceChanged { UserId = 103, IsOnline = true };

        var back = RoundTrip<PresenceChanged>(PacketCommand.PresenceChanged, local);

        Assert.Equal(103, back.UserId);
        Assert.True(back.IsOnline);
    }

    [Fact]
    public void PushTokenRequestsAndResponses_RoundTrip()
    {
        var register = new RegisterPushTokenRequest
        {
            RequestId = "req-pt",
            Platform = PushPlatform.Apns,
            Token = "apns-token",
            AppDeviceLabel = "ios-main"
        };
        var backRegister = RoundTrip<RegisterPushTokenRequest>(PacketCommand.RegisterPushTokenRequest, register);
        Assert.Equal("req-pt", backRegister.RequestId);
        Assert.Equal(PushPlatform.Apns, backRegister.Platform);
        Assert.Equal("apns-token", backRegister.Token);
        Assert.Equal("ios-main", backRegister.AppDeviceLabel);

        var registerResponse = new RegisterPushTokenResponse
        {
            RequestId = "req-pt",
            Succeeded = true,
            ActiveTokenCount = 3
        };
        var backRegisterResponse = RoundTrip<RegisterPushTokenResponse>(
            PacketCommand.RegisterPushTokenResponse, registerResponse);
        Assert.True(backRegisterResponse.Succeeded);
        Assert.Equal(3, backRegisterResponse.ActiveTokenCount);

        var unregister = new UnregisterPushTokenRequest { RequestId = "req-ut", Token = "apns-token" };
        var backUnregister = RoundTrip<UnregisterPushTokenRequest>(PacketCommand.UnregisterPushTokenRequest, unregister);
        Assert.Equal("req-ut", backUnregister.RequestId);
        Assert.Equal("apns-token", backUnregister.Token);

        var unregisterResponse = new UnregisterPushTokenResponse
        {
            RequestId = "req-ut",
            Succeeded = true,
            ActiveTokenCount = 2
        };
        var backUnregisterResponse = RoundTrip<UnregisterPushTokenResponse>(
            PacketCommand.UnregisterPushTokenResponse, unregisterResponse);
        Assert.True(backUnregisterResponse.Succeeded);
        Assert.Equal(2, backUnregisterResponse.ActiveTokenCount);
    }

    // ──────────── 附件 ────────────

    [Fact]
    public void AttachmentLifecycleUpdate_RoundTrips()
    {
        var local = new AttachmentLifecycleUpdate
        {
            AttachmentId = "att-9",
            Status = 3,
            OccurredAtMs = 1725015296000,
            RejectReason = "virus",
            ThumbnailApiHint = "thumb-9",
            DownloadToken = "tok-9"
        };

        var back = RoundTrip<AttachmentLifecycleUpdate>(PacketCommand.AttachmentLifecycleChanged, local);

        Assert.Equal("att-9", back.AttachmentId);
        Assert.Equal(3, back.Status);
        Assert.Equal(1725015296000, back.OccurredAtMs);
        Assert.Equal("virus", back.RejectReason);
        Assert.Equal("thumb-9", back.ThumbnailApiHint);
        Assert.Equal("tok-9", back.DownloadToken);
    }

    [Fact]
    public void AttachmentFinalizeRequestAndResponse_RoundTrip()
    {
        var request = new AttachmentFinalizeRequest
        {
            RequestId = "req-af",
            AttachmentId = "att-1",
            SizeBytes = 987654,
            ContentHash = "aabbcc"
        };
        var backRequest = RoundTrip<AttachmentFinalizeRequest>(PacketCommand.AttachmentFinalizeRequest, request);
        Assert.Equal("req-af", backRequest.RequestId);
        Assert.Equal("att-1", backRequest.AttachmentId);
        Assert.Equal(987654, backRequest.SizeBytes);
        Assert.Equal("aabbcc", backRequest.ContentHash);

        var response = new AttachmentFinalizeResponse
        {
            RequestId = "req-af",
            Succeeded = true,
            AttachmentId = "att-1",
            Status = 2
        };
        var backResponse = RoundTrip<AttachmentFinalizeResponse>(PacketCommand.AttachmentFinalizeResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal("att-1", backResponse.AttachmentId);
        Assert.Equal((short)2, backResponse.Status);
    }

    [Fact]
    public void AttachmentDownloadAuthorizeRequestAndResponse_RoundTrip()
    {
        var request = new AttachmentDownloadAuthorizeRequest
        {
            RequestId = "req-ad",
            AttachmentId = "att-1",
            ConversationId = "conv-1"
        };
        var backRequest = RoundTrip<AttachmentDownloadAuthorizeRequest>(
            PacketCommand.AttachmentDownloadAuthorizeRequest, request);
        Assert.Equal("req-ad", backRequest.RequestId);
        Assert.Equal("att-1", backRequest.AttachmentId);
        Assert.Equal("conv-1", backRequest.ConversationId);

        var response = new AttachmentDownloadAuthorizeResponse
        {
            RequestId = "req-ad",
            Succeeded = true,
            AttachmentId = "att-1",
            DownloadUrl = "https://api/att-1/download",
            DownloadToken = "tok-dl",
            ExpiresAtMs = 1725016000000
        };
        var backResponse = RoundTrip<AttachmentDownloadAuthorizeResponse>(
            PacketCommand.AttachmentDownloadAuthorizeResponse, response);
        Assert.True(backResponse.Succeeded);
        Assert.Equal("att-1", backResponse.AttachmentId);
        Assert.Equal("https://api/att-1/download", backResponse.DownloadUrl);
        Assert.Equal("tok-dl", backResponse.DownloadToken);
        Assert.Equal(1725016000000, backResponse.ExpiresAtMs);
    }

    // ──────────── 共享类型恒等（wire 类型即共享类型的命令） ────────────

    [Fact]
    public void SharedTypeCommands_PassThroughToSharedAndToLocal()
    {
        var historyResponse = new MessageHistoryResponse
        {
            RequestId = "req-h",
            Succeeded = true,
            Items = [],
            HasMore = false
        };
        Assert.True(ReferenceEquals(
            historyResponse,
            BinaryPayloadMapper.ToShared(PacketCommand.MessageHistoryPage, historyResponse)));
        Assert.True(ReferenceEquals(
            historyResponse,
            BinaryPayloadMapper.ToLocal<MessageHistoryResponse>(PacketCommand.MessageHistoryPage, historyResponse)));

        var historyRequest = new MessageHistoryRequest { RequestId = "req-h", Limit = 10 };
        Assert.True(ReferenceEquals(
            historyRequest,
            BinaryPayloadMapper.ToShared(PacketCommand.MessageHistoryRequest, historyRequest)));
        Assert.True(ReferenceEquals(
            historyRequest,
            BinaryPayloadMapper.ToLocal<MessageHistoryRequest>(PacketCommand.MessageHistoryRequest, historyRequest)));

        var syncResponse = new SyncBootstrapResponse { RequestId = "req-s", Succeeded = true };
        Assert.True(ReferenceEquals(
            syncResponse,
            BinaryPayloadMapper.ToShared(PacketCommand.SyncBootstrapResponse, syncResponse)));
        Assert.True(ReferenceEquals(
            syncResponse,
            BinaryPayloadMapper.ToLocal<SyncBootstrapResponse>(PacketCommand.SyncBootstrapResponse, syncResponse)));

        var syncRequest = new SyncBootstrapRequest { RequestId = "req-s" };
        Assert.True(ReferenceEquals(
            syncRequest,
            BinaryPayloadMapper.ToShared(PacketCommand.SyncBootstrapRequest, syncRequest)));

        var callResponse = new TcpCallCommandResponse { RequestId = "req-c", CallId = "call-1" };
        Assert.True(ReferenceEquals(
            callResponse,
            BinaryPayloadMapper.ToShared(PacketCommand.CallCommandResponse, callResponse)));
        Assert.True(ReferenceEquals(
            callResponse,
            BinaryPayloadMapper.ToLocal<TcpCallCommandResponse>(PacketCommand.CallCommandResponse, callResponse)));

        var callRequest = new TcpCallCommandRequest { RequestId = "req-c", CallId = "call-1" };
        Assert.True(ReferenceEquals(
            callRequest,
            BinaryPayloadMapper.ToShared(PacketCommand.CallCommandRequest, callRequest)));

        var signal = new TcpCallSignal { SignalId = "sig-1" };
        Assert.True(ReferenceEquals(signal, BinaryPayloadMapper.ToShared(PacketCommand.CallSignal, signal)));
        Assert.True(ReferenceEquals(signal, BinaryPayloadMapper.ToLocal<TcpCallSignal>(PacketCommand.CallSignal, signal)));

        var listRequest = new TcpRelationshipListRequest { RequestId = "req-rl", PageSize = 50 };
        Assert.True(ReferenceEquals(
            listRequest,
            BinaryPayloadMapper.ToShared(PacketCommand.RelationshipListRequest, listRequest)));

        var listResponse = new TcpRelationshipListResponse { RequestId = "req-rl", Succeeded = true };
        Assert.True(ReferenceEquals(
            listResponse,
            BinaryPayloadMapper.ToShared(PacketCommand.RelationshipListResponse, listResponse)));
        Assert.True(ReferenceEquals(
            listResponse,
            BinaryPayloadMapper.ToLocal<TcpRelationshipListResponse>(PacketCommand.RelationshipListResponse, listResponse)));

        var errorFrame = new ProtocolErrorFrame { Code = ProtocolErrorCode.InvalidPayload, Message = "bad" };
        Assert.True(ReferenceEquals(
            errorFrame,
            BinaryPayloadMapper.ToShared(PacketCommand.Error, errorFrame)));
        Assert.True(ReferenceEquals(
            errorFrame,
            BinaryPayloadMapper.ToLocal<ProtocolErrorFrame>(PacketCommand.Error, errorFrame)));

        var goAway = new GoAway { Reason = "shutdown", RetryAfterMs = 1000 };
        Assert.True(ReferenceEquals(goAway, BinaryPayloadMapper.ToShared(PacketCommand.GoAway, goAway)));
        Assert.True(ReferenceEquals(goAway, BinaryPayloadMapper.ToLocal<GoAway>(PacketCommand.GoAway, goAway)));
    }

    [Fact]
    public void MessageHistoryResponseAndSyncBootstrapResponse_BinaryRoundTrip()
    {
        // 任务要求的恒等共享类型同样必须经真实二进制编码/解码保持内容一致。
        var historyResponse = new MessageHistoryResponse
        {
            RequestId = "req-h",
            ConversationId = "conv-1",
            Succeeded = true,
            ErrorCode = null,
            ErrorMessage = null,
            HasMore = false
        };

        var historyPayload = EncodeShared(historyResponse);
        var decodedHistory = (MessageHistoryResponse)DecodeShared(PacketCommand.MessageHistoryPage, historyPayload);
        Assert.Equal("req-h", decodedHistory.RequestId);
        Assert.Equal("conv-1", decodedHistory.ConversationId);
        Assert.True(decodedHistory.Succeeded);
        Assert.Same(
            decodedHistory,
            BinaryPayloadMapper.ToLocal<MessageHistoryResponse>(PacketCommand.MessageHistoryPage, decodedHistory));

        var syncResponse = new SyncBootstrapResponse
        {
            RequestId = "req-s",
            Succeeded = true,
            ServerTimeMs = 1725015296000,
            Conversations =
            [
                new TcpConversationListItem
                {
                    ConversationId = "conv-1",
                    UnreadCount = 1,
                    IsPinned = true
                }
            ],
            ConversationsNextCursor = new TcpConversationListCursor
            {
                IsPinned = true,
                ConversationId = "conv-1"
            },
            ConversationsHasMore = true
        };

        var syncPayload = EncodeShared(syncResponse);
        var decodedSync = (SyncBootstrapResponse)DecodeShared(PacketCommand.SyncBootstrapResponse, syncPayload);
        Assert.Equal("req-s", decodedSync.RequestId);
        Assert.True(decodedSync.Succeeded);
        Assert.Equal(1725015296000, decodedSync.ServerTimeMs);
        Assert.Equal("conv-1", decodedSync.Conversations[0].ConversationId);
        Assert.Equal(1, decodedSync.Conversations[0].UnreadCount);
        Assert.True(decodedSync.ConversationsHasMore);
        Assert.Same(
            decodedSync,
            BinaryPayloadMapper.ToLocal<SyncBootstrapResponse>(PacketCommand.SyncBootstrapResponse, decodedSync));

        var callResponse = new TcpCallCommandResponse
        {
            RequestId = "req-c",
            CallId = "call-1",
            Succeeded = true,
            State = TcpCallState.Active,
            Revision = 5,
            Replayed = false
        };

        var callPayload = EncodeShared(callResponse);
        var decodedCall = (TcpCallCommandResponse)DecodeShared(PacketCommand.CallCommandResponse, callPayload);
        Assert.Equal("req-c", decodedCall.RequestId);
        Assert.Equal("call-1", decodedCall.CallId);
        Assert.True(decodedCall.Succeeded);
        Assert.Equal(TcpCallState.Active, decodedCall.State);
        Assert.Equal(5, decodedCall.Revision);
        Assert.Same(
            decodedCall,
            BinaryPayloadMapper.ToLocal<TcpCallCommandResponse>(PacketCommand.CallCommandResponse, decodedCall));
    }

    // ──────────── fail-closed 负例 ────────────

    [Fact]
    public void ToShared_UnmappedCommand_Throws()
    {
        // 握手段与心跳不经映射层（始终 JSON / 空载荷），映射器必须拒绝而非猜测。
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.Heartbeat, new object()));
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.ClientHello, new object()));
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.ServerHello, new object()));
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.ResumeResponse, new object()));
    }

    [Fact]
    public void ToShared_WrongPayloadTypeForMappedCommand_Throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.ChatMessage, new PresenceChanged()));
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.PresenceChanged, new ChatMessage()));
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToShared(PacketCommand.MessageHistoryPage, new ChatMessage()));
    }

    [Fact]
    public void ToLocal_UnmappedCommandOrWrongType_Throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToLocal<ChatMessage>(PacketCommand.Heartbeat, new object()));
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToLocal<ChatMessage>(PacketCommand.ClientHello, new object()));
        // 命令已映射但共享载荷类型与命令不符：寄存器每命令只产出唯一 DTO，
        // 该状态只可能来自编程错误，Require fail-closed。
        Assert.Throws<InvalidOperationException>(
            () => BinaryPayloadMapper.ToLocal<ChatMessage>(
                PacketCommand.PresenceChanged,
                new TcpCallCommandRequest { CallId = "call-1" }));
        // 共享载荷类型与命令匹配、但与请求的本地 DTO 不符：映射成功后类型转换 fail-closed。
        var tcpPresence = new TcpPresenceChanged { UserId = 1, IsOnline = true };
        Assert.Throws<InvalidCastException>(
            () => BinaryPayloadMapper.ToLocal<ChatMessage>(PacketCommand.PresenceChanged, tcpPresence));
    }

    [Fact]
    public void ToLocal_NullValue_ReturnsNull()
    {
        Assert.Null(BinaryPayloadMapper.ToLocal<ChatMessage>(PacketCommand.ChatMessage, null));
        Assert.Null(BinaryPayloadMapper.ToLocal<MessageHistoryResponse>(PacketCommand.MessageHistoryPage, null));
    }
}
