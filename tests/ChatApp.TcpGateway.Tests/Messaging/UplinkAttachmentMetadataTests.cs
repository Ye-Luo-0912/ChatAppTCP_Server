using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// VOICE-MSG-2：上行 ChatMessage.Attachments → IncomingMessageCommand.Attachments
/// 元数据快照提取（MapUplinkAttachmentMetadata）行为。
/// </summary>
public sealed class UplinkAttachmentMetadataTests
{
    [Fact]
    public void Metadata_Null_When_UplinkHasNoAttachments()
    {
        var result = MessagingCommandHandler.MapUplinkAttachmentMetadata(
            ["att-1"],
            uplinkAttachments: null);

        Assert.Null(result);
    }

    [Fact]
    public void Metadata_Null_When_MessageHasNoAttachmentIds()
    {
        var result = MessagingCommandHandler.MapUplinkAttachmentMetadata(
            attachmentIds: null,
            [Ref("att-1")]);

        Assert.Null(result);
    }

    [Fact]
    public void Metadata_OnlyKeepsReferencesMatchingAttachmentIds()
    {
        var result = MessagingCommandHandler.MapUplinkAttachmentMetadata(
            ["att-1"],
            [Ref("att-1"), Ref("att-2"), Ref("att-3")]);

        Assert.NotNull(result);
        var attachment = Assert.Single(result);
        Assert.Equal("att-1", attachment.AttachmentId);
    }

    [Fact]
    public void Metadata_PreservesVoiceFieldsOfMatchedReference()
    {
        var voice = new AttachmentRef
        {
            AttachmentId = "att-voice",
            ContentType = "audio/wav",
            IsVoice = true,
            VoiceCodec = "pcm",
            VoiceContainer = "wav",
            VoiceDurationMs = 3_500,
            VoiceSampleRateHz = 16_000,
            VoiceChannels = 1,
            VoiceWaveformPeaks = [3, 77, 200, 41]
        };
        var result = MessagingCommandHandler.MapUplinkAttachmentMetadata(
            ["att-voice"],
            [voice]);

        Assert.NotNull(result);
        var attachment = Assert.Single(result);
        Assert.True(attachment.IsVoice);
        Assert.Equal("pcm", attachment.VoiceCodec);
        Assert.Equal("wav", attachment.VoiceContainer);
        Assert.Equal(3_500L, attachment.VoiceDurationMs);
        Assert.Equal(16_000, attachment.VoiceSampleRateHz);
        Assert.Equal((short)1, attachment.VoiceChannels);
        // VOICE-MSG-2 waveform：匹配引用的波形峰值随快照透传（进入 IncomingMessageCommand.Attachments）。
        Assert.Equal(new byte[] { 3, 77, 200, 41 }, attachment.VoiceWaveformPeaks);
    }

    [Fact]
    public void Metadata_Null_When_NoReferenceMatchesIds()
    {
        var result = MessagingCommandHandler.MapUplinkAttachmentMetadata(
            ["att-x"],
            [Ref("att-1"), Ref("att-2")]);

        Assert.Null(result);
    }

    [Fact]
    public void Metadata_SkipsReferencesWithEmptyAttachmentId()
    {
        var result = MessagingCommandHandler.MapUplinkAttachmentMetadata(
            ["att-1"],
            [new AttachmentRef { AttachmentId = string.Empty, ContentType = "a/b" }, Ref("att-1")]);

        Assert.NotNull(result);
        var attachment = Assert.Single(result);
        Assert.Equal("att-1", attachment.AttachmentId);
    }

    private static AttachmentRef Ref(string attachmentId) => new()
    {
        AttachmentId = attachmentId,
        ContentType = "application/octet-stream"
    };
}
