using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.TcpGateway.Gateway.Messaging;
using Xunit;
using SharedAttachmentRef = ChatApp.Shared.Protocol.Tcp.TcpAttachmentRef;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// HistoryWireMapper.MapAttachments 语音元数据映射验证（VOICE-MSG-2）：
/// Realtime 语音附件的 6 个字段应完整映射到 Shared TCP 线协议，非语音附件保持默认值。
/// </summary>
public sealed class HistoryWireMapperAttachmentTests
{
    [Fact]
    public void MapAttachments_MapsVoiceMetadata()
    {
        var source = new[]
        {
            new AttachmentRef
            {
                AttachmentId = "voice-01",
                FileName = "voice.opus",
                ContentType = "audio/opus",
                SizeBytes = 1234,
                Status = AttachmentWireStatus.Available,
                IsVoice = true,
                VoiceCodec = "opus",
                VoiceContainer = "ogg",
                VoiceDurationMs = 4_500,
                VoiceSampleRateHz = 48_000,
                VoiceChannels = 1
            }
        };

        var mapped = HistoryWireMapper.MapAttachments(source);

        var item = Assert.Single(mapped!);
        Assert.Equal("voice-01", item.AttachmentId);
        Assert.True(item.IsVoice);
        Assert.Equal("opus", item.VoiceCodec);
        Assert.Equal("ogg", item.VoiceContainer);
        Assert.Equal(4_500, item.VoiceDurationMs);
        Assert.Equal(48_000, item.VoiceSampleRateHz);
        Assert.Equal((short)1, item.VoiceChannels);
    }

    [Fact]
    public void MapAttachments_NonVoiceAttachmentKeepsVoiceFieldsDefault()
    {
        var source = new[]
        {
            new AttachmentRef
            {
                AttachmentId = "plain-01",
                ContentType = "application/pdf",
                SizeBytes = 2048,
                Status = AttachmentWireStatus.Available
            }
        };

        var mapped = HistoryWireMapper.MapAttachments(source);

        var item = Assert.Single(mapped!);
        Assert.False(item.IsVoice);
        Assert.Null(item.VoiceCodec);
        Assert.Null(item.VoiceContainer);
        Assert.Null(item.VoiceDurationMs);
        Assert.Null(item.VoiceSampleRateHz);
        Assert.Null(item.VoiceChannels);
        Assert.Equal("plain-01", item.AttachmentId);
    }

    [Fact]
    public void MapAttachments_NullOrEmptyReturnsNull()
    {
        Assert.Null(HistoryWireMapper.MapAttachments(null));
        Assert.Null(HistoryWireMapper.MapAttachments([]));
    }
}