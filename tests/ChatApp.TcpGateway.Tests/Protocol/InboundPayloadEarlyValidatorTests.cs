using System.Buffers;
using System.Text;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Tests.Protocol;

public sealed class InboundPayloadEarlyValidatorTests
{
    [Fact]
    public void RejectsEmptyPayload()
    {
        var ok = InboundPayloadEarlyValidator.TryValidateChatMessage(
            ReadOnlySequence<byte>.Empty,
            maxAttachments: 32,
            maxAttachmentIdLength: 64,
            out var code,
            out _);

        Assert.False(ok);
        Assert.Equal(InboundPayloadEarlyValidator.EmptyMessageCode, code);
    }

    [Fact]
    public void RejectsTooManyAttachmentsBeforeFullDeserialize()
    {
        var ids = string.Join(
            ',',
            Enumerable.Range(0, 33).Select(static i => $"\"{i:x8}\""));
        var json = $$"""{"targetUserId":1,"content":"x","attachmentIds":[{{ids}}]}""";

        var ok = InboundPayloadEarlyValidator.TryValidateChatMessage(
            ToSequence(json),
            maxAttachments: 32,
            maxAttachmentIdLength: 64,
            out var code,
            out _);

        Assert.False(ok);
        Assert.Equal(InboundPayloadEarlyValidator.TooManyAttachmentsCode, code);
    }

    [Fact]
    public void RejectsOversizedAttachmentId()
    {
        var id = new string('a', 65);
        var json = $$"""{"targetUserId":1,"attachmentIds":["{{id}}"]}""";

        var ok = InboundPayloadEarlyValidator.TryValidateChatMessage(
            ToSequence(json),
            maxAttachments: 32,
            maxAttachmentIdLength: 64,
            out var code,
            out _);

        Assert.False(ok);
        Assert.Equal(InboundPayloadEarlyValidator.InvalidAttachmentIdCode, code);
    }

    [Fact]
    public void AcceptsAttachmentOnlyMessage()
    {
        var json = """{"targetUserId":1,"content":"","attachmentIds":["abcd1234"]}""";

        var ok = InboundPayloadEarlyValidator.TryValidateChatMessage(
            ToSequence(json),
            maxAttachments: 32,
            maxAttachmentIdLength: 64,
            out var code,
            out _);

        Assert.True(ok);
        Assert.Equal(string.Empty, code);
    }

    [Theory]
    [InlineData(0, 1024, true)]
    [InlineData(1024, 1024, true)]
    [InlineData(1025, 1024, false)]
    [InlineData(-1, 1024, false)]
    public void PayloadLimitUsesConfiguredCeiling(
        long length,
        int max,
        bool expected)
    {
        Assert.Equal(
            expected,
            InboundPayloadEarlyValidator.IsPayloadWithinLimit(length, max));
    }

    private static ReadOnlySequence<byte> ToSequence(string json) =>
        new(Encoding.UTF8.GetBytes(json));
}
