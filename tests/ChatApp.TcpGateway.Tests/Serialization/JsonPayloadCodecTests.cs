using System.Buffers;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Tests.Protocol;

namespace ChatApp.TcpGateway.Tests.Serialization;

public sealed class JsonPayloadCodecTests
{
    private static readonly JsonPayloadCodec<ChatMessage> Codec =
        new(GatewayJsonSerializerContext.Default.ChatMessage);

    [Fact]
    public void DeserializeReadsMultiSegmentJsonWithoutFlatteningContract()
    {
        var message = new ChatMessage
        {
            MessageId = Guid.CreateVersion7().ToString("N"),
            SenderUserId = 11,
            TargetUserId = 22,
            Content = "hello",
            SentUtc = DateTime.UtcNow
        };

        var writer = new ArrayBufferWriter<byte>();
        Codec.Serialize(writer, message);
        var bytes = writer.WrittenMemory;

        var split = Math.Max(1, bytes.Length / 2);
        var sequence = SequenceFactory.Create(
            bytes[..split],
            bytes[split..]);

        var decoded = Codec.Deserialize(sequence);

        Assert.NotNull(decoded);
        Assert.Equal(message.MessageId, decoded.MessageId);
        Assert.Equal(message.SenderUserId, decoded.SenderUserId);
        Assert.Equal(message.TargetUserId, decoded.TargetUserId);
        Assert.Equal(message.Content, decoded.Content);
    }
}

