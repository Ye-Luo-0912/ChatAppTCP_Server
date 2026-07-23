using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;

namespace ChatApp.TcpGateway.Networking.Buffers;

internal static class OutboundFrameFactory
{
    private const int InitialCapacity = PacketProtocol.HeaderSize + 256;
    private const int MaximumCapacity =
        PacketProtocol.HeaderSize + PacketProtocol.MaxPayloadSize;

    public static SharedOutboundFrame Create<T>(
        PacketCommand command,
        IPayloadCodec<T> codec,
        T value)
    {
        using var writer = new PooledBufferWriter(
            InitialCapacity,
            MaximumCapacity);

        writer.Advance(PacketProtocol.HeaderSize);
        codec.Serialize(writer, value);

        var payloadLength = writer.WrittenCount - PacketProtocol.HeaderSize;
        PacketParser.WriteHeader(
            writer.WrittenSpan,
            command,
            payloadLength);

        return writer.Detach();
    }

    public static SharedOutboundFrame CreateEmpty(PacketCommand command)
    {
        using var writer = new PooledBufferWriter(
            PacketProtocol.HeaderSize,
            MaximumCapacity);

        writer.Advance(PacketProtocol.HeaderSize);
        PacketParser.WriteHeader(writer.WrittenSpan, command, payloadLength: 0);
        return writer.Detach();
    }
}
