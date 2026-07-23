using System.Buffers;
using System.Buffers.Binary;

namespace ChatApp.TcpGateway.Core.Protocol;

public static class PacketParser
{
    public static PacketParseStatus TryParse(
        ref ReadOnlySequence<byte> buffer,
        out PacketFrame frame)
    {
        frame = default;

        if (buffer.Length < PacketProtocol.HeaderSize)
        {
            return PacketParseStatus.NeedMoreData;
        }

        Span<byte> header = stackalloc byte[PacketProtocol.HeaderSize];
        buffer.Slice(0, PacketProtocol.HeaderSize).CopyTo(header);

        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != PacketProtocol.MagicNumber)
        {
            return PacketParseStatus.InvalidPacket;
        }

        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            header[PacketProtocol.LengthOffset..]);

        if (payloadLength is < 0 or > PacketProtocol.MaxPayloadSize)
        {
            return PacketParseStatus.InvalidPacket;
        }

        var frameLength = PacketProtocol.HeaderSize + payloadLength;
        if (buffer.Length < frameLength)
        {
            return PacketParseStatus.NeedMoreData;
        }

        var command = (PacketCommand)BinaryPrimitives.ReadUInt16LittleEndian(
            header[PacketProtocol.CommandOffset..]);

        frame = new PacketFrame(
            command,
            buffer.Slice(PacketProtocol.HeaderSize, payloadLength));

        buffer = buffer.Slice(frameLength);
        return PacketParseStatus.Success;
    }

    public static void WriteHeader(
        Span<byte> destination,
        PacketCommand command,
        int payloadLength)
    {
        if (destination.Length < PacketProtocol.HeaderSize)
        {
            throw new ArgumentException("Destination is smaller than the packet header.", nameof(destination));
        }

        if (payloadLength is < 0 or > PacketProtocol.MaxPayloadSize)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadLength));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination, PacketProtocol.MagicNumber);
        BinaryPrimitives.WriteUInt16LittleEndian(
            destination[PacketProtocol.CommandOffset..],
            (ushort)command);
        BinaryPrimitives.WriteInt32LittleEndian(
            destination[PacketProtocol.LengthOffset..],
            payloadLength);
    }
}
