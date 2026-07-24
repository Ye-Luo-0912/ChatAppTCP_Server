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

        // 连续 FirstSpan 快路径：绝大多数完整 TCP 段无需复制 10 字节头。
        // stackalloc 不得赋给跨分支存活的 Span（CS8352），因此在分支内直接读字段。
        uint magic;
        ushort commandRaw;
        int payloadLength;
        if (buffer.FirstSpan.Length >= PacketProtocol.HeaderSize)
        {
            var header = buffer.FirstSpan[..PacketProtocol.HeaderSize];
            magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
            commandRaw = BinaryPrimitives.ReadUInt16LittleEndian(
                header[PacketProtocol.CommandOffset..]);
            payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
                header[PacketProtocol.LengthOffset..]);
        }
        else
        {
            Span<byte> headerCopy = stackalloc byte[PacketProtocol.HeaderSize];
            buffer.Slice(0, PacketProtocol.HeaderSize).CopyTo(headerCopy);
            magic = BinaryPrimitives.ReadUInt32LittleEndian(headerCopy);
            commandRaw = BinaryPrimitives.ReadUInt16LittleEndian(
                headerCopy[PacketProtocol.CommandOffset..]);
            payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
                headerCopy[PacketProtocol.LengthOffset..]);
        }

        if (magic != PacketProtocol.MagicNumber || payloadLength is < 0 or > PacketProtocol.MaxPayloadSize)
        {
            return PacketParseStatus.InvalidPacket;
        }

        var frameLength = PacketProtocol.HeaderSize + payloadLength;
        if (buffer.Length < frameLength)
        {
            return PacketParseStatus.NeedMoreData;
        }

        var command = (PacketCommand)commandRaw;

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
