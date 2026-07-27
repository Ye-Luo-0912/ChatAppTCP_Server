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

        PacketCommand command;
        int payloadLength;
        PacketParseStatus headerStatus;
        if (buffer.FirstSpan.Length >= PacketProtocol.HeaderSize)
        {
            headerStatus = TryParseHeader(
                buffer.FirstSpan,
                out command,
                out payloadLength);
        }
        else
        {
            Span<byte> headerCopy = stackalloc byte[PacketProtocol.HeaderSize];
            buffer.Slice(0, PacketProtocol.HeaderSize).CopyTo(headerCopy);
            headerStatus = TryParseHeader(
                headerCopy,
                out command,
                out payloadLength);
        }

        if (headerStatus != PacketParseStatus.Success)
            return headerStatus;

        var frameLength = PacketProtocol.HeaderSize + payloadLength;
        if (buffer.Length < frameLength)
        {
            return PacketParseStatus.NeedMoreData;
        }

        frame = new PacketFrame(
            command,
            buffer.Slice(PacketProtocol.HeaderSize, payloadLength));

        buffer = buffer.Slice(frameLength);
        return PacketParseStatus.Success;
    }

    /// <summary>
    /// 只解析并校验固定 10 字节包头，不等待 Payload。供 Pipelines 与 DirectSocket
    /// 共用同一个命令方向、Magic 和命令级长度校验入口。
    /// </summary>
    public static PacketParseStatus TryParseHeader(
        ReadOnlySpan<byte> buffer,
        out PacketCommand command,
        out int payloadLength)
    {
        command = default;
        payloadLength = 0;
        if (buffer.Length < PacketProtocol.HeaderSize)
            return PacketParseStatus.NeedMoreData;

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(buffer);
        if (magic != PacketProtocol.MagicNumber)
            return PacketParseStatus.InvalidPacket;

        command = (PacketCommand)
            BinaryPrimitives.ReadUInt16LittleEndian(
                buffer[PacketProtocol.CommandOffset..]);
        payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            buffer[PacketProtocol.LengthOffset..]);
        if (payloadLength < 0)
            return PacketParseStatus.InvalidPacket;

        // 未定义/服务端命令为 -1；命令级超限在读取 Payload 前立即拒绝。
        var maxPayloadForCommand =
            PacketProtocol.GetMaxPayloadSize(command);
        return maxPayloadForCommand < 0 ||
               payloadLength > maxPayloadForCommand
            ? PacketParseStatus.InvalidPacket
            : PacketParseStatus.Success;
    }

    /// <summary>
    /// 尝试从缓冲区头部读取命令字段（不消费缓冲区）。
    /// <para>
    /// 用于在等待完整 Payload 前进行状态相关的早期校验
    ///（如未认证状态拒绝非认证命令），避免攻击者慢速发送大 Payload 占用资源。
    /// 仅在 Magic 匹配且缓冲区包含完整包头时返回 true。
    /// </para>
    /// </summary>
    public static bool TryPeekCommand(
        ReadOnlySequence<byte> buffer,
        out PacketCommand command)
    {
        command = default;

        if (buffer.Length < PacketProtocol.HeaderSize)
            return false;

        ushort commandRaw;
        if (buffer.FirstSpan.Length >= PacketProtocol.HeaderSize)
        {
            var header = buffer.FirstSpan[..PacketProtocol.HeaderSize];
            var magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
            if (magic != PacketProtocol.MagicNumber)
                return false;
            commandRaw = BinaryPrimitives.ReadUInt16LittleEndian(
                header[PacketProtocol.CommandOffset..]);
        }
        else
        {
            Span<byte> headerCopy = stackalloc byte[PacketProtocol.HeaderSize];
            buffer.Slice(0, PacketProtocol.HeaderSize).CopyTo(headerCopy);
            var magic = BinaryPrimitives.ReadUInt32LittleEndian(headerCopy);
            if (magic != PacketProtocol.MagicNumber)
                return false;
            commandRaw = BinaryPrimitives.ReadUInt16LittleEndian(
                headerCopy[PacketProtocol.CommandOffset..]);
        }

        command = (PacketCommand)commandRaw;
        return true;
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
