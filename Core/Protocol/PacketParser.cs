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

        if (magic != PacketProtocol.MagicNumber || payloadLength < 0)
        {
            return PacketParseStatus.InvalidPacket;
        }

        // P0-5：解析包头后立即按命令校验 Payload 上限，不等完整 Payload 到达。
        // - 未定义命令 / 服务端→客户端命令 → GetMaxPayloadSize 返回 -1 → 立即拒绝
        // - 命令级长度超限 → 立即拒绝
        // 防止攻击者声明小命令（如 Heartbeat）却附带 80 KiB Payload 慢速发送占用缓冲。
        var command = (PacketCommand)commandRaw;
        var maxPayloadForCommand = PacketProtocol.GetMaxPayloadSize(command);
        if (maxPayloadForCommand < 0 || payloadLength > maxPayloadForCommand)
        {
            return PacketParseStatus.InvalidPacket;
        }

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
    /// 尝试从缓冲区头部读取命令字段（不消费缓冲区）。
    /// <para>
    /// P0-5：用于在等待完整 Payload 前进行状态相关的早期校验
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
