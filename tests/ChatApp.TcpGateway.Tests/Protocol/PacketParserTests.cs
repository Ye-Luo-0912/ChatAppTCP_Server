using System.Buffers;
using System.Buffers.Binary;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Tests.Protocol;

public sealed class PacketParserTests
{
    [Fact]
    public void TryParseReturnsNeedMoreDataWhenHeaderIsSplit()
    {
        var bytes = CreateFrame(PacketCommand.Heartbeat, []);
        var sequence = SequenceFactory.Create(
            bytes.AsMemory(0, 5),
            bytes.AsMemory(5, 3));

        var status = PacketParser.TryParse(
            ref sequence,
            out _);

        Assert.Equal(PacketParseStatus.NeedMoreData, status);
    }

    [Fact]
    public void TryParseParsesMultipleFramesWithoutCopyingPayload()
    {
        var first = CreateFrame(
            PacketCommand.AuthenticationRequest,
            [1, 2, 3]);
        var second = CreateFrame(
            PacketCommand.Heartbeat,
            []);

        var combined = new byte[first.Length + second.Length];
        first.CopyTo(combined, 0);
        second.CopyTo(combined, first.Length);

        var sequence = SequenceFactory.Create(
            combined.AsMemory(0, 7),
            combined.AsMemory(7, 5),
            combined.AsMemory(12));

        var firstStatus = PacketParser.TryParse(
            ref sequence,
            out var firstFrame);
        var secondStatus = PacketParser.TryParse(
            ref sequence,
            out var secondFrame);

        Assert.Equal(PacketParseStatus.Success, firstStatus);
        Assert.Equal(
            PacketCommand.AuthenticationRequest,
            firstFrame.Command);
        Assert.Equal(
            new byte[] { 1, 2, 3 },
            firstFrame.Payload.ToArray());

        Assert.Equal(PacketParseStatus.Success, secondStatus);
        Assert.Equal(PacketCommand.Heartbeat, secondFrame.Command);
        Assert.True(sequence.IsEmpty);
    }

    [Fact]
    public void TryParseRejectsPayloadLargerThanProtocolLimit()
    {
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(
            header,
            PacketCommand.ChatMessage,
            payloadLength: 0);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset),
            PacketProtocol.MaxPayloadSize + 1);

        var sequence = new ReadOnlySequence<byte>(header);
        var status = PacketParser.TryParse(
            ref sequence,
            out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    [Fact]
    public void TryParseRejectsInvalidMagicNumber()
    {
        var bytes = CreateFrame(PacketCommand.Heartbeat, []);
        bytes[0] ^= 0xFF;

        var sequence = new ReadOnlySequence<byte>(bytes);
        var status = PacketParser.TryParse(
            ref sequence,
            out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    // --- 按命令校验 Payload 上限 ---

    [Fact]
    public void TryParseRejectsHeartbeatWithNonZeroPayload()
    {
        // Heartbeat 上限为 0；附带任意 Payload 立即拒绝，不等完整帧到达。
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(
            header,
            PacketCommand.Heartbeat,
            payloadLength: 0);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset),
            value: 1);

        var sequence = new ReadOnlySequence<byte>(header);
        var status = PacketParser.TryParse(ref sequence, out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    [Fact]
    public void TryParseRejectsUndefinedCommand()
    {
        // 原始 ushort 值 999 不对应任何已定义命令 → GetMaxPayloadSize 返回 -1 → 立即拒绝。
        var header = new byte[PacketProtocol.HeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(header, PacketProtocol.MagicNumber);
        BinaryPrimitives.WriteUInt16LittleEndian(
            header.AsSpan(PacketProtocol.CommandOffset),
            value: 999);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset),
            value: 0);

        var sequence = new ReadOnlySequence<byte>(header);
        var status = PacketParser.TryParse(ref sequence, out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    [Fact]
    public void TryParseRejectsServerToClientCommand()
    {
        // AuthenticationResponse 是服务端→客户端命令，客户端不应发送 → 返回 -1 → 立即拒绝。
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(
            header,
            PacketCommand.AuthenticationResponse,
            payloadLength: 0);

        var sequence = new ReadOnlySequence<byte>(header);
        var status = PacketParser.TryParse(ref sequence, out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    [Fact]
    public void TryParseRejectsPayloadExceedingPerCommandLimit()
    {
        // ChatMessage 上限 64 KiB；声明 70 KiB（低于全局 80 KiB 但超过命令级上限）→ 立即拒绝。
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(
            header,
            PacketCommand.ChatMessage,
            payloadLength: 0);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset),
            value: 70 * 1024);

        var sequence = new ReadOnlySequence<byte>(header);
        var status = PacketParser.TryParse(ref sequence, out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    [Fact]
    public void TryParseRejectsPerCommandLimitBeforeFullPayloadArrives()
    {
        // 仅提供包头（声明 TypingNotify + 600 字节 Payload），不提供 Payload 字节。
        // 旧实现返回 NeedMoreData（等待完整帧）；新实现立即返回 InvalidPacket。
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(
            header,
            PacketCommand.TypingNotify,
            payloadLength: 0);
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset),
            value: 600); // TypingNotify 上限 512

        var sequence = new ReadOnlySequence<byte>(header);
        var status = PacketParser.TryParse(ref sequence, out _);

        Assert.Equal(PacketParseStatus.InvalidPacket, status);
    }

    [Fact]
    public void TryParseAcceptsPayloadAtPerCommandLimit()
    {
        // TypingNotify 上限 512；恰好 512 字节 → Success（需提供完整帧）。
        var payload = new byte[512];
        var bytes = CreateFrame(PacketCommand.TypingNotify, payload);

        var sequence = new ReadOnlySequence<byte>(bytes);
        var status = PacketParser.TryParse(ref sequence, out var frame);

        Assert.Equal(PacketParseStatus.Success, status);
        Assert.Equal(PacketCommand.TypingNotify, frame.Command);
        Assert.Equal(512, frame.Payload.Length);
    }

    // --- TryPeekCommand ---

    [Fact]
    public void TryPeekCommandReturnsCommandWithoutConsumingBuffer()
    {
        var bytes = CreateFrame(PacketCommand.ChatMessage, new byte[64]);
        var sequence = new ReadOnlySequence<byte>(bytes);

        Assert.True(PacketParser.TryPeekCommand(sequence, out var command));
        Assert.Equal(PacketCommand.ChatMessage, command);

        // Buffer 未被消费，TryParse 仍可完整解析。
        var status = PacketParser.TryParse(ref sequence, out var frame);
        Assert.Equal(PacketParseStatus.Success, status);
        Assert.Equal(PacketCommand.ChatMessage, frame.Command);
    }

    [Fact]
    public void TryPeekCommandReturnsFalseForInvalidMagic()
    {
        var bytes = CreateFrame(PacketCommand.Heartbeat, []);
        bytes[0] ^= 0xFF;

        var sequence = new ReadOnlySequence<byte>(bytes);
        Assert.False(PacketParser.TryPeekCommand(sequence, out _));
    }

    [Fact]
    public void TryPeekCommandReturnsFalseWhenHeaderIncomplete()
    {
        var bytes = CreateFrame(PacketCommand.Heartbeat, []);
        var sequence = new ReadOnlySequence<byte>(bytes.AsMemory(0, 5));

        Assert.False(PacketParser.TryPeekCommand(sequence, out _));
    }

    private static byte[] CreateFrame(
        PacketCommand command,
        byte[] payload)
    {
        var frame = new byte[
            PacketProtocol.HeaderSize + payload.Length];
        PacketParser.WriteHeader(
            frame,
            command,
            payload.Length);
        payload.CopyTo(frame, PacketProtocol.HeaderSize);
        return frame;
    }
}

internal static class SequenceFactory
{
    public static ReadOnlySequence<byte> Create(
        params ReadOnlyMemory<byte>[] segments)
    {
        if (segments.Length == 0)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        var first = new Segment(segments[0]);
        var last = first;

        for (var index = 1; index < segments.Length; index++)
        {
            last = last.Append(segments[index]);
        }

        return new ReadOnlySequence<byte>(
            first,
            0,
            last,
            last.Memory.Length);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = next;
            return next;
        }
    }
}
