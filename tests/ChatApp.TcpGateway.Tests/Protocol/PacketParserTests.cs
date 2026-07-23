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
