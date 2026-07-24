using System.Buffers;
using System.Buffers.Binary;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class OutboundFrameFactoryTests
{
    [Fact]
    public void CreateWritesHeaderAndJsonIntoSinglePooledFrame()
    {
        var codec = new JsonPayloadCodec<AuthenticationResponse>(
            GatewayJsonSerializerContext.Default.AuthenticationResponse);
        var response = new AuthenticationResponse
        {
            Success = true,
            UserId = 42,
            SessionId = "session"
        };

        using var outbound = OutboundFrameFactory.Create(
            PacketCommand.AuthenticationResponse,
            codec,
            response);

        // P0-5 后 PacketParser.TryParse 拒绝服务端→客户端命令（GetMaxPayloadSize 返回 -1），
        // 因此这里直接读取包头验证，不走解析器。
        var span = outbound.Memory.Span;
        Assert.Equal(
            PacketProtocol.MagicNumber,
            BinaryPrimitives.ReadUInt32LittleEndian(span));
        Assert.Equal(
            (ushort)PacketCommand.AuthenticationResponse,
            BinaryPrimitives.ReadUInt16LittleEndian(
                span[PacketProtocol.CommandOffset..]));
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            span[PacketProtocol.LengthOffset..]);

        var payload = outbound.Memory.Slice(
            PacketProtocol.HeaderSize,
            payloadLength);
        var decoded = codec.Deserialize(
            new ReadOnlySequence<byte>(payload));

        Assert.NotNull(decoded);
        Assert.True(decoded.Success);
        Assert.Equal(42, decoded.UserId);
    }

    [Fact]
    public void SharedFrameRemainsAliveUntilEveryReferenceIsReleased()
    {
        using var owner = OutboundFrameFactory.CreateEmpty(
            PacketCommand.Heartbeat);

        Assert.True(owner.TryRetain());
        var retainedMemory = owner.Memory;

        owner.Dispose();

        Assert.Equal(
            PacketProtocol.HeaderSize,
            retainedMemory.Length);
        Assert.Equal(
            PacketProtocol.HeaderSize,
            owner.Memory.Length);
    }

    [Fact]
    public void PooledWriterRejectsWritesBeyondProtocolLimit()
    {
        using var writer = new PooledBufferWriter(
            initialCapacity: 32,
            maximumCapacity: 64);

        writer.Advance(32);

        Assert.Throws<InvalidOperationException>(
            () => writer.GetSpan(33));
    }
}

