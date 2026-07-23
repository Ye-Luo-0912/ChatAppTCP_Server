using System.Buffers;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Networking.Buffers;

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

        var sequence = new ReadOnlySequence<byte>(outbound.Memory);
        var status = PacketParser.TryParse(
            ref sequence,
            out var frame);
        var decoded = codec.Deserialize(frame.Payload);

        Assert.Equal(PacketParseStatus.Success, status);
        Assert.Equal(
            PacketCommand.AuthenticationResponse,
            frame.Command);
        Assert.NotNull(decoded);
        Assert.True(decoded.Success);
        Assert.Equal(42, decoded.UserId);
        Assert.True(sequence.IsEmpty);
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

