using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

internal static class OutboundFrameFactory
{
    private const int InitialCapacity = PacketProtocol.HeaderSize + 256;
    private const int MaximumCapacity =
        PacketProtocol.HeaderSize + PacketProtocol.MaxPayloadSize;

    /// <summary>
    /// Heartbeat ACK 是固定的 10 字节帧（仅包头，payload 长度 0）。
    /// 使用静态 pinned 帧避免每次 Heartbeat 重复分配 PooledBufferWriter + byte[] + SharedOutboundFrame。
    /// </summary>
    private static readonly SharedOutboundFrame HeartbeatAckFrame =
        CreatePinnedHeartbeatAck();

    private static SharedOutboundFrame CreatePinnedHeartbeatAck()
    {
        var buffer = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(buffer, PacketCommand.HeartbeatAcknowledgement, payloadLength: 0);
        return SharedOutboundFrame.CreatePinned(buffer, PacketProtocol.HeaderSize);
    }

    /// <summary>
    /// 返回静态共享的 Heartbeat ACK 帧（调用方仍需 TryQueue，但无需 Dispose）。
    /// </summary>
    public static SharedOutboundFrame GetHeartbeatAck() => HeartbeatAckFrame;

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
