using ChatApp.Binary.Core;
using ChatApp.Shared.Protocol.Tcp.Binary.Schemas;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Gateway.Serialization;

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

    /// <summary>
    /// 连接级格式分流的出站入口：二进制会话走 <see cref="TcpBinaryWireEncoder"/> 寄存器
    /// （与解码寄存器同一份 schema 目录），JSON 会话沿用调用方注入的 codec。
    /// 格式在握手后不可变，单目标帧恰好编码一次。
    /// </summary>
    public static SharedOutboundFrame Create<T>(
        PacketCommand command,
        IPayloadCodec<T> codec,
        TcpClientSession session,
        T value)
        where T : class
    {
        if (session.NegotiatedPayloadFormat == PayloadFormat.Binary)
        {
            return CreateBinary(command, value);
        }

        return Create(command, codec, value);
    }

    /// <summary>
    /// 用二进制 schema 编码并组帧。先把本地 DTO 映射为共享规范 DTO（寄存器按具体类型分发，
    /// 本地 DTO 不在 schema 目录内，直接编码必然 SchemaNotCovered）；再在当前空闲缓冲内单遍编码
    /// （小 payload 0 重试）；<see cref="BinaryStatus.DestinationTooSmall"/> 时把缓冲增长到满
    /// payload 窗口重试一次（payload 上限 = <see cref="TcpBinaryPayloadCodec.DecodeLimits"/> =
    /// MaxPayloadSize），其余失败状态一律抛出，绝不发送半编码帧。
    /// </summary>
    public static SharedOutboundFrame CreateBinary<T>(PacketCommand command, T value)
        where T : class
    {
        var shared = BinaryPayloadMapper.ToShared(command, value);

        using var writer = new PooledBufferWriter(
            InitialCapacity,
            MaximumCapacity);

        writer.Advance(PacketProtocol.HeaderSize);

        var encode = TcpBinaryWireEncoder.TryEncode(
            shared,
            writer.GetSpan(0),
            TcpBinaryPayloadCodec.DecodeLimits);
        if (encode.Status == TcpBinaryWireEncodeStatus.EncodeFailure &&
            encode.EncodeStatus == BinaryStatus.DestinationTooSmall)
        {
            encode = TcpBinaryWireEncoder.TryEncode(
                shared,
                writer.GetSpan(PacketProtocol.MaxPayloadSize),
                TcpBinaryPayloadCodec.DecodeLimits);
        }

        if (encode.Status != TcpBinaryWireEncodeStatus.Encoded)
        {
            throw new InvalidOperationException(
                $"binary payload encode failed for {command}: {encode.EncodeStatus}");
        }

        writer.Advance(encode.Written);

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
