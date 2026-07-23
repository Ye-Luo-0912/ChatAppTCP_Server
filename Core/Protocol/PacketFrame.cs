using System.Buffers;

namespace ChatApp.TcpGateway.Core.Protocol;

public readonly record struct PacketFrame(
    PacketCommand Command,
    ReadOnlySequence<byte> Payload);
