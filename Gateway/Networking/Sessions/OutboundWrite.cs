using ChatApp.TcpGateway.Networking.Buffers;

namespace ChatApp.TcpGateway.Networking.Sessions;

internal readonly record struct OutboundWrite(
    SharedOutboundFrame Frame,
    int ByteCount,
    SessionCloseReason? CloseAfterSend);
