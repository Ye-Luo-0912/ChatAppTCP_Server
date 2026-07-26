using ChatApp.TcpGateway.Gateway.Networking.Buffers;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal readonly record struct OutboundWrite(
    SharedOutboundFrame Frame,
    int ByteCount,
    SessionCloseReason? CloseAfterSend);
