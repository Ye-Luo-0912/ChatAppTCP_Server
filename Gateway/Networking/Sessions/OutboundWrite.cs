using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal readonly record struct OutboundWrite(
    SharedOutboundFrame Frame,
    int ByteCount,
    SessionCloseReason? CloseAfterSend);
