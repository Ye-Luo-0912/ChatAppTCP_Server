namespace ChatApp.TcpGateway.Core.Protocol;

public enum PacketParseStatus : byte
{
    NeedMoreData,
    Success,
    InvalidPacket
}
