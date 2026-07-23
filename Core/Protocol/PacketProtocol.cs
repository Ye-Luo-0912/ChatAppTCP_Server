namespace ChatApp.TcpGateway.Core.Protocol;

public static class PacketProtocol
{
    public const uint MagicNumber = 0x1A2B3C4D;
    public const int CommandOffset = sizeof(uint);
    public const int LengthOffset = sizeof(uint) + sizeof(ushort);
    public const int HeaderSize = sizeof(uint) + sizeof(ushort) + sizeof(int);
    public const int MaxPayloadSize = 80 * 1024;
}
