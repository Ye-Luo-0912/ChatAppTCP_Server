using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Server;

namespace ChatApp.TcpGateway.Infrastructure.Server;

/// <summary>
/// 服务端实例身份默认实现。启动时生成 128 位 ServerDeviceId（GUID），
/// 协议版本固定为 <see cref="PacketProtocol.CurrentProtocolVersion"/>。
/// </summary>
public sealed class ServerIdentity : IServerIdentity
{
    public string ServerDeviceId { get; }

    public ushort ProtocolVersion => PacketProtocol.CurrentProtocolVersion;

    public uint FeatureBits => 0;

    /// <param name="serverDeviceId">
    /// 显式指定 ServerDeviceId（32 字符十六进制 GUID）。
    /// null 或空时自动生成新 GUID。配置加载场景传入持久化值。
    /// </param>
    public ServerIdentity(string? serverDeviceId = null)
    {
        ServerDeviceId = string.IsNullOrWhiteSpace(serverDeviceId)
            ? Guid.NewGuid().ToString("N")
            : serverDeviceId;
    }
}
