namespace ChatApp.TcpGateway.Core.Server;

/// <summary>
/// 服务端实例身份。服务启动时生成或从配置加载，用于协议握手（ServerHello.serverDeviceId）
/// 和跨网关路由标识。
/// </summary>
public interface IServerIdentity
{
    /// <summary>
    /// 服务端实例标识（128 位 GUID 的 32 字符十六进制表示）。
    /// 客户端可据此实现亲和性，重连时优先选择相同实例。
    /// </summary>
    string ServerDeviceId { get; }

    /// <summary>协商后的协议版本。</summary>
    ushort ProtocolVersion { get; }

    /// <summary>服务端能力位掩码。</summary>
    uint FeatureBits { get; }
}
