
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
namespace ChatApp.TcpGateway.Gateway.Dispatching;

/// <summary>
/// 单次命令执行的上下文。仅含每请求变化的值（session、remoteIp）。
/// <para>
/// 公共单例依赖（metrics、logger、timeProvider 等）由 handler 自己通过 DI 构造函数注入，
/// 不放在 context 内，避免 context 膨胀。
/// </para>
/// </summary>
internal interface ICommandContext
{
    /// <summary>当前连接的会话。</summary>
    TcpClientSession Session { get; }

    /// <summary>客户端远端 IP（用于审计与限流）。</summary>
    string RemoteIp { get; }
}
