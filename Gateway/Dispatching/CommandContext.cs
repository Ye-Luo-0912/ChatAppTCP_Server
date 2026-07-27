using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Gateway.Dispatching;

/// <summary>
/// 单次命令执行的上下文。仅含每请求变化的值（session、remoteIp）。
/// <para>
/// <b>readonly struct</b>（非 class）：消除每命令的堆分配。仅 2 个引用（16 字节），
/// 按值传递开销可忽略。公共单例依赖（metrics、logger、timeProvider 等）由 handler 自己
/// 通过 DI 构造函数注入，不放在 context 内。
/// </para>
/// </summary>
internal readonly struct CommandContext
{
    /// <summary>当前连接的会话。</summary>
    public TcpClientSession Session { get; }

    /// <summary>客户端远端 IP（用于审计与限流）。</summary>
    public string RemoteIp { get; }

    public CommandContext(TcpClientSession session, string remoteIp)
    {
        Session = session;
        RemoteIp = remoteIp;
    }
}
