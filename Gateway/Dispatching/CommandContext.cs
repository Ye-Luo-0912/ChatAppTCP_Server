
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
namespace ChatApp.TcpGateway.Gateway.Dispatching;

/// <summary>
/// <see cref="ICommandContext"/> 的默认实现。
/// 轻量 class（仅 2 个引用），每次命令分发时构造，分配开销可忽略。
/// </summary>
internal sealed class CommandContext : ICommandContext
{
    /// <inheritdoc />
    public TcpClientSession Session { get; }

    /// <inheritdoc />
    public string RemoteIp { get; }

    public CommandContext(TcpClientSession session, string remoteIp)
    {
        Session = session;
        RemoteIp = remoteIp;
    }
}
