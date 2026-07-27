using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Gateway.Dispatching;

/// <summary>
/// 单条客户端命令的处理器。
/// <para>
/// 每个命令类别一个实现（如 PushTokenCommandHandler、ReactionCommandHandler），
/// 内部按 <see cref="PacketCommand"/> 分发到具体处理方法。handler 自己持有相关 Codec 与业务端口，
/// 不依赖 <c>TcpGatewayService</c> 的私有字段。
/// </para>
/// <para>
/// 设计约束：不引入 MediatR、反射扫描、运行时 Attribute 查找或 Dictionary 驱动的通用框架。
/// handler 注册与分发均通过 <see cref="CommandDispatcher"/> 的手写 switch 完成。
/// </para>
/// <para>
/// <see cref="CommandContext"/> 为 readonly struct，按值传递，消除每命令堆分配。
/// </para>
/// </summary>
internal interface ICommandHandler
{
    /// <summary>
    /// 处理一条已通过鉴权前置与 payload 上限校验的命令帧。
    /// </summary>
    /// <param name="frame">已解析的命令帧，payload 在 <see cref="PacketFrame.Payload"/>。</param>
    /// <param name="context">本次命令的执行上下文（session、remoteIp，struct 按值传递）。</param>
    /// <param name="cancellationToken">会话生命周期令牌。</param>
    ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken);
}
