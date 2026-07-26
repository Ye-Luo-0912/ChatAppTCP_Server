using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime;

/// <summary>
/// 单一 RealtimeEvent 类型处理器。由 <see cref="RealtimeEventHandlerRegistry"/> 显式注册，
/// 不使用 attribute/Dictionary 自动发现（与 <c>ICommandHandler</c> 同样的约束）。
/// </summary>
internal interface IRealtimeEventHandler
{
    /// <summary>处理单个事件。实现不得抛出异常；校验失败应走 Reject 路径并返回。</summary>
    void Handle(RealtimeEvent realtimeEvent);
}
