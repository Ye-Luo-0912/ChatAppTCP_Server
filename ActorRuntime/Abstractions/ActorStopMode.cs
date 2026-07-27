namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Runtime 停止模式。控制 <see cref="IActorRuntime{TKey,TState,TMessage}.StopAsync"/> 的行为。
/// </summary>
public enum ActorStopMode : byte
{
    /// <summary>
    /// 立即停止：丢弃所有 Mailbox 中残留消息，Deactivate 所有 Actor。
    /// 用于宿主崩溃式停止或测试快速清理。
    /// </summary>
    Immediate = 0,

    /// <summary>
    /// 排空模式：拒绝新消息，等待所有 Mailbox 中已有消息处理完成或超时。
    /// 用于优雅停机：让 Chat/Receipt 等命令完成后再关闭。
    /// </summary>
    Drain = 1
}
