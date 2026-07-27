namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// <see cref="IActorBehavior{TKey,TState,TMessage}.Receive"/> 的返回值。
/// 控制 Actor Shard 调度循环：是否继续处理同 Actor 的下一条消息，还是挂起等待外部事件。
/// </summary>
public enum ActorTurnResult : byte
{
    /// <summary>
    /// 继续处理同 Actor Mailbox 中的下一条消息（如果存在）。
    /// 用于无 I/O 的纯状态变更命令。
    /// </summary>
    Continue = 0,

    /// <summary>
    /// 挂起当前 Actor：保留 Mailbox 中剩余消息，不再继续处理直到收到下一条消息或 Completion 回调。
    /// 用于提交 AsyncOperation 后等待外部 I/O 完成的场景。
    /// </summary>
    Suspend = 1,

    /// <summary>
    /// 处理完一条 OperationCompleted 后恢复 Mailbox 处理。
    /// 语义与 Continue 相同，但显式标记"等待结束、恢复消费"的语义。
    /// </summary>
    ResumeMailbox = 2,

    /// <summary>
    /// 完成 Actor：Deactivate 后不再接收消息。后续 TryTell 返回 <see cref="ActorPostStatus.ActorClosed"/>。
    /// 用于 Session 关闭、错误终止等。
    /// </summary>
    Complete = 3
}
