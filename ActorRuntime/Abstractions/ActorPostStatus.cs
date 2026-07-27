namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// 同步入队 <see cref="IActorRuntime{TKey,TState,TMessage}.TryTell"/> 的结果。
/// 不创建 Task/CTS/CompletionSource，仅返回状态由调用方决定后续动作。
/// </summary>
public enum ActorPostStatus : byte
{
    /// <summary>消息被接收（FIFO 入队或 Latest-only 替换旧值）。</summary>
    Accepted = 0,

    /// <summary>Latest-only 模式下替换了同 Key 旧消息（仍是 Accepted 语义）。</summary>
    Replaced = 1,

    /// <summary>FIFO Mailbox 满。调用方应拒绝、返回 Overloaded 或关闭连接（Durable/Critical）。</summary>
    MailboxFull = 2,

    /// <summary>Shard Ingress Ring 满：跨线程生产者临时超过 Shard 容量。</summary>
    ShardOverloaded = 3,

    /// <summary>Actor 已 Deactivate 或从未 Activate。</summary>
    ActorClosed = 4,

    /// <summary>消息引用的 generation 已过期（迟到的 OperationCompleted 等）。</summary>
    StaleGeneration = 5,

    /// <summary>Runtime 正在停止或已停止。</summary>
    RuntimeStopping = 6
}
