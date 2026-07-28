namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Runtime 未把消息成功交给 Behavior 时的终止原因。
/// 持有池化缓冲区、预算租约或其他外部资源的消息可据此安全释放所有权。
/// </summary>
public enum ActorMessageDropReason : byte
{
    Replaced = 0,
    MailboxFull = 1,
    ActorClosed = 2,
    ActivationFailed = 3,
    BehaviorFaulted = 4,
    ActorCompleted = 5,
    IdleTimeout = 6,
    RuntimeStopping = 7,
    StaleGeneration = 8,
    DeadlineRejected = 9,

    /// <summary>Actor 数达到 MaxActiveActors / MaxActiveActorsPerShard 上限，新 Actor 激活被拒绝。</summary>
    AdmissionRejected = 10,

    /// <summary>宿主通过 TryDeactivate 显式回收 Actor，其 Mailbox 剩余消息被丢弃。</summary>
    ExplicitlyDeactivated = 11
}
