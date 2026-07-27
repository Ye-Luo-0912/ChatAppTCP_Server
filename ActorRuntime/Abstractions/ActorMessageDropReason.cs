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
    DeadlineRejected = 9
}
