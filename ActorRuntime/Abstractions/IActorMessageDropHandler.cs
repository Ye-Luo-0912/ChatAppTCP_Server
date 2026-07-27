namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// 消息未被 Behavior 正常接管时的资源释放回调。
/// Runtime 保证每条被替换或丢弃的消息最多调用一次。
/// </summary>
public interface IActorMessageDropHandler<TMessage>
    where TMessage : struct
{
    void OnDropped(in TMessage message, ActorMessageDropReason reason);
}
