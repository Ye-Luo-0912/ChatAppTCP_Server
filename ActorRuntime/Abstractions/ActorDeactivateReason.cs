namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Actor 被 Deactivate 的原因。传给 <see cref="IActorBehavior{TKey,TState,TMessage}.Deactivate"/>。
/// </summary>
public enum ActorDeactivateReason : byte
{
    /// <summary>Actor 显式返回 <see cref="ActorTurnResult.Complete"/>。</summary>
    Completed = 0,

    /// <summary>Actor 空闲超过 <see cref="ActorRuntimeOptions.ActorIdleTimeout"/>。</summary>
    IdleTimeout = 1,

    /// <summary>Runtime 正在停止，所有 Actor 被强制 Deactivate。</summary>
    RuntimeStopping = 2,

    /// <summary>Behavior 抛出未处理异常：Shard 不能继续驱动该 Actor。</summary>
    Faulted = 3
}
