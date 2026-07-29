using ChatApp.ActorRuntime.Abstractions;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// Shard Ingress Ring 中的消息封装。结构体避免装箱——存入 <see cref="Primitives.BoundedMpscRing{T}"/>。
/// <para>
/// 携带 Actor Key + 业务消息。Shard Consumer 取出后按 Key 路由到 ActorCell，
/// 按 MailboxMode 入队或替换，再驱动 Ready Queue。
/// Completion / Deactivate 属于控制信封：不占用 Mailbox 准入容量，直接驱动控制通道或生命周期。
/// Deadline 触发不经过 Ingress——由 Shard 单线程 DeadlineWheel 在 Consumer Loop 内直接投递控制通道。
/// </para>
/// </summary>
internal readonly struct ActorEnvelope<TKey, TMessage>
    where TKey : notnull
    where TMessage : struct
{
    public readonly TKey Key;
    public readonly TMessage Message;
    public readonly ActorAdmission? Admission;
    public readonly ActivationId Activation;
    public readonly ActorDeactivateReason DeactivateReason;
    public readonly ActorEnvelopeKind Kind;

    public ActorEnvelope(
        in TKey key,
        in TMessage message,
        ActorAdmission? admission,
        ActivationId activation,
        ActorEnvelopeKind kind,
        ActorDeactivateReason deactivateReason = default)
    {
        Key = key;
        Message = message;
        Admission = admission;
        Activation = activation;
        Kind = kind;
        DeactivateReason = deactivateReason;
    }
}

internal enum ActorEnvelopeKind : byte
{
    Message = 0,
    Completion = 1,

    /// <summary>
    /// 显式 Deactivate 请求（TryDeactivate）。携带 ActivationId：
    /// <see cref="ActivationId.None"/> 匹配当前任意激活；否则仅当精确匹配时生效。
    /// </summary>
    Deactivate = 2,

    /// <summary>
    /// 控制通道 Invalidations（如 Typing 授权失效）。不占用 Mailbox 准入容量，
    /// 也不需要 Completion Credit。路由到 ActorCell 的 Invalidation 控制槽，
    /// 优先级高于 Completion（确保失效在处理过期 Completion 前生效）。
    /// 幂等：多次 Invalidation 等效于一次。
    /// </summary>
    Invalidation = 3
}

internal readonly struct ActorMailboxItem<TMessage>
    where TMessage : struct
{
    public readonly TMessage Message;
    public readonly ActorAdmission? Admission;

    public ActorMailboxItem(in TMessage message, ActorAdmission? admission)
    {
        Message = message;
        Admission = admission;
    }
}
