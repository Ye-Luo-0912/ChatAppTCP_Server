namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// Shard Ingress Ring 中的消息封装。结构体避免装箱——存入 <see cref="Primitives.BoundedMpscRing{T}"/>。
/// <para>
/// 携带 Actor Key + 业务消息。Shard Consumer 取出后按 Key 路由到 ActorCell，
/// 按 MailboxMode 入队或替换，再驱动 Ready Queue。
/// </para>
/// </summary>
internal readonly struct ActorEnvelope<TKey, TMessage>
    where TKey : notnull
    where TMessage : struct
{
    public readonly TKey Key;
    public readonly TMessage Message;
    public readonly ActorAdmission? Admission;
    public readonly uint Generation;
    public readonly ActorEnvelopeKind Kind;

    public ActorEnvelope(
        in TKey key,
        in TMessage message,
        ActorAdmission? admission,
        uint generation,
        ActorEnvelopeKind kind)
    {
        Key = key;
        Message = message;
        Admission = admission;
        Generation = generation;
        Kind = kind;
    }
}

internal enum ActorEnvelopeKind : byte
{
    Message = 0,
    Completion = 1,
    Scheduled = 2
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
