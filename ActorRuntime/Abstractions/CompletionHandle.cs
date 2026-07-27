namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// 池化 Completion 的轻量句柄。由 <c>PooledCompletionSource&lt;TResult&gt;</c> 租用后产生。
/// <para>
/// 业务消息携带此句柄，外部 I/O 完成后通过 <c>runtime.TryComplete(handle, result)</c>
/// 把结果投递回原 Actor（同 Shard 的 Ingress Ring）。
/// 句柄内含 SlotIndex + Generation，避免 ABA（槽位被复用后旧句柄误命中）。
/// </para>
/// </summary>
public readonly struct CompletionHandle
{
    /// <summary>池中的槽位索引。-1 表示无效句柄。</summary>
    public int SlotIndex { get; init; }

    /// <summary>槽位版本：每次 Rent 自增，TryComplete 时校验防止 ABA。</summary>
    public uint Generation { get; init; }

    /// <summary>结果类型标识：用于 TryComplete 时反序列化/类型转换的契约。
    /// 由租用方约定，Runtime 不解释此字段。</summary>
    public int ResultTypeId { get; init; }

    public bool IsValid => SlotIndex >= 0;

    public static CompletionHandle Invalid => default;
}
