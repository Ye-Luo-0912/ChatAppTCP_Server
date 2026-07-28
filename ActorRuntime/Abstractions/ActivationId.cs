namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Actor 激活纪元。每次 Activate 从所属 Shard 的单调计数器分配新值（不按 Key 重置），
/// 用于识别迟到的 Completion / Deadline / Deactivate 是否属于当前激活实例（防 ABA）。
/// <para>
/// <see cref="ActivationId.None"/>（Value=0）保留为"未指定/匹配任意"语义，
/// 真实激活从 1 开始分配。
/// </para>
/// </summary>
public readonly record struct ActivationId(ulong Value)
{
    /// <summary>未指定激活纪元。用于 TryDeactivate 等"匹配当前任意激活"的调用。</summary>
    public static ActivationId None => default;

    /// <summary>是否为有效的激活纪元（非 None）。</summary>
    public bool IsValid => Value != 0;
}
