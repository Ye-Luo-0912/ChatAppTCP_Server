using ChatApp.TcpGateway.Core.Messaging.Relationships;

namespace ChatApp.TcpGateway.Core.Messaging.Sync;

/// <summary>
/// 关系列表增量同步结果（S2C）。
/// <para>
/// 与 <see cref="ConversationHistoryCatchUp"/> 平行，但以 list_type 为维度。
/// BlockedUsers 因底层表无变更时间戳，<see cref="NewAfterChangedAtMs"/> 始终为 0
/// （客户端需全量 diff）。
/// </para>
/// </summary>
public sealed record RelationshipCatchUp
{
    /// <summary>关系列表类型。</summary>
    public required RelationshipListType ListType { get; init; }

    /// <summary>当前列表项（全量或增量）。</summary>
    public IReadOnlyList<RelationshipItem> Items { get; init; } =
        Array.Empty<RelationshipItem>();

    /// <summary>是否还有更多数据（分页）。</summary>
    public bool HasMore { get; init; }

    /// <summary>下一页游标（opaque）。null 表示无更多数据。</summary>
    public string? NextCursor { get; init; }

    /// <summary>
    /// 服务端推进后的新水位。客户端应持久化此值作为下次同步的 AfterChangedAtMs。
    /// BlockedUsers 始终为 0。
    /// </summary>
    public long NewAfterChangedAtMs { get; init; }

    /// <summary>该列表类型是否需要客户端本地全量重置（水位无效时）。</summary>
    public bool ResetRequired { get; init; }

    /// <summary>重置原因（仅当 ResetRequired=true 时有效）。</summary>
    public string? ResetReason { get; init; }
}
