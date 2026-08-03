using ChatApp.TcpGateway.Core.Messaging.Relationships;

namespace ChatApp.TcpGateway.Core.Messaging.Sync;

/// <summary>
/// 关系列表增量同步水位（C2S）。
/// <para>
/// 客户端按 <see cref="ListType"/> 维度维护本地水位，表示已处理所有
/// occurred_at_ms &lt;= <see cref="AfterChangedAtMs"/> 的关系变更事件。
/// </para>
/// </summary>
public sealed class RelationshipSyncWatermark
{
    /// <summary>关系列表类型（Friends / FriendRequests / BlockedUsers）。</summary>
    public required RelationshipListType ListType { get; init; }

    /// <summary>客户端已处理到的变更水位（Unix ms）。</summary>
    public long AfterChangedAtMs { get; init; }
}
