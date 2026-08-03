namespace ChatApp.TcpGateway.Core.Messaging.Sync;

/// <summary>
/// 关系变更日志操作类型（wire）。
/// <para>
/// 数值与 Realtime 侧 <see cref="ChatApp.Realtime.Abstractions.Relationships.RelationshipChangeOperation"/>
/// 一一对应，通过强制转换映射。
/// </para>
/// </summary>
public enum RelationshipChangeOperation : byte
{
    /// <summary>创建或状态变更（upsert）。</summary>
    Upsert = 0,

    /// <summary>删除（tombstone）。</summary>
    Delete = 1
}