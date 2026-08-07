namespace ChatApp.TcpGateway.Core.Messaging.Relationships;

/// <summary>
/// 主线四：关系列表查询请求（C2S）。
/// <para>
/// 支持分页：PageSize + Cursor。Cursor 为 opaque 字符串，由上一次响应的 NextCursor 提供。
/// </para>
/// </summary>
public sealed class RelationshipListRequest
{
    public required string RequestId { get; init; }

    /// <summary>列表类型。</summary>
    public required RelationshipListType ListType { get; init; }

    /// <summary>页大小（1-200）。null 或 0 表示默认值 50。</summary>
    public int? PageSize { get; init; }

    /// <summary>分页游标（opaque）。null 表示首页。</summary>
    public string? Cursor { get; init; }
}
