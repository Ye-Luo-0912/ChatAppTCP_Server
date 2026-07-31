namespace ChatApp.TcpGateway.Core.Messaging.Relationships;

/// <summary>
/// 主线四：关系列表查询响应（S2C）。
/// </summary>
public sealed class RelationshipListResponse
{
    public required string RequestId { get; init; }

    public required bool Succeeded { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>关系条目列表。</summary>
    public IReadOnlyList<RelationshipItem>? Items { get; init; }

    /// <summary>下一页游标。null 表示无更多数据。</summary>
    public string? NextCursor { get; init; }

    /// <summary>是否还有更多数据可分页获取。</summary>
    public bool HasMore { get; init; }
}
