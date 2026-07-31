namespace ChatApp.TcpGateway.Core.Messaging.Relationships;

/// <summary>
/// 主线四：关系操作命令响应（S2C）。
/// </summary>
public sealed class RelationshipCommandResponse
{
    public required string RequestId { get; init; }

    public required bool Succeeded { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>操作类型（回显）。</summary>
    public RelationshipOperation? Operation { get; init; }

    /// <summary>目标用户 Id（回显）。</summary>
    public long? TargetUserId { get; init; }

    /// <summary>资源 Id（好友请求 Id / 友谊 Id / 黑名单记录 Id）。</summary>
    public string? ResourceId { get; init; }
}
