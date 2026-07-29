namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 设备租约接管结果：包含旧会话的 SessionId 和 ConnectionLeaseId。
/// 吊销事件优先匹配 ConnectionLeaseId，而非仅匹配 SessionId。
/// </summary>
/// <remarks>
/// P0-7 修复：Resume 复用原 SessionId 时，仅比较 SessionId 无法区分新旧连接，
/// 导致旧 TCP 连接与新连接共存。TakeOverAsync 返回此结构后，调用方按
/// <see cref="PreviousConnectionLeaseId"/> 判断是否存在需要吊销的旧连接，
/// 并将其写入 SessionRevoked 事件的 PayloadJson 供目标 Gateway 精确匹配。
/// </remarks>
public readonly record struct DeviceLeaseTakeoverResult
{
    /// <summary>旧会话的逻辑 SessionId（可能与新会话相同，如 Resume 复用场景）。</summary>
    public string? PreviousSessionId { get; init; }

    /// <summary>旧连接的租约 ID（唯一标识旧 TCP Transport）。</summary>
    public string? PreviousConnectionLeaseId { get; init; }
}
