namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// TakeOver 操作的结果状态。
/// <para>
/// 显式三态替代旧的「返回值 + 抛异常」模式，使调用方无需 try/catch 即可区分
/// 成功接管、无旧租约、依赖不可用三种情况，便于在 Prepare/Commit 协议中
/// 按状态决定回滚或继续。
/// </para>
/// </summary>
public enum TakeOverStatus : byte
{
    /// <summary>
    /// 成功接管设备租约，且发现存在旧租约（跨 Gateway 旧连接需吊销）。
    /// <see cref="TakeOverResult.PreviousTransportId"/> 携带旧 TransportId。
    /// </summary>
    Success = 0,

    /// <summary>
    /// 成功接管设备租约，但无旧租约或旧租约与当前连接相同（无需吊销）。
    /// </summary>
    NoPreviousLease = 1,

    /// <summary>
    /// 依赖不可用（Redis 异常或熔断器开路），未执行接管。
    /// <see cref="TakeOverResult.Exception"/> 携带原始异常供日志/指标使用。
    /// 调用方应 fail-closed：拒绝 Resume、回滚已完成的本地状态、要求完整认证。
    /// </summary>
    DependencyUnavailable = 2
}

/// <summary>
/// 设备租约接管结果：显式三态 + 旧会话信息。
/// <para>
/// P1-A2：<see cref="PreviousTransportId"/> 是旧连接的公开路由标识（TransportId），
/// 与私有 <c>LeaseOwnerToken</c> 分离——TakeOver 只返回可广播的 TransportId 供吊销路由，
/// 不返回私有所有权凭证（旧凭证已被新接管覆盖，无意义）。
/// </para>
/// <para>
/// 吊销事件优先匹配 <see cref="PreviousTransportId"/>（即旧连接的 TransportId），
/// 而非仅匹配 <see cref="PreviousSessionId"/>——Resume 复用 SessionId 时必须按 TransportId 区分新旧。
/// </para>
/// </summary>
public readonly record struct TakeOverResult
{
    /// <summary>接管状态。</summary>
    public required TakeOverStatus Status { get; init; }

    /// <summary>
    /// 旧会话的逻辑 SessionId（可能与新会话相同，如 Resume 复用场景）。
    /// 仅在 <see cref="TakeOverStatus.Success"/> 时有意义。
    /// </summary>
    public string? PreviousSessionId { get; init; }

    /// <summary>
    /// 旧连接的 TransportId（公开路由标识，用于跨 Gateway 吊销匹配）。
    /// 仅在 <see cref="TakeOverStatus.Success"/> 且存在旧租约时非空。
    /// <para>
    /// P1-A2：原 <c>PreviousConnectionLeaseId</c> 重命名为 <c>PreviousTransportId</c>，
    /// 明确语义为「可广播的路由标识」而非「私有所有权凭证」。
    /// </para>
    /// </summary>
    public string? PreviousTransportId { get; init; }

    /// <summary>
    /// 依赖不可用时的原始异常（仅 <see cref="TakeOverStatus.DependencyUnavailable"/> 时非空）。
    /// 调用方用于日志与指标归因，不应重新抛出。
    /// </summary>
    public Exception? Exception { get; init; }

    /// <summary>成功接管且发现旧租约。</summary>
    public static TakeOverResult Success(string? previousSessionId, string? previousTransportId) =>
        new()
        {
            Status = TakeOverStatus.Success,
            PreviousSessionId = previousSessionId,
            PreviousTransportId = previousTransportId
        };

    /// <summary>成功接管但无旧租约（或旧租约与当前 lease 相同，无需吊销）。</summary>
    public static TakeOverResult NoPreviousLease() =>
        new() { Status = TakeOverStatus.NoPreviousLease };

    /// <summary>
    /// 依赖不可用：Redis 异常或熔断器开路。调用方应 fail-closed。
    /// </summary>
    public static TakeOverResult Unavailable(Exception exception) =>
        new() { Status = TakeOverStatus.DependencyUnavailable, Exception = exception };

    /// <summary>是否存在跨 Gateway 旧连接需要吊销（成功 + 旧 TransportId 非空）。</summary>
    public bool HasPreviousLease =>
        Status == TakeOverStatus.Success
        && !string.IsNullOrWhiteSpace(PreviousTransportId);
}
