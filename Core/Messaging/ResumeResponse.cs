namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 服务端 → 客户端的断线重连响应（PacketCommand.ResumeResponse = 7）。
/// 服务端校验 ResumeToken 后发送，告知客户端恢复结果。
/// </summary>
public sealed class ResumeResponse
{
    /// <summary>是否恢复成功。</summary>
    public bool Success { get; set; }

    /// <summary>
    /// P1-B：失败种类分类（仅 <see cref="Success"/>=false 时有意义）。
    /// 客户端据此区分不可恢复的 Token 失效与可重试的依赖故障。
    /// </summary>
    public ResumeFailureKind FailureKind { get; set; }

    /// <summary>
    /// 恢复成功时返回新的 ResumeToken（旧 Token 已失效）。
    /// 失败时为 null，客户端应走完整认证流程。
    /// </summary>
    public string? ResumeToken { get; set; }

    /// <summary>用户 Id（恢复成功时回填）。</summary>
    public long UserId { get; set; }

    /// <summary>会话 Id（恢复成功时回填）。</summary>
    public string? SessionId { get; set; }

    /// <summary>设备 Id（恢复成功时回填）。</summary>
    public string? DeviceId { get; set; }

    /// <summary>
    /// 服务端记录的该会话最后已知的对话水位（用于客户端增量同步）。
    /// 客户端可据此发起 SyncBootstrapRequest 增量拉取缺失消息。
    /// </summary>
    public long? LastConversationSequence { get; set; }

    /// <summary>失败时的错误消息（仅调试用）。</summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// P1-B：依赖不可用时的重试退避建议（毫秒）。
    /// 仅在 <see cref="FailureKind"/>=<see cref="ResumeFailureKind.DependencyUnavailable"/> 时有意义。
    /// 客户端应在此时间后重试 Resume，或回退到完整认证。
    /// </summary>
    public int? RetryAfterMs { get; set; }
}
