namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 连接准入状态机：显式跟踪连接在生命周期中的准入阶段，
/// 替代从 <c>UserId &gt; 0</c> 推断，防止连接计数泄漏。
/// <para>
/// 状态转换：<c>Unauthenticated</c> → <c>Promoted</c>（认证成功）→
/// <c>Released</c>（连接关闭清理时）。
/// </para>
/// <para>
/// P0-4 / 主线二子项 2：Resume Commit 失败时 <c>UserId</c> 已设置但
/// <c>Promoted</c> 从未标记，清理路径据此正确递减未认证计数。
/// </para>
/// </summary>
public enum AdmissionState : byte
{
    /// <summary>
    /// 连接已建立但未完成认证（含 ClientHello 握手阶段、AuthenticationRequest 处理中、
    /// Resume Prepare/Commit 失败）。清理时须递减未认证计数。
    /// </summary>
    Unauthenticated = 0,

    /// <summary>
    /// 认证成功或 Resume Commit 成功，已占用已认证连接槽位。
    /// 由 <c>MarkAdmissionPromoted</c> 设置。
    /// </summary>
    Promoted = 1,

    /// <summary>
    /// 连接已关闭，准入槽位已释放。防止重复递减。
    /// 由清理路径 <c>ReleaseAdmission</c> 设置。
    /// </summary>
    Released = 2,
}
