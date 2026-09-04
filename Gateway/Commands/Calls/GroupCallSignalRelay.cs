using System.Globalization;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Commands.Calls;

/// <summary>群组通话 grant 签名配置（GROUP-CALL-1）。与 Server <c>JwtSettings.Secret</c> 同源。</summary>
/// <remarks>
/// 密钥未配置（空/空白）时群组中继整体禁用：群组命令 fail-closed 返回
/// <see cref="TcpCallErrorCode.GrantInvalid"/>，不影响 1:1 既有链路。
/// </remarks>
internal sealed class GroupCallGrantOptions
{
    public const string SectionName = "CallGrantSigning";

    /// <summary>群组 grant 的 HMAC-SHA256 共享密钥（canonical 载荷见 <c>TcpCallGrantSignature</c>）。</summary>
    public string? Secret { get; set; }
}

/// <summary>
/// 群组信令中继裁决结果。
/// </summary>
/// <param name="Succeeded">grant 校验与授权是否通过。</param>
/// <param name="ErrorCode">失败时的稳定错误码（<see cref="TcpCallErrorCode.GrantInvalid"/> /
/// <see cref="TcpCallErrorCode.GrantExpired"/> / <see cref="TcpCallErrorCode.BadRequest"/>）。</param>
/// <param name="ErrorMessage">面向用户的失败说明。</param>
/// <param name="Signals">通过时按参与者名单扇出的信号（已排除发起者会话）。</param>
/// <param name="RelayState">成功响应回显的通话状态（无状态中继的语义提示值）。</param>
/// <param name="RelayEndReason">成功响应回显的终态原因（仅 State=Ended 时非 None）。</param>
internal sealed record GroupCallRelayVerdict(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    IReadOnlyList<TcpCallSignal> Signals,
    TcpCallState RelayState,
    TcpCallEndReason RelayEndReason)
{
    public static GroupCallRelayVerdict Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, [], TcpCallState.Idle, TcpCallEndReason.None);
}

/// <summary>
/// 群通话阶段一（Mesh ≤4 人）的<b>无状态</b>信令中继（GROUP-CALL-1）。
/// <para>
/// Gateway 不维护房间状态机：授权完全由 Server 签发的群组 grant 承载——grant 的 HMAC 签名
/// 覆盖 CallId、发起者、过期时间、nonce 与<b>全部参与者名单</b>（<c>TcpCallGrantSignature</c>）。
/// 中继逐命令校验 grant（结构 + 过期 + 签名 + actor 成员资格），通过后把信令按参与者名单
/// 扇出到其余成员的在线会话：invite/accept/reject/cancel/ringing/reconnect 原样中继（既有
/// <see cref="TcpCallCommandType"/> kind），成员主动离开（End）转为
/// <see cref="TcpCallConstants.SignalEventParticipantLeft"/> 事件。成员变更 = 新 grant 批次 +
/// revision 递增，客户端以 revision/成员列表自洽（见 group-call-sfu-design.md §4.1/§4.2）。
/// </para>
/// <para>
/// 群组命令<b>不进入</b> Realtime 1:1 状态机（<see cref="ICallBackend"/>）——群组 grant 的
/// CalleeUserId 恒为 0，旧双人校验端天然拒绝，两阶段互不干扰。
/// </para>
/// </summary>
internal sealed class GroupCallSignalRelay
{
    private readonly string? _secret;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<GroupCallSignalRelay> _logger;

    public GroupCallSignalRelay(
        IOptions<GroupCallGrantOptions> options,
        TimeProvider timeProvider,
        ILogger<GroupCallSignalRelay> logger)
    {
        _secret = NormalizeSecret(options.Value.Secret);
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <summary>群组中继是否已配置密钥（未配置时所有群组命令 fail-closed）。</summary>
    public bool IsEnabled => _secret is not null;

    /// <summary>该 grant 是否为群组 grant（群组命令走中继路径，其余走既有 1:1 后端）。</summary>
    public static bool IsGroupGrant(TcpCallGrant? grant) => grant is { CallKind: TcpCallKind.Group };

    /// <summary>
    /// 校验群组命令并构造扇出信号。失败 fail-closed（不触碰任何转发路径）。
    /// </summary>
    public GroupCallRelayVerdict Evaluate(
        string requestId,
        TcpCallCommandRequest request,
        TcpCallGrant grant,
        long actorUserId)
    {
        var callId = request.CallId;
        var nowMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        if (!IsEnabled)
            return GroupCallRelayVerdict.Fail(
                TcpCallErrorCode.GrantInvalid, "群组通话中继未配置签名密钥。");

        if (!string.Equals(grant.CallId, callId, StringComparison.Ordinal))
            return GroupCallRelayVerdict.Fail(
                TcpCallErrorCode.GrantInvalid, "群组 grant 与通话 Id 不匹配。");

        // 结构 + 过期 + 签名（HMAC 覆盖全部参与者；名单非规范化形态直接拒绝）。
        if (!TcpCallGrantSignature.TryVerify(grant, _secret, nowMs, out var errorCode))
        {
            return errorCode == TcpCallErrorCode.GrantExpired
                ? GroupCallRelayVerdict.Fail(TcpCallErrorCode.GrantExpired, "群组通话授权已过期。")
                : GroupCallRelayVerdict.Fail(TcpCallErrorCode.GrantInvalid, "群组通话授权无效。");
        }

        // actor 成员资格：invite/cancel 仅限主叫；其余命令限名单内成员（非成员 fail-closed）。
        var isCaller = actorUserId == grant.CallerUserId;
        var isMember = grant.Participants?.Contains(actorUserId) == true;
        var allowed = request.Type switch
        {
            TcpCallCommandType.Invite or TcpCallCommandType.Cancel => isCaller,
            TcpCallCommandType.Ringing
                or TcpCallCommandType.Accept
                or TcpCallCommandType.Reject
                or TcpCallCommandType.End
                or TcpCallCommandType.Reconnect => isMember,
            _ => false
        };
        if (!allowed)
        {
            return GroupCallRelayVerdict.Fail(
                TcpCallErrorCode.GrantInvalid, "发起者不在群组通话成员名单内。");
        }

        var occurredAtMs = nowMs;
        var recipients = grant.Participants!.Where(id => id != actorUserId);
        var signals = recipients
            .Select(recipient => new TcpCallSignal
            {
                // 幂等去重键：同一命令（含重放）对同一成员产生稳定 SignalId。
                SignalId = string.Create(
                    CultureInfo.InvariantCulture,
                    $"{request.CommandId}:{recipient}"),
                CallId = callId,
                FromUserId = actorUserId,
                ToUserId = recipient,
                Kind = request.Type,
                Sdp = request.Sdp ?? string.Empty,
                Revision = request.Revision,
                OccurredAtMs = occurredAtMs,
                Event = request.Type == TcpCallCommandType.End
                    ? TcpCallConstants.SignalEventParticipantLeft
                    : null,
                ParticipantUserId = request.Type == TcpCallCommandType.End
                    ? actorUserId
                    : null
            })
            .ToArray();

        _logger.GroupCallRelayed(callId, actorUserId, (int)request.Type, signals.Length);

        return new GroupCallRelayVerdict(
            Succeeded: true,
            ErrorCode: null,
            ErrorMessage: null,
            Signals: signals,
            RelayState: ResolveRelayState(request.Type),
            RelayEndReason: ResolveRelayEndReason(request.Type));
    }

    /// <summary>无状态中继回显的状态提示值（房间状态由客户端按成员/revision 自洽）。</summary>
    private static TcpCallState ResolveRelayState(TcpCallCommandType type) => type switch
    {
        TcpCallCommandType.Invite or TcpCallCommandType.Ringing => TcpCallState.Ringing,
        TcpCallCommandType.Accept or TcpCallCommandType.Reconnect => TcpCallState.Active,
        TcpCallCommandType.Reject => TcpCallState.Ended,
        TcpCallCommandType.Cancel => TcpCallState.Ended,
        TcpCallCommandType.End => TcpCallState.Ended,
        _ => TcpCallState.Idle,
    };

    private static TcpCallEndReason ResolveRelayEndReason(TcpCallCommandType type) => type switch
    {
        TcpCallCommandType.Reject => TcpCallEndReason.Rejected,
        TcpCallCommandType.Cancel => TcpCallEndReason.Cancelled,
        TcpCallCommandType.End => TcpCallEndReason.HungUp,
        _ => TcpCallEndReason.None,
    };

    private static string? NormalizeSecret(string? secret) =>
        string.IsNullOrWhiteSpace(secret) ? null : secret;
}
