using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// Typing Actor 路由键：(SenderUserId, TargetUserId)。
/// <para>
/// 从 conversationId (dm:lo:hi) 解析出的发送方与接收方。二者可推导原始 conversationId：
/// <c>dm:{min(sender,target)}:{max(sender,target)}</c>。Key 内不含字符串引用，避免 GC 压力。
/// </para>
/// <para>
/// 同一发送方在同一会话的多次 typing 状态变更共享同一 Actor，
/// LatestOnly Mailbox 自动合并 typing=true→typing=false。
/// </para>
/// </summary>
internal readonly record struct TypingActorKey(long SenderUserId, long TargetUserId)
{
    /// <summary>
    /// 重建 conversationId 字符串。仅在 Activate 时调用一次并缓存到 State，
    /// 避免热路径上反复构造字符串。
    /// </summary>
    public string ToConversationId() =>
        ConversationId.CreateDirect(SenderUserId, TargetUserId);
}

/// <summary>
/// Typing Actor 消息类型。区分业务通知与授权完成回调。
/// </summary>
internal enum TypingActorMessageKind : byte
{
    /// <summary>客户端 typing 状态变更通知。</summary>
    Notify = 0,

    /// <summary>授权 I/O 完成（成功或失败，结果在 <see cref="TypingActorMessage.Authorized"/>）。</summary>
    AuthorizationCompleted = 1,

    /// <summary>
    /// 授权失效通知（关系变更触发）：清空 Actor 内缓存的 Authorized=true，
    /// 下一次 Notify 必须重新走授权 I/O。不携带业务字段。
    /// </summary>
    AuthorizationInvalidated = 2
}

/// <summary>
/// Typing Actor 消息：值类型联合，避免装箱。
/// <para>
/// Notify 消息携带目标用户、typing 状态与时间戳；AuthorizationCompleted 携带授权结果与
/// 授权纪元（用于拒绝 stale Completion——失效后自增的 epoch 使旧 Completion 被拒绝）。
/// 不携带 byte[]/Session/RemoteIp 等通用 SessionCommand 字段，热路径零分配。
/// </para>
/// </summary>
internal readonly struct TypingActorMessage
{
    public readonly TypingActorMessageKind Kind;

    // --- Notify 字段 ---
    public readonly bool IsTyping;
    public readonly long Timestamp;
    public readonly long SessionGeneration;

    // --- AuthorizationCompleted 字段 ---
    public readonly bool Authorized;

    /// <summary>
    /// 授权纪元：AuthorizationCompleted 携带提交 I/O 时捕获的 epoch。
    /// Behavior 仅在 CompletionEpoch == state.AuthorizationEpoch 时接受结果，
    /// 否则视为 stale Completion（失效后旧 I/O 的回投）并拒绝。
    /// </summary>
    public readonly uint AuthorizationEpoch;

    private TypingActorMessage(
        TypingActorMessageKind kind,
        bool isTyping,
        long timestamp,
        long sessionGeneration,
        bool authorized,
        uint authorizationEpoch)
    {
        Kind = kind;
        IsTyping = isTyping;
        Timestamp = timestamp;
        SessionGeneration = sessionGeneration;
        Authorized = authorized;
        AuthorizationEpoch = authorizationEpoch;
    }

    /// <summary>构造 typing 状态变更通知。</summary>
    public static TypingActorMessage Notify(
        bool isTyping,
        long timestamp,
        long sessionGeneration)
        => new(TypingActorMessageKind.Notify, isTyping, timestamp, sessionGeneration, authorized: false, authorizationEpoch: 0);

    /// <summary>
    /// 构造授权完成回调，携带提交时捕获的授权纪元。
    /// Behavior 比较此 epoch 与 state.AuthorizationEpoch 以拒绝 stale Completion。
    /// </summary>
    public static TypingActorMessage AuthorizationCompleted(bool authorized, uint authorizationEpoch)
        => new(TypingActorMessageKind.AuthorizationCompleted, isTyping: false, timestamp: 0, sessionGeneration: 0, authorized, authorizationEpoch);

    /// <summary>构造授权失效通知（关系变更触发）。</summary>
    public static TypingActorMessage AuthorizationInvalidated()
        => new(TypingActorMessageKind.AuthorizationInvalidated, isTyping: false, timestamp: 0, sessionGeneration: 0, authorized: false, authorizationEpoch: 0);
}

/// <summary>
/// Typing Actor 状态。仅由 Shard Consumer 单线程修改，无需原子操作。
/// </summary>
internal struct TypingActorState
{
    /// <summary>在 Activate 时从 Key 构造一次，后续 fanout/TryAccept 复用。</summary>
    public string ConversationId;

    /// <summary>当前期望的 typing 状态（来自最新 Notify）。</summary>
    public bool DesiredIsTyping;

    /// <summary>上次已发射到 TypingFanoutCoordinator 的 typing 状态。</summary>
    public bool LastEmittedIsTyping;

    /// <summary>授权是否已获得（缓存）。授权过期后需重新提交 I/O。</summary>
    public bool Authorized;

    /// <summary>是否有授权 I/O 正在进行。</summary>
    public bool AuthPending;

    /// <summary>最近一次 Notify 的时间戳，用于过期判断。</summary>
    public long LastNotifyTimestamp;

    /// <summary>当前会话代次，用于检测会话切换。</summary>
    public long SessionGeneration;

    /// <summary>
    /// 授权有效期截止时间戳（<see cref="TimeProvider.GetTimestamp"/> 单位）。
    /// 超过此时间后下一次 Notify 必须重新走授权 I/O，避免关系变更后授权长期不过期。
    /// 0 表示尚未获得授权或需重新授权。
    /// </summary>
    public long AuthorizedUntilTimestamp;

    /// <summary>
    /// 授权纪元：每次授权 I/O 完成时自增。关系变更失效时自增全局纪元，
    /// 使 Actor 内缓存的 Authorized=true 立即作废（无需等待 TTL）。
    /// </summary>
    public uint AuthorizationEpoch;
}
