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
    AuthorizationCompleted = 1
}

/// <summary>
/// Typing Actor 消息：值类型联合，避免装箱。
/// <para>
/// Notify 消息携带目标用户、typing 状态与时间戳；AuthorizationCompleted 仅携带授权结果。
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

    private TypingActorMessage(
        TypingActorMessageKind kind,
        bool isTyping,
        long timestamp,
        long sessionGeneration,
        bool authorized)
    {
        Kind = kind;
        IsTyping = isTyping;
        Timestamp = timestamp;
        SessionGeneration = sessionGeneration;
        Authorized = authorized;
    }

    /// <summary>构造 typing 状态变更通知。</summary>
    public static TypingActorMessage Notify(
        bool isTyping,
        long timestamp,
        long sessionGeneration)
        => new(TypingActorMessageKind.Notify, isTyping, timestamp, sessionGeneration, authorized: false);

    /// <summary>构造授权完成回调。</summary>
    public static TypingActorMessage AuthorizationCompleted(bool authorized)
        => new(TypingActorMessageKind.AuthorizationCompleted, isTyping: false, timestamp: 0, sessionGeneration: 0, authorized: authorized);
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
}
