using ChatApp.TcpGateway.Gateway.Networking.Buffers;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 出站队列元素。Critical（Auth/SessionRevoked/Error）和 Durable（Chat/Receipt/Edit）通过
/// <see cref="TryQueue"/> 入队；Ephemeral（Typing/Presence）通过 <see cref="TryQueueEphemeral"/>
/// 写入独立 mailbox 后由 flush sentinel 触发排空。
/// </summary>
/// <param name="Frame">帧数据；为 null 时表示 Ephemeral flush sentinel，发送循环应排空 mailbox。</param>
/// <param name="ByteCount">帧字节预算占用；sentinel 为 0。</param>
/// <param name="CloseAfterSend">发完即断原因；仅 Critical 路径使用。</param>
internal readonly record struct OutboundWrite(
    SharedOutboundFrame? Frame,
    int ByteCount,
    SessionCloseReason? CloseAfterSend);

/// <summary>
/// Ephemeral 帧的 coalescing key：相同 key 的最新状态覆盖旧状态，不同 key 独立共存。
/// <para>
/// Typing: Kind=1, Id1=SenderUserId, Id2=ConversationIdHash（同一发送者+会话只保留最新 typing 状态，
/// 不同发送者互不覆盖）。
/// Presence: Kind=2, Id1=UserId, Id2=0（同一用户的在线状态只保留最新）。
/// </para>
/// </summary>
internal readonly record struct EphemeralKey(byte Kind, long Id1, long Id2)
{
    public const byte KindTyping = 1;
    public const byte KindPresence = 2;

    public static EphemeralKey Typing(long senderUserId, long conversationIdHash) =>
        new(KindTyping, senderUserId, conversationIdHash);

    public static EphemeralKey Presence(long userId) =>
        new(KindPresence, userId, 0);

    /// <summary>
    /// 将 ConversationId 字符串确定性哈希为 long。
    /// 使用 FNV-1a 64-bit（确定性，不依赖进程随机种子），用于 Typing coalescing key。
    /// </summary>
    public static long HashConversationId(string? conversationId)
    {
        if (string.IsNullOrEmpty(conversationId))
            return 0;
        var hash = -3750763034362895579L; // FNV-1a 64-bit offset basis
        foreach (var c in conversationId)
        {
            hash ^= c;
            hash *= 1099511628211L; // FNV-1a 64-bit prime
        }
        return hash;
    }
}

/// <summary>
/// Ephemeral mailbox 中的条目：持有帧引用与字节预算占用。
/// 被 newer 帧覆盖时需 Dispose 帧 + 释放预算；Version 由 mailbox 在 lock 内赋值，
/// 用于关闭竞态下精确条件移除，防止相同 Frame 重复入槽时发生 ABA。
/// </summary>
internal readonly record struct EphemeralEntry(
    SharedOutboundFrame Frame,
    int ByteCount,
    long Version = 0);
