namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Actor Mailbox 模式。
/// <para>
/// FIFO：消息严格按入队顺序处理，满时拒绝（Durable/Critical 命令、需要严格顺序的状态变更）。
/// LatestOnly：仅保留最新一条消息，旧值被静默替换（Typing/Presence/网络质量/进度更新等瞬态状态）。
/// </para>
/// </summary>
public enum ActorMailboxMode : byte
{
    /// <summary>
    /// 严格 FIFO 有界队列。满时 <see cref="ActorPostStatus.MailboxFull"/>。
    /// 用于 Session OrderedWrite/Query、Realtime Conversation Event 等需要严格顺序的路径。
    /// </summary>
    Fifo = 0,

    /// <summary>
    /// 仅保留最新一条消息。同 Key 新消息覆盖旧消息。
    /// 用于 Typing/Presence 等瞬态状态——只关心最新值，旧值无意义。
    /// </summary>
    LatestOnly = 1
}
