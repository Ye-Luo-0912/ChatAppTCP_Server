using System.Buffers;
using ChatApp.TcpGateway.Core.Protocol;

// CommandLane enum 已迁移至 Core/Protocol/CommandCatalog.cs，
// 本命名空间通过 using 引用，保持现有代码引用 CommandLane 时不需改动命名空间。

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 已从 Pipe 复制的入站命令。消费者处理完后须归还 <see cref="RentedBuffer"/>（仅当 <see cref="IsPooled"/> 为 true）
/// 并释放 <see cref="ReservedInboundBytes"/>。
/// <para>
/// V2 重构：原 per-connection <c>SessionCommandScheduler</c>（3 Channel + 3 Consumer Task）已删除。
/// OrderedWrite/Query 命令由全局 <see cref="Executor.SessionCommandExecutor"/>（共享 worker 池）处理，
/// Inline/Ephemeral 命令在读循环内同步处理。本 struct 作为命令载体保留。
/// </para>
/// </summary>
internal readonly struct SessionCommand
{
    public required PacketCommand Command { get; init; }

    /// <summary>
    /// payload 缓冲区。长度为 0 表示无 payload（使用 <see cref="Array.Empty{T}"/>）。
    /// <para>
    /// 当 <see cref="IsPooled"/> 为 true 时，从 ArrayPool 租用，消费者须归还。
    /// 当 <see cref="IsPooled"/> 为 false 时，为普通分配（Ephemeral 命令），由 GC 回收。
    /// </para>
    /// </summary>
    public required byte[] RentedBuffer { get; init; }

    public required int PayloadLength { get; init; }

    /// <summary>
    /// 是否从 ArrayPool 租用。Ephemeral 命令使用普通分配以避免 DropOldest 丢弃时泄漏 ArrayPool 槽位。
    /// </summary>
    public required bool IsPooled { get; init; }

    /// <summary>
    /// 已从 <see cref="InboundBudget"/> 预留的字节数（通常等于 <see cref="PayloadLength"/>）。
    /// </summary>
    public int ReservedInboundBytes { get; init; }

    /// <summary>
    /// 全局入站预算；处理完成或丢弃时释放 <see cref="ReservedInboundBytes"/>。
    /// </summary>
    public GlobalInboundBudget? InboundBudget { get; init; }

    /// <summary>
    /// 目标会话。全局执行器回调通过此字段恢复 per-connection 上下文。
    /// </summary>
    public required TcpClientSession Session { get; init; }

    /// <summary>
    /// 客户端远程 IP（用于日志与回调）。
    /// </summary>
    public required string RemoteIp { get; init; }

    /// <summary>
    /// 从缓冲区构造 ReadOnlySequence，供 ProcessPacketAsync 使用。
    /// </summary>
    public ReadOnlySequence<byte> AsPayloadSequence() =>
        PayloadLength == 0
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(RentedBuffer, 0, PayloadLength);
}
