using System.Buffers;
using System.Threading.Channels;
using ChatApp.TcpGateway.Core.Protocol;

// CommandLane enum 已迁移至 Core/Protocol/CommandCatalog.cs，
// 本命名空间通过 using 引用，保持现有代码引用 CommandLane 时不需改动命名空间。

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 每会话有界命令调度器。
/// <para>
/// 将命令按语义分到三条 lane，避免慢请求阻塞同连接的其他命令（队头阻塞）：
/// <list type="bullet">
/// <item>OrderedWrite：Chat/Receipt/Edit/Recall/Reaction/Group 等写操作，单消费者保持顺序。</item>
/// <item>Query：History/List/Sync/PresenceQuery 等查询操作，单消费者但与 OrderedWrite 并行。</item>
/// <item>Ephemeral：Typing 等瞬态命令，DropOldest 模式只保留最新帧，允许丢弃。</item>
/// </list>
/// Control 命令（Auth/Heartbeat/PresenceUnwatch）由读循环内联处理，不入队。
/// </para>
/// <para>
/// 入队前 payload 已从 Pipe 复制到缓冲区，Pipe 可立即回收。
/// OrderedWrite/Query 使用 ArrayPool 租用缓冲区 + Wait 模式，提供自然背压。
/// Ephemeral 使用普通分配 + 手动 DropOldest，被丢弃的帧归还入站预算后由 GC 回收。
/// </para>
/// </summary>
internal sealed class SessionCommandScheduler : IAsyncDisposable
{
    private readonly Channel<SessionCommand> _orderedWriteChannel;
    private readonly Channel<SessionCommand> _queryChannel;
    private readonly Channel<SessionCommand> _ephemeralChannel;
    private readonly Task _orderedWriteLoop;
    private readonly Task _queryLoop;
    private readonly Task _ephemeralLoop;
    private readonly CancellationTokenSource _cts;
    private readonly Action<Exception>? _onFatalError;
    private bool _disposed;

    public SessionCommandScheduler(
        Func<SessionCommand, CancellationToken, ValueTask> processor,
        int orderedWriteCapacity,
        int queryCapacity,
        int ephemeralCapacity,
        CancellationToken lifetimeToken,
        Action<Exception>? onFatalError = null)
    {
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(orderedWriteCapacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(queryCapacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ephemeralCapacity, 0);
        _onFatalError = onFatalError;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken);
        var token = _cts.Token;

        _orderedWriteChannel = Channel.CreateBounded<SessionCommand>(
            new BoundedChannelOptions(orderedWriteCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });

        _queryChannel = Channel.CreateBounded<SessionCommand>(
            new BoundedChannelOptions(queryCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });

        // Ephemeral：Wait + 手动 DropOldest，以便在丢弃时释放入站预算。
        _ephemeralChannel = Channel.CreateBounded<SessionCommand>(
            new BoundedChannelOptions(ephemeralCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });

        _orderedWriteLoop = RunLaneAsync(
            _orderedWriteChannel.Reader,
            processor,
            token);
        _queryLoop = RunLaneAsync(
            _queryChannel.Reader,
            processor,
            token);
        _ephemeralLoop = RunLaneAsync(
            _ephemeralChannel.Reader,
            processor,
            token);
    }

    /// <summary>
    /// 入队 OrderedWrite 命令。Channel 满时等待（自然背压）。
    /// </summary>
    public async ValueTask EnqueueOrderedAsync(
        SessionCommand command,
        CancellationToken cancellationToken)
    {
        await _orderedWriteChannel.Writer
            .WriteAsync(command, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 入队 Query 命令。Channel 满时等待（自然背压）。
    /// </summary>
    public async ValueTask EnqueueQueryAsync(
        SessionCommand command,
        CancellationToken cancellationToken)
    {
        await _queryChannel.Writer
            .WriteAsync(command, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 入队 Ephemeral 命令。非阻塞：Channel 满时丢弃最旧帧并释放其入站预算，保留最新状态。
    /// <para>
    /// 调用方须确保 <paramref name="command"/> 使用普通分配（<see cref="SessionCommand.IsPooled"/>=false），
    /// 因为被 DropOldest 丢弃的帧无法归还 ArrayPool。
    /// </para>
    /// </summary>
    /// <returns>true 表示已入队；false 表示调度器已关闭。</returns>
    public bool TryEnqueueEphemeral(SessionCommand command)
    {
        if (_ephemeralChannel.Writer.TryWrite(command))
            return true;

        // Channel 已满或已关闭：尝试丢弃最旧帧后重试。
        if (_ephemeralChannel.Reader.TryRead(out var dropped))
            ReleaseCommandResources(dropped);

        return _ephemeralChannel.Writer.TryWrite(command);
    }

    private async Task RunLaneAsync(
        ChannelReader<SessionCommand> reader,
        Func<SessionCommand, CancellationToken, ValueTask> processor,
        CancellationToken token)
    {
        try
        {
            await foreach (var command in reader.ReadAllAsync(token))
            {
                try
                {
                    await processor(command, token).ConfigureAwait(false);
                }
                finally
                {
                    ReleaseCommandResources(command);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Expected shutdown path.
        }
        catch (ChannelClosedException)
        {
            // Channel completed during shutdown.
        }
        catch (Exception ex)
        {
            // processor 应已捕获所有预期异常；此处仅为兜底。
            _onFatalError?.Invoke(ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _orderedWriteChannel.Writer.TryComplete();
        _queryChannel.Writer.TryComplete();
        _ephemeralChannel.Writer.TryComplete();

        try
        {
            await _orderedWriteLoop.ConfigureAwait(false);
        }
        catch
        {
            // Lane exceptions are observed via onFatalError callback or swallowed on dispose.
        }

        try
        {
            await _queryLoop.ConfigureAwait(false);
        }
        catch
        {
        }

        try
        {
            await _ephemeralLoop.ConfigureAwait(false);
        }
        catch
        {
        }

        DrainChannel(_orderedWriteChannel);
        DrainChannel(_queryChannel);
        DrainChannel(_ephemeralChannel);

        _cts.Dispose();
    }

    private static void DrainChannel(Channel<SessionCommand> channel)
    {
        while (channel.Reader.TryRead(out var command))
        {
            ReleaseCommandResources(command);
        }
    }

    private static void ReleaseCommandResources(in SessionCommand command)
    {
        if (command.IsPooled && command.RentedBuffer.Length > 0)
            ArrayPool<byte>.Shared.Return(command.RentedBuffer);

        if (command.ReservedInboundBytes > 0 && command.InboundBudget is not null)
            command.InboundBudget.Release(command.ReservedInboundBytes);
    }
}

/// <summary>
/// 已从 Pipe 复制的入站命令。消费者处理完后须归还 <see cref="RentedBuffer"/>（仅当 <see cref="IsPooled"/> 为 true）
/// 并释放 <see cref="ReservedInboundBytes"/>。
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
    /// 从缓冲区构造 ReadOnlySequence，供 ProcessPacketAsync 使用。
    /// </summary>
    public ReadOnlySequence<byte> AsPayloadSequence() =>
        PayloadLength == 0
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(RentedBuffer, 0, PayloadLength);
}


