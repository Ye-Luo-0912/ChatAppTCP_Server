using System.Threading.Channels;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// P0-5：基于 <see cref="Channel{T}"/> 的成熟出站队列实现（生产默认）。
/// <para>
/// 语义与 <see cref="LazySegmentedOutboundQueue"/> 一致（有界 MPSC FIFO）：
/// 满时 <see cref="TryWrite"/> 返回 false（调用方关闭连接）；
/// <see cref="TryComplete"/> 后 <see cref="TryWrite"/> 返回 false、
/// <see cref="WaitToReadAsync"/> 排空残留帧后返回 false。
/// </para>
/// <para>
/// 内部使用 <see cref="BoundedChannelFullMode.Wait"/> + <c>TryWrite</c>：
/// <c>TryWrite</c> 非阻塞，满时立即返回 false，与 LazySegmented 行为一致。
/// </para>
/// </summary>
internal sealed class BoundedChannelOutboundQueue : IOutboundQueue
{
    private readonly Channel<OutboundWrite> _channel;

    public BoundedChannelOutboundQueue(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(capacity, 0);
        _channel = Channel.CreateBounded<OutboundWrite>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public bool TryWrite(OutboundWrite item) =>
        _channel.Writer.TryWrite(item);

    public bool TryRead(out OutboundWrite item) =>
        _channel.Reader.TryRead(out item);

    public bool TryPeek(out OutboundWrite item)
    {
        // Channel<T> 无 TryPeek；有界通道下 Count 反映当前可读项数。
        // 由单消费者调用，语义等价于"是否有可读项"。
        item = default;
        return _channel.Reader.Count > 0;
    }

    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken);

    public void TryComplete() =>
        _channel.Writer.TryComplete();
}