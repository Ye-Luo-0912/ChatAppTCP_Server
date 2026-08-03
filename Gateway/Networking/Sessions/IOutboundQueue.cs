namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// P0-5：出站队列抽象。多生产者单消费者 FIFO，语义与
/// <c>Channel.CreateBounded&lt;OutboundWrite&gt;(N, FullMode=Wait)</c> 一致：
/// <list type="bullet">
/// <item>满时 <see cref="TryWrite"/> 返回 false（调用方关闭连接）；</item>
/// <item><see cref="TryComplete"/> 后 <see cref="TryWrite"/> 返回 false，</item>
/// <item><see cref="WaitToReadAsync"/> 排空残留帧后返回 false。</item>
/// </list>
/// 由 <see cref="Configuration.OutboundQueueMode"/> 选择实现：
/// <see cref="Configuration.OutboundQueueMode.BoundedChannel"/>（成熟实现，生产默认）或
/// <see cref="Configuration.OutboundQueueMode.LazySegmented"/>（自定义 MPSC 队列）。
/// </summary>
internal interface IOutboundQueue
{
    /// <summary>多生产者入队。满或已 Complete 时返回 false。</summary>
    bool TryWrite(OutboundWrite item);

    /// <summary>单消费者读取。无已发布项时返回 false。</summary>
    bool TryRead(out OutboundWrite item);

    /// <summary>单消费者窥视（不消费）。用于判断是否有可读项。</summary>
    bool TryPeek(out OutboundWrite item);

    /// <summary>单消费者异步等待可读项。有项返回 true；Complete 且排空后返回 false；取消抛 OCE。</summary>
    ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken);

    /// <summary>标记队列完成。后续 TryWrite 返回 false；WaitToReadAsync 排空后返回 false。</summary>
    void TryComplete();
}