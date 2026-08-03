namespace ChatApp.TcpGateway.Gateway.Configuration;

/// <summary>
/// P0-5：出站队列实现模式。用于配置回退，避免自定义 MPSC 队列直接作为唯一生产实现。
/// <para>
/// String 为 JSON 序列化友好命名，便于在配置文件中显式指定。
/// </para>
/// </summary>
public enum OutboundQueueMode
{
    /// <summary>
    /// 系统 <see cref="System.Threading.Channels.Channel{T}"/> 有界队列（FullMode.Wait + TryWrite）。
    /// <para>
    /// 成熟实现，生产默认。行为与历史版本一致：满时 TryWrite 返回 false（关闭连接），
    /// Complete 后 TryWrite 返回 false、WaitToReadAsync 排空后返回 false。
    /// </para>
    /// </summary>
    BoundedChannel = 0,

    /// <summary>
    /// Lazy Segmented MPSC 队列（<see cref="Networking.Sessions.LazySegmentedOutboundQueue"/>）。
    /// <para>
    /// 空闲连接零段分配，每连接节省约 87% 出站队列内存。仅供 A/B 对照与负载测试，
    /// 在完整 Transport Matrix（含 100M+ 随机操作与 8~24h 稳定运行）通过前不作为生产默认。
    /// </para>
    /// </summary>
    LazySegmented = 1
}