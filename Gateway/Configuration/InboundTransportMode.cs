namespace ChatApp.TcpGateway.Gateway.Configuration;

/// <summary>
/// 单连接入站读取与解析模式。两条路径保持相同 wire、鉴权、限流和命令调度语义，
/// 仅改变 Socket 字节进入 PacketFrame 的方式。
/// </summary>
public enum InboundTransportMode : byte
{
    /// <summary>System.IO.Pipelines 基线实现，可随时回退。</summary>
    Pipelines = 0,

    /// <summary>固定接收缓冲区 + 增量状态机，跨缓冲大帧才租 payload。</summary>
    DirectSocket = 1
}
