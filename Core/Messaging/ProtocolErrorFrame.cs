using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// Error 帧（PacketCommand.Error = 500）的 payload。
/// 服务端通过此帧向客户端传递协议级错误，附带 RetryAfter 提示。
/// </summary>
public sealed class ProtocolErrorFrame
{
    /// <summary>错误码。客户端依据此值决定是否重试。</summary>
    public ProtocolErrorCode Code { get; set; }

    /// <summary>
    /// 是否为致命错误。致命错误客户端不应重试相同请求，通常需关闭连接。
    /// 非致命错误客户端可按 <see cref="RetryAfterMs"/> 退避后重试。
    /// </summary>
    public bool Fatal { get; set; }

    /// <summary>
    /// 重试建议间隔（毫秒）。0 表示立即可重试；-1 或省略表示无建议。
    /// 仅对非致命错误有意义。
    /// </summary>
    public int? RetryAfterMs { get; set; }

    /// <summary>人类可读错误消息（英文，仅用于调试，客户端不应解析）。</summary>
    public string? Message { get; set; }

    /// <summary>
    /// 触发该错误的原始命令（若可识别）。0 表示未关联具体命令。
    /// 客户端可据此将错误映射到对应请求。
    /// </summary>
    public ushort? OriginCommand { get; set; }
}
