namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// 协议级错误码。统一用于 Error 帧（PacketCommand.Error = 500）。
/// 客户端依据 <see cref="IsFatal"/> 决定是否重试当前请求或重连：
/// Fatal = true 时不应重试相同请求（协议违规、不支持的命令/版本）；
/// Fatal = false 时可按 <c>RetryAfterMs</c> 提示重试或重新登录。
/// </summary>
public enum ProtocolErrorCode : ushort
{
    /// <summary>占位值，不应发送。</summary>
    None = 0,

    // === 致命错误（不可重试，应关闭连接） ===

    /// <summary>协议违规：magic 不匹配、长度越界、未认证状态发送业务命令等。</summary>
    ProtocolViolation = 1,

    /// <summary>命令不支持或未识别。</summary>
    UnsupportedCommand = 2,

    /// <summary>协议版本不支持。客户端应升级后重连。</summary>
    UnsupportedVersion = 3,

    /// <summary>JSON 反序列化失败或字段校验不通过。</summary>
    InvalidPayload = 4,

    // === 可重试错误（连接可保持或重新建立） ===

    /// <summary>未认证或认证已失效。客户端应重新发起 AuthenticationRequest。</summary>
    AuthRequired = 10,

    /// <summary>认证被拒绝（凭据无效、设备不匹配、依赖不可用）。</summary>
    AuthRejected = 11,

    /// <summary>会话已被吊销。客户端应重新登录。</summary>
    SessionRevoked = 12,

    /// <summary>ResumeToken 无效或已过期。客户端应走完整认证流程。</summary>
    ResumeFailed = 13,

    /// <summary>请求频率超限。客户端应按 <c>RetryAfterMs</c> 退避后重试。</summary>
    RateLimited = 20,

    /// <summary>Payload 超过该命令允许的上限。客户端应减小 payload 后重试。</summary>
    PayloadTooLarge = 21,

    /// <summary>服务端过载。客户端应按 <c>RetryAfterMs</c> 退避或重连其他实例。</summary>
    ServerOverloaded = 30,

    /// <summary>服务端正在排空连接（滚动升级、优雅停机）。客户端应重连其他实例。</summary>
    Shutdown = 31,

    /// <summary>出站队列饱和。客户端应等待或重连。</summary>
    OutboundQueueFull = 32,

    /// <summary>未分类内部错误。客户端可重试。</summary>
    InternalError = 99
}

/// <summary>
/// 错误码扩展：判断是否为致命错误。
/// </summary>
public static class ProtocolErrorCodeExtensions
{
    /// <summary>
    /// 致命错误：客户端不应重试相同请求，通常需关闭连接。
    /// 非致命错误：客户端可按 RetryAfterMs 退避后重试，或保持连接。
    /// </summary>
    public static bool IsFatal(this ProtocolErrorCode code) => code switch
    {
        ProtocolErrorCode.ProtocolViolation => true,
        ProtocolErrorCode.UnsupportedCommand => true,
        ProtocolErrorCode.UnsupportedVersion => true,
        ProtocolErrorCode.InvalidPayload => true,
        _ => false
    };
}
