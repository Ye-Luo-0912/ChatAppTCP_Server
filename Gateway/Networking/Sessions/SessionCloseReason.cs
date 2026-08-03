namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal enum SessionCloseReason : byte
{
    None,
    RemoteClosed,
    ApplicationStopping,
    AuthenticationTimedOut,
    IdleTimedOut,
    ProtocolViolation,
    RateLimitExceeded,
    AuthenticationRejected,
    OutboundQueueFull,
    SendTimedOut,
    SessionRevoked,
    /// <summary>三-3：账号被冻结，活跃会话由 UserLifecycleChangedHandler 关闭。</summary>
    AccountSuspended,
    TransportError,
    /// <summary>全局入站缓冲预算耗尽。</summary>
    InboundBudgetExceeded,
    /// <summary>DirectSocket 帧装配超时：客户端发送 Header 或 Payload 过慢。</summary>
    SlowFrameAssembly
}
