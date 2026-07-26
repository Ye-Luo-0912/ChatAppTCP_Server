namespace ChatApp.TcpGateway.Networking.Sessions;

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
    TransportError,
    /// <summary>全局入站缓冲预算耗尽。</summary>
    InboundBudgetExceeded
}
