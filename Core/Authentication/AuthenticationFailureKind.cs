namespace ChatApp.TcpGateway.Core.Authentication;

public enum AuthenticationFailureKind : byte
{
    None,
    InvalidCredentials,
    DeviceMismatch,
    DependencyUnavailable,

    /// <summary>三-3：账号已被冻结，认证拒绝。</summary>
    UserFrozen
}
