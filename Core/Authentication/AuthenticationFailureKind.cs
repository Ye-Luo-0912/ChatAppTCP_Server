namespace ChatApp.TcpGateway.Core.Authentication;

public enum AuthenticationFailureKind : byte
{
    None,
    InvalidCredentials,
    DeviceMismatch,
    DependencyUnavailable
}
