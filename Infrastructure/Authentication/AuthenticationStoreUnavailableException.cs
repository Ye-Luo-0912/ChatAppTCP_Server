namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal sealed class AuthenticationStoreUnavailableException(
    string message,
    Exception innerException)
    : Exception(message, innerException);
