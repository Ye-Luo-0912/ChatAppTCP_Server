namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Identifies the transport-side operation that failed for a connection.
/// </summary>
public enum GatewayTransportOperation : byte
{
    ClientProcessing = 1,
    SendLoop = 2
}
