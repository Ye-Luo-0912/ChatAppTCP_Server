namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Identifies the external dependency a gateway operation targets.
/// Used as a structured log field (low-cardinality) and metrics tag.
/// </summary>
public enum GatewayDependency : byte
{
    Redis = 1,
    NatsCore = 2,
    JetStream = 3,
    RealtimeService = 4
}
