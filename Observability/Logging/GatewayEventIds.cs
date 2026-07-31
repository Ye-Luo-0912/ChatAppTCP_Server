namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Global EventId allocation for the TCP gateway observability contract.
/// EventId expresses a stable event structure, not source code ordering.
/// </summary>
public static class GatewayEventIds
{
    // Lifecycle: 1000–1099
    public const int GatewayStarted = 1000;
    public const int GatewayStopped = 1001;
    public const int GatewayFatal = 1002;
    public const int LifecycleCleanupFailed = 1003;

    // Connection & transport: 1100–1199
    public const int TransportFailed = 1100;

    // TCP commands: 1200–1299
    public const int CommandFailed = 1200;
    public const int SessionRevocationFailed = 1201;

    // External dependencies: 1300–1399
    public const int DependencyOperationFailed = 1300;
    public const int DependencyUnavailable = 1301;
    public const int DependencyDataInvalid = 1302;
    public const int DependencyConnected = 1303;
    public const int DependencyDisconnected = 1304;
    public const int DependencyRestored = 1305;

    // Realtime: 1400–1499
    public const int RealtimeBusReady = 1400;
    public const int RealtimeSubscriptionFailed = 1401;
    public const int RealtimeDeliveryFailed = 1402;
    public const int RealtimeNakFailed = 1403;
    public const int RealtimeEventRejected = 1404;
    public const int RealtimeEventUnsupported = 1405;
    public const int PushDeliveryDispatched = 1406;
    public const int PushDeliveryFailed = 1407;

    // Ephemeral events: 1500–1599
    public const int EphemeralDisabled = 1500;

    // Stubs (主线四 placeholder backends): 1600–1699
    public const int AttachmentBackendUnavailable = 1600;
    public const int RelationshipMutateBackendUnavailable = 1601;
    public const int RelationshipListBackendUnavailable = 1602;
}
