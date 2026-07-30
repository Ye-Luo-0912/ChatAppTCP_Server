namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Identifies the specific operation performed against a gateway dependency.
/// Used as a structured log field and metrics tag.
/// </summary>
public enum GatewayDependencyOperation : byte
{
    AccessTokenLookup = 1,
    DeviceLeaseTakeOver = 2,
    DeviceLeaseRelease = 3,
    DeviceLeaseRefresh = 4,
    PresenceSetOnline = 5,
    PresenceSetOffline = 6,
    PresenceRefresh = 7,
    PresenceQuery = 8,
    PresenceAuthorize = 9,
    SessionRevocationPublish = 10,
    EphemeralTypingPublish = 11,
    EphemeralPresencePublish = 12,
    ResumeTokenLookup = 13,
    PushTokenRegister = 14,
    PushTokenUnregister = 15,
    PushTokenList = 16,
    GatewayDirectoryQuery = 17,
    WatcherDirectoryQuery = 18,
    DeviceLeaseQuery = 19,
    ResumeTokenRevoke = 20,
    ResumeWatermarkQuery = 21,
    GroupIdempotencyLookup = 22,
    GroupIdempotencyStore = 23
}
