namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Stable reasons for rejecting a realtime event during dispatch.
/// Replaces arbitrary string reasons that previously leaked into metrics tags.
/// The event type already conveys which payload kind failed, so the reason
/// does not re-encode the payload type.
/// </summary>
public enum RealtimeRejectReason : byte
{
    MissingPayload = 1,
    InvalidJson = 2,
    InvalidPayload = 3,
    TargetMismatch = 4,
    MissingSessionId = 5
}
