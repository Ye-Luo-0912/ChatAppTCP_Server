namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Identifies which realtime subscription kind failed.
/// Typing and Presence ephemeral subscriptions share a single subscription-failed
/// log template, distinguished by this value.
/// </summary>
public enum RealtimeSubscriptionKind : byte
{
    DurableEvents = 1,
    EphemeralTyping = 2,
    EphemeralPresence = 3,
    PushDeliveries = 4
}
