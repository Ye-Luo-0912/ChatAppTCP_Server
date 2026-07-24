namespace ChatApp.TcpGateway.Core.Messaging.Sync;

/// <summary>
/// Why a client sync cursor cannot be used for incremental catch-up and requires full recovery.
/// </summary>
public enum SyncCursorResetReason : byte
{
    MessageNotFound = 1,
    AheadOfTip = 2,
    MembershipLost = 3,
    GapTooLarge = 4,

    /// <summary>Cursor is older than the realtime service's configured retention horizon.</summary>
    BeyondRetention = 5
}
