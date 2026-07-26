namespace ChatApp.TcpGateway.Core.Messaging.Push;

/// <summary>
/// 推送平台标识。FCM=Firebase Cloud Messaging（Android/浏览器）；
/// Apns=Apple Push Notification service（iOS/macOS）。
/// </summary>
public enum PushPlatform : byte
{
    Fcm = 1,
    Apns = 2
}
