using ChatApp.TcpGateway.Core.Messaging;

namespace ChatApp.TcpGateway.Gateway.Messaging;

public static class AttachmentWireMapper
{
    public static IReadOnlyList<AttachmentRef>? Map(
        IReadOnlyList<ChatApp.Realtime.Abstractions.Messaging.AttachmentRef>? source)
    {
        if (source is null || source.Count == 0)
            return null;

        return source;
    }
}
