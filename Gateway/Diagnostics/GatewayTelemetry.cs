using System.Diagnostics;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Diagnostics;

public static class GatewayTelemetry
{
    public const string ActivitySourceName = "ChatApp.TcpGateway";

    private static readonly ActivitySource Source = new(ActivitySourceName, "1.0.0");

    public static Activity? StartCommand(PacketCommand command)
    {
        if (command == PacketCommand.Heartbeat)
            return null;

        var activity = Source.StartActivity(
            "tcp.command",
            ActivityKind.Server);
        activity?.SetTag("network.protocol.name", "tcp");
        activity?.SetTag("chat.command", command.ToString());
        return activity;
    }

    public static Activity? StartEventConsumer(ActivityContext parentContext)
    {
        var activity = parentContext.TraceId == default
            ? Source.StartActivity("realtime.event.consume", ActivityKind.Consumer)
            : Source.StartActivity(
                "realtime.event.consume",
                ActivityKind.Consumer,
                parentContext);
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.operation.name", "process");
        return activity;
    }

    public static void RecordException(Activity? activity, Exception exception)
    {
        activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity?.SetTag("error.type", exception.GetType().FullName);
    }
}
