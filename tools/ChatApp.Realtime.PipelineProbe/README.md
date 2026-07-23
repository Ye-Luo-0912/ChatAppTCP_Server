# Realtime pipeline probe

This tool verifies the real cross-process message and read-receipt pipeline without
requiring a TCP access token:

    Integration client -> INCOMING_MESSAGES -> RealtimeServices ->
    PostgreSQL message/outbox transaction -> REALTIME_EVENTS ->
    read receipt -> PostgreSQL status/outbox transaction -> REALTIME_EVENTS ->
    Core NATS history query -> PostgreSQL keyset page -> reply

Start NATS JetStream, PostgreSQL, Garnet, and ChatApp.RealtimeServices, then run:

    dotnet run --project tools/ChatApp.Realtime.PipelineProbe -c Release

An optional first argument overrides the NATS URL. The tool acknowledges every event
it observes and exits only after its unique message and read receipt both complete
persistence and Outbox publication, then verifies that history returns the same
message with delivered/read timestamps.