# TCP Gateway load generator

The generator has four modes. All clients start concurrently, so connection mode
also acts as a connection-storm test.

Connection-only run:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode connection --connections 1000 --duration-seconds 5

Authenticated heartbeat RTT/throughput run:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode heartbeat --connections 100 --duration-seconds 30 --token "<access-token>"

Chat self-delivery and multi-device fan-out run. Reusing one token authenticates
all connections as devices of the same user. The final connections selected by
`--slow-readers` deliberately stop reading and use a small socket receive buffer:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode chat --connections 100 --duration-seconds 30 --token "<access-token>" --messages-per-second 10 --payload-bytes 512 --slow-readers 5

Use repeated `--token` options to distribute connections across multiple users. For
automation, prefer `--token-file PATH`, with one token per line, so credentials do not
appear in the process command line. The file is read once at startup and is never
serialized into reports.
Without `--target-user-id`, each sender targets its authenticated user. Set a fixed
target to test fan-out toward a known user:

    --token "<user-a-token>" --token "<user-b-token>" --target-user-id 42

Invalid packet rejection run:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode invalid-packet --connections 100 --duration-seconds 10

The tool reports connection success, send/delivery throughput, and p50/p95/p99
latency. Add `--report-directory .artifacts/performance` to persist AOT-safe JSON and
Markdown reports; access-token values are never serialized. Authenticated modes require
valid tokens from the configured Redis/Garnet instance. For soak tests, increase
`--duration-seconds` to 1800 or longer and collect gateway process CPU, working set,
GC allocation, queue-depth, and queue-byte metrics at the same time. The multi-process
orchestrator performs this sampling and combines this report with the persistent pipeline.

Chat mode requires the complete pipeline: NATS JetStream, PostgreSQL,
Redis/Garnet, ChatApp.RealtimeServices, and the TCP gateway. `sent` measures TCP
upstream requests, `MQ-accepted` measures successful JetStream publish ACKs, and
`deliveries received` measures events that completed persistence and Outbox
publication before returning through the gateway.
