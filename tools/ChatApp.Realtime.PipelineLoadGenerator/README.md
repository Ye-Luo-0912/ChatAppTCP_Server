# Realtime pipeline load generator

This tool measures the durable service pipeline independently from TCP connection
load:

    IncomingMessage -> PostgreSQL/Outbox -> MessageReceived ->
    Read receipt -> PostgreSQL/Outbox -> MessageReceiptUpdated ->
    Core NATS history request/reply

Each worker uses its own sender/receiver pair and executes one pipeline at a time,
which prevents one worker's history page from hiding another worker's latest
message. Latency storage uses a fixed-size histogram, so long soak tests do not
grow memory with the number of operations.

Short baseline:

    dotnet run --project tools/ChatApp.Realtime.PipelineLoadGenerator -c Release -- --warmup-seconds 5 --duration-seconds 30 --concurrency 8 --operations-per-second 40 --payload-bytes 512 --report-directory .artifacts/performance

Use `--operations-per-second 0` for maximum throughput. For a 30-minute baseline
set `--duration-seconds 1800`; use 28800–86400 seconds for an 8–24 hour soak test.

The JSON and Markdown reports include:

- NATS ping and completed durable pipelines per second;
- failure count and error rate;
- p50/p95/p99 for publish ACK, message persistence/Outbox, receipt
  persistence/Outbox, history query, and the complete pipeline;
- generator runtime, GC mode, allocation, and working-set context.

This generator complements `ChatApp.TcpGateway.LoadGenerator`: run the TCP tool
for connections, heartbeats, fan-out, and slow consumers, then run this tool to
isolate the durable message-service pipeline.

Run only one instance of this generator per host and NATS environment. Its event
consumer name is stable per host so repeated benchmarks reuse the same durable
consumer instead of accumulating server-side resources.
