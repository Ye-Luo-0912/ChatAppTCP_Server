# TCP Gateway load generator

The generator has five modes. Use `--connections-per-second 0` for a deliberate
connection storm, or set a positive global connection rate for a bounded ramp.
The fixed measurement duration starts only after every client has completed its
connection/authentication ready gate and the optional stabilization phase.

Connection-only run:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode connection --connections 1000 --duration-seconds 5

Connection mode observes every socket for an early remote close or unexpected frame;
either fails the run. It is intended for bounded connection-ramp/storm survival checks,
not as an unauthenticated long-duration soak (the Gateway may legitimately enforce its
hello/authentication deadline). Use heartbeat or chat mode for authenticated soaks.

Authenticated heartbeat RTT/throughput run:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode heartbeat --connections 100 --duration-seconds 30 --token "<access-token>"

Chat peer-delivery and multi-device fan-out run. The final connections selected by
`--slow-readers` deliberately stop reading and use a small socket receive buffer.
They still send phased, write-only protocol heartbeats so the Gateway idle timeout
cannot turn a disconnected slow consumer into a false pass. Consequently,
`--inactive-heartbeat-seconds` must be greater than zero when slow readers are used:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode chat --connections 100 --duration-seconds 30 --token-file tokens.txt --active-senders 10 --messages-per-second 0.8 --payload-bytes 512 --inactive-heartbeat-seconds 30 --delivery-drain-seconds 30 --slow-readers 5

Slow readers become write-only immediately after authentication, including during
ramp and stabilization; they never consume heartbeat ACKs or business frames at the
measurement boundary.

Use repeated `--token` options to distribute connections across multiple users. For
automation, prefer `--token-file PATH`, with one token per line, so credentials do not
appear in the process command line. The file is read once at startup and is never
serialized into reports.
Without `--target-user-id`, distinct authenticated user IDs are sorted numerically
and form a deterministic peer ring (`user -> next user`, last -> first). At least
two distinct users are required. Set a fixed target to test fan-out toward a known
user; the run fails its ready gate if that target equals any sender's authenticated
user:

    --token "<user-a-token>" --token "<user-b-token>" --target-user-id 42

For controlled soak phases, use for example:

    --connections-per-second 100 --stabilization-seconds 300 --duration-seconds 28800

Ramp and stabilization time are reported separately and are never included in the
measurement throughput denominator. Any connection/authentication failure prevents
measurement from starting. Chat and heartbeat modes also apply configurable semantic
gates (`--min-ack-ratio` and `--min-delivery-ratio`). During measurement, the first
client failure, chat rejection, in-flight tracking overflow, or tracking TTL expiry
cancels the shared window immediately so an invalid soak cannot amplify failures for
hours.

Chat runs use a bounded delivery-drain phase after the fixed measurement window.
At the measurement deadline, active senders stop scheduling new messages while every
non-slow-reader connection keeps receiving. The run closes the connections when every
successfully sent message has both an accepted MQ acknowledgement and one delivery on
every expected readable recipient connection, or when `--delivery-drain-seconds`
expires (default 30 seconds). Expected recipients include every readable target-user
device plus sender-echo devices whose SessionId differs from the origin SessionId.
Runtime failures still
cancel sending and receiving immediately. Reports keep measurement and drain elapsed
time separate and record whether the drain completed. Set the option to `0` to skip
waiting; in that compatibility mode the ordinary acknowledgement/delivery ratio gates
still apply, but drain completion is not itself required by the semantic gate.

In heartbeat and chat modes, bounded heartbeat round trips begin while clients wait for
the shared ready/stabilization gate. During measurement, inactive heartbeat-mode clients
continue phased send/receive keepalives; inactive chat clients send phased keepalives and
their single chat receive loop consumes the ACKs. These keepalives are excluded from the
formal sent/ACK/delivery and latency counters. If either mode has inactive authenticated
clients, or chat mode has slow readers, `--inactive-heartbeat-seconds` must be greater
than zero. It may be set to `0` only when every authenticated client is an active sender,
there are no slow readers, and the Gateway idle timeout exceeds the complete ramp,
stabilization, measurement, and drain window.

Invalid packet rejection run:

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode invalid-packet --connections 100 --duration-seconds 10

The tool reports connection success, send/delivery throughput, separate MQ ACK and
peer-delivery p50/p95/p99 latency, and labels peer delivery as the primary chat
latency. Latencies use a fixed-memory streaming histogram. Chat message correlation
is bounded by `--max-inflight` and `--inflight-ttl-seconds`; an entry is removed only
after its unique ACK and all expected per-connection deliveries have arrived. Duplicate,
unexpected, or untracked terminal frames fail the semantic gate. Add
`--report-directory .artifacts/performance` to persist AOT-safe JSON and
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
