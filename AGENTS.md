# AGENTS.md — ChatApp TCP Gateway

Guidance for humans and coding agents working in this repository.

## Architecture boundaries

| Layer | Path | May depend on | Must not |
|-------|------|---------------|----------|
| Core | `Core/` | BCL only | Logging, Redis, Hosting, Gateway |
| Infrastructure | `Infrastructure/` | Core, Redis, DI | TcpGatewayService / sessions |
| Observability | `Observability/` | BCL + logging/metrics abstractions | Business protocol handlers |
| Gateway | `Gateway/` | Core, Infrastructure, Observability, Realtime Integration | Direct DB / Outbox |
| Host | `Program.cs` | All of the above | Business logic |

Dependency direction: **Gateway → Infrastructure → Core**. Cross-process messaging uses the sibling repo `../ChatApp.RealtimeServices` (`ChatApp.Realtime.Integration` / `Abstractions`). Cloning only this repo is not enough to build.

Long-term: publish shared Realtime contracts as a versioned package, or merge into a single repo, so agents do not need a sibling checkout.

## Protocol invariants

1. Fixed 10-byte packet header; payload codec is pluggable (`IPayloadCodec<T>`). Current wire format is camelCase JSON via source-generated `GatewayJsonSerializerContext`.
2. Connection state machine (strict serial on the read loop **Inline** lane):
   - `ClientHello` → `ServerHello` (or Resume success → authenticated)
   - then `AuthenticationRequest` (unless Resume already authenticated)
   - business commands only after `IsAuthenticated`
3. When `RequireClientHello=true` (default), `AuthenticationRequest` before handshake is a fatal protocol violation.
4. `ClientHello`, `AuthenticationRequest`, `Heartbeat`, `PresenceUnwatch` are **Inline** — never OrderedWrite — so multi-frame TCP segments cannot reorder handshake vs auth.
5. Soft response budget: `PacketProtocol.WireResponseSoftLimit` (64 KiB); hard: `MaxPayloadSize` (80 KiB).

## Build / test / publish

```powershell
dotnet restore ChatApp.TcpGateway.sln
dotnet build ChatApp.TcpGateway.sln -c Release
dotnet test tests/ChatApp.TcpGateway.Tests/ChatApp.TcpGateway.Tests.csproj -c Release --no-build
```

### Native AOT vs JIT

**Default: JIT + TieredPGO** (`PublishAot=false`). StackExchange.Redis and related deps still emit trim/AOT warnings under `TreatWarningsAsErrors`. For a long-lived gateway, peak throughput is usually not AOT-bound; prefer measured load tests before re-enabling AOT.

- All protocol/store JSON must use `GatewayJsonSerializerContext` / `JsonTypeInfo` (no reflection `JsonSerializerOptions` for wire or Redis values).
- To experiment with AOT: set `<PublishAot>true</PublishAot>`, fix Redis/trim warnings, then `dotnet publish -c Release`.

### Performance tools

See `tools/ChatApp.Performance.Orchestrator/README.md`, `docs/performance-baseline.md`, and `docs/optimization-roadmap.md`.

## Known structural debt

- `Gateway/Networking/TcpGatewayService.cs` and `Gateway/Messaging/RealtimeEventDispatcher.cs` are oversized. Prefer extracting command handlers / presence / push into focused types rather than growing these files further.
- Global inbound budget is enforced by `GlobalInboundBudget` + `SessionInboundPipeLease`; do not add unbounded Pipe or ArrayPool copies without reserving.

## Scratch / local-only

Do not commit:

- `_debug*.ps1`, `_check_le.ps1`, `_copy_bus.ps1` (hard-coded absolute paths / overwrite sibling files)
- `.trae/` local skill/patch scripts
- `scratch/`

Use `scratch/` for temporary scripts (gitignored).
