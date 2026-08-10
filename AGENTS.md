# AGENTS.md — ChatApp TCP Gateway

Guidance for humans and coding agents working in this repository.

接手时先读相关实现、调用方、上下游契约和测试，确认命令语义、兼容窗口与资源所有权后再改。优先级是正确/安全、可维护、可测量的性能、真实复用；只共享稳定、线程安全且生命周期匹配的资源，禁止共享 `DbContext`、事务、流和连接级可变会话。验证按聚焦单测/契约测试 → Release 构建 → 短时 smoke 推进，阶段长测和发布 soak 只用于功能冻结后的候选版本。当前路线见 `docs/NEXT-STAGE.md`。

## Architecture boundaries

| Layer | Path | May depend on | Must not |
|-------|------|---------------|----------|
| Core | `Core/` | BCL + versioned wire/Realtime contract packages | Logging, Redis, Hosting, Gateway, Infrastructure, Observability |
| Infrastructure | `Infrastructure/` | Core, Observability, Redis, DI (incl. Hosting.Abstractions, Options), Realtime Integration | Gateway / TcpGatewayService / sessions |
| Observability | `Observability/` | BCL + logging/metrics abstractions | Gateway / Infrastructure, Business protocol handlers |
| Gateway | `Gateway/` | Core, Infrastructure, Observability, Realtime Integration | Direct DB / Outbox |
| Host | `Program.cs` | All of the above | Business logic |

Dependency direction: **Gateway → Infrastructure → Core**, with **Observability** as a leaf dependency shared by Infrastructure and Gateway. Cross-process messaging uses the versioned `ChatApp.Realtime.Contracts` / `ChatApp.Realtime.Integration` packages. Core may expose BCL-only types from the contracts packages, but must not reference Realtime implementation or transport packages.

## Protocol invariants

1. Fixed 10-byte packet header; payload codec is pluggable (`IPayloadCodec<T>`). Current wire format is camelCase JSON via source-generated `GatewayJsonSerializerContext`.
2. `ClientHello.featureBits` is opt-in compatible: command-level feature enforcement applies only when
   `CommandCapabilities` is negotiated. Keep `CommandCatalog.RequiredFeature` and
   `GatewayFeatureSet.Implemented` synchronized; see `docs/protocol-capabilities.md`.
3. Connection state machine (strict serial on the read loop **Inline** lane):
   - `ClientHello` → `ServerHello` (or Resume success → authenticated)
   - then `AuthenticationRequest` (unless Resume already authenticated)
   - business commands only after `IsAuthenticated`
4. When `RequireClientHello=true` (default), `AuthenticationRequest` before handshake is a fatal protocol violation.
5. `ClientHello`, `AuthenticationRequest`, `Heartbeat`, `PresenceUnwatch` are **Inline** — never OrderedWrite — so multi-frame TCP segments cannot reorder handshake vs auth.
6. Soft response budget: `PacketProtocol.WireResponseSoftLimit` (64 KiB); hard: `MaxPayloadSize` (80 KiB).

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

## Performance runs

按 `tools/ChatApp.Performance.Orchestrator/README.md` 执行：每轮使用独立 run 目录，记录 invocation manifest、源码/工具 hash 与报告路径；开发阶段优先短测，长测只用于冻结后的候选版本，禁止用 `git pull` 改写已声明的测试快照。
