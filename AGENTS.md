# AGENTS.md — ChatApp TCP Gateway

Guidance for humans and coding agents working in this repository.

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

## Linux test environment

长期性能测试（soak / capacity curve）在专用 Linux 机器上执行。

- **SSH**: `ssh chatapp-linux`（已在本机 `~/.ssh/config` 配置别名，密钥认证免密）
- **IP**: 192.168.5.49（内网）
- **用户**: yeluo
- **仓库路径**:
  - `/home/yeluo/chatapp-perf/ChatAppTCP_Server`（本仓库）
  - `/home/yeluo/chatapp-perf/ChatApp.RealtimeServices`（同级 RealtimeServices 仓库）
- **环境**: .NET SDK 11.0 preview + PowerShell 7.4.7 (`/home/yeluo/.local/bin/pwsh`) + Docker 29.6.2
- **Shell**: 默认 fish，远程脚本须用 `bash -c '...'` 或 `pwsh -c '...'` 执行
- **注意**: `global.json` 要求 SDK 10.0.301（`allowPrerelease: false`），连接后先确认 10.x SDK 可用；若仅有 11.0 preview 需先安装 10.0.301

执行长时间 soak 测试的标准流程：
1. SSH 连接后 `cd /home/yeluo/chatapp-perf/ChatAppTCP_Server`
2. `git pull` 同步最新代码（含 Runtime V2 改动）
3. `dotnet build ChatApp.TcpGateway.sln -c Release`
4. 运行 `pwsh tools/ChatApp.Performance.Orchestrator/scripts/Run-Soak.ps1` 并指定 `-OutboundSendMode`（两种模式各跑一次对比）
