# SDK / TFM baseline

## Chosen approach

**Align all ChatApp services on .NET 10 / SDK 10.0.301** (stability over preview).

Gateway previously targeted `net11.0` with SDK `11.0.100-preview.*`. That drift made
local restores and CI non-reproducible relative to Server and RealtimeServices.
Gateway now uses the same baseline:

| Repo | TFM | SDK pin (`global.json`) |
|------|-----|-------------------------|
| `ChatApp.Server` | `net10.0` | `10.0.301` (`rollForward: latestFeature`) |
| `ChatApp.RealtimeServices` | `net10.0` | `10.0.301` (`rollForward: latestFeature`) |
| `ChatAppTCP_Server` (Gateway) | `net10.0` | `10.0.301` (`rollForward: latestFeature`) |

Gateway Microsoft.Extensions.* package versions are pinned to **10.0.5** via
`Directory.Packages.props`, matching RealtimeServices.

## Upgrade policy

1. Keep Server, RealtimeServices, and Gateway on the **same major TFM**.
2. Prefer the latest **stable** .NET SDK patch/feature band; do not adopt preview
   SDKs in these repos unless a hard dependency requires it and is documented here.
3. When bumping (for example 10.0.x → 10.0.y, or later to .NET 11 stable):
   - Update each repo’s `global.json` together.
   - Update Gateway `Directory.Build.props` / `Directory.Packages.props`.
   - Rebuild Gateway against the sibling Realtime Integration project.
4. Preview APIs are not a reason to leave Gateway on a different major forever;
   either wait for stable or isolate the preview surface behind an optional package.

## Local builds

From each repo root, `dotnet --version` should resolve to `10.0.301` (or a newer
10.0 feature band allowed by `rollForward`). Install the matching SDK if restore
or build selects a different major.
