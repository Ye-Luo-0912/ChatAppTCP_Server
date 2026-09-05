#!/usr/bin/env bash
# 检查 memory-profile-report.json 聚合证据。远端默认 shell 为 fish，故用 bash。
set -euo pipefail
REPORT="$1"
python3 - "$REPORT" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1]))
print("TopKeys =", list(d.keys()))
print("OverallSucceeded =", d.get("OverallSucceeded"))
for i, r in enumerate(d.get("Results", [])):
    print(f"--- Result[{i}] keys = {list(r.keys())}")
    print(f"    Profile={r.get('Profile')} Label={r.get('Label')} RunValid={r.get('RunValid')} "
          f"Gcdumps={r.get('Gcdumps')} SocketSnapshotKeys={list(r.get('SocketSnapshot',{}).keys()) if isinstance(r.get('SocketSnapshot'),dict) else type(r.get('SocketSnapshot')).__name__}")
    gw = r.get("GatewayResources")
    print(f"    GatewayResources type={type(gw).__name__}")
    if isinstance(gw, list):
        for g in gw:
            if isinstance(g, dict):
                print(f"      gw keys={list(g.keys())}")
                for k2,v2 in g.items():
                    if k2 in ('MaximumPssBytes','MaximumVmRssBytes','MaximumVmHwmBytes','MaximumCgroupMemoryPeakBytes','MaximumFileDescriptorCount'):
                        print(f"        {k2} = {v2}")
PYEOF