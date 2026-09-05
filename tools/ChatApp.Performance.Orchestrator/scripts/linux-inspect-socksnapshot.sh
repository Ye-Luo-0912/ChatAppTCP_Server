#!/usr/bin/env bash
# 检查 report 中 SocketSnapshot 原始值与证据文件。远端默认 shell 为 fish，故用 bash。
set -euo pipefail
REPORT="$1"
python3 - "$REPORT" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1]))
for r in d.get("Results", []):
    ss = r.get("SocketSnapshot")
    print(f"{r.get('Profile')}/{r.get('Repeat')}: SocketSnapshot = {ss!r}")
PYEOF
BASE="$(dirname "$1")"
echo "=== evidence files per profile ==="
for d in "$BASE"/*-1/evidence; do
  echo "-- $d"
  ls -la "$d" 2>&1 | grep -E 'gcdump|ss-|sockstat' || echo "   (none)"
done