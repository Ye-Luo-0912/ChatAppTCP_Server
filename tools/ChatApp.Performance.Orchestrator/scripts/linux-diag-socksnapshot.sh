#!/usr/bin/env bash
set -euo pipefail
echo '--- /proc/net/sockstat readability ---'
ls -la /proc/net/sockstat /proc/net/sockstat6 2>&1
echo '--- head sockstat ---'
head -3 /proc/net/sockstat 2>&1
echo '--- Test-Path Leaf ---'
pwsh -NoProfile -Command "Write-Output ('Leaf=' + (Test-Path -LiteralPath '/proc/net/sockstat' -PathType Leaf)); Write-Output ('Any=' + (Test-Path '/proc/net/sockstat'))"
echo '--- report CollectorError + SocketSnapshot ---'
python3 - /home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-202729Z/memory-profile-report.json <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1]))
for r in d.get("Results", []):
    print(r.get("Profile"), "/", r.get("Repeat"),
          "CollectorError=", repr(r.get("CollectorError")),
          "SocketSnapshot=", repr(r.get("SocketSnapshot")))
PYEOF