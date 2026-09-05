#!/usr/bin/env bash
# 查看最新 memory-profile 报告摘要
set -u
PERF=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance
latest=$(ls -1dt "$PERF"/memory-profile-* | head -1)
echo "LATEST=$latest"
R="$latest/memory-profile-report.json"
echo "=== OverallSucceeded ==="
grep -oE '"OverallSucceeded": [^,}]*' "$R"
echo "=== per-result RunValid / Exit / gcdumps ==="
grep -oE '"(Profile|RunValid|CapacityExitCode|Gcdumps)": [^,}]*' "$R"
echo "=== generated artifacts under latest ==="
find "$latest" -type f \( -name '*.gcdump' -o -name 'ss-tinm.txt' -o -name 'proc-net-sockstat.txt' -o -name 'benchmark-report.json' \) 2>/dev/null | head -30