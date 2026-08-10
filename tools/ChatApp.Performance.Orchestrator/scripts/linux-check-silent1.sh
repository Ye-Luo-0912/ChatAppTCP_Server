#!/usr/bin/env bash
# 查看正式测量最新目录的 silent-1 结果
set -u
latest=$(ls -1dt /home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-* | head -1)
echo "LATEST=$latest"
echo "=== silent-1 evidence ==="
ls -la "$latest/silent-1/evidence/" 2>&1
echo "=== memory-profile-report (partial) ==="
R="$latest/memory-profile-report.json"
[ -f "$R" ] && grep -oE '"(OverallSucceeded|Profile|RunValid|CapacityExitCode|Gcdumps)": [^,}]*' "$R" | head -20 || echo "report not yet written"
echo "=== benchmark-report silent-1 (ProcessResources/gateway) ==="
BR=$(find "$latest/silent-1" -name 'benchmark-report.json' | head -1)
echo "BR=$BR"
[ -n "$BR" ] && grep -oE '"(Label|MaximumPssBytes|MaximumVmRssBytes|MaximumVmHwmBytes|MaximumCgroupMemoryPeakBytes|MaximumFileDescriptorCount)": [^,}]*' "$BR" | head -30 || echo "no benchmark report"