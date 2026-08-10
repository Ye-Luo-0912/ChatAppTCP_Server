#!/usr/bin/env bash
# Linux 端 smoke 执行：验证 in-flight TTL + 聚合修复。远端默认 shell 为 fish，故用 bash 显式执行。
set -euo pipefail
export PATH=/usr/bin:/bin:/usr/local/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:/home/yeluo/.local/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet
ulimit -Sn 65535
echo "ulimit -Sn = $(ulimit -Sn)"
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator/scripts
echo "=== SMOKE RUN ==="
pwsh -NoProfile -File ./Run-MemoryProfile.ps1 -Smoke 2>&1 | tail -60
echo "=== SMOKE_EXIT=${PIPESTATUS[0]} ==="
exit ${PIPESTATUS[0]}