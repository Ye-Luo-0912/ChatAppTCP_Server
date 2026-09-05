#!/usr/bin/env bash
# 在 Linux 端重建 orchestrator（Release），失败即退出非零。远端默认 shell 为 fish，故用 bash 显式执行。
set -euo pipefail
export PATH=/usr/bin:/bin:/usr/local/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:/home/yeluo/.local/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet
ulimit -Sn 65535
echo "ulimit -Sn = $(ulimit -Sn)"
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server
echo "=== global.json ==="
cat global.json 2>/dev/null || echo "(no root global.json)"
cd tools/ChatApp.Performance.Orchestrator
echo "=== BUILD ==="
dotnet build -c Release 2>&1 | tail -15
echo "=== BUILD_EXIT=${PIPESTATUS[0]} ==="
exit ${PIPESTATUS[0]}