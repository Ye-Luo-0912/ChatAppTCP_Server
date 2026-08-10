#!/usr/bin/env bash
# 重新构建 orchestrator（确保 Linux 二进制与本地源码一致），然后运行冒烟测试
set -e
export PATH=/usr/bin:/bin:/usr/local/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:/home/yeluo/.local/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet

ulimit -Sn 65535
echo "ulimit -Sn = $(ulimit -Sn)"

cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator
echo "=== BUILD ORCHESTRATOR ==="
dotnet build -c Release 2>&1 | tail -8
echo "=== BUILD EXIT=${PIPESTATUS[0]} ==="

cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator/scripts
echo "=== RUN SMOKE ==="
pwsh -NoProfile -File ./Run-MemoryProfile.ps1 -Smoke
echo "=== SMOKE EXIT=$? ==="