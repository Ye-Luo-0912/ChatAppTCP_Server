#!/usr/bin/env bash
# 短测量复现 gcdump 255：silent 单轮 90s，让 collector 在测量中段 attach 真实 Gateway
set -e
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
ulimit -Sn 65535
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator/scripts
nohup pwsh -NoProfile -File ./Run-MemoryProfile.ps1 -SkipBuild -Profiles silent -Repeats 1 -DurationSeconds 90 -TcpConnections 500 > /home/yeluo/chatapp-perf/repro-gcdump.log 2>&1 &
echo "PID=$!"
sleep 3
pgrep -af 'Run-MemoryProfile' | head -2