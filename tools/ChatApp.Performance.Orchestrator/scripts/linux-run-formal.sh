#!/usr/bin/env bash
# TCP-MEM-1 正式测量启动器：设置 ulimit、构建 orchestrator(如需要)、后台 nohup 运行、
# 落盘日志，抗 SSH 断连。
set -o pipefail
export PATH=/usr/bin:/bin:/usr/local/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:/home/yeluo/.local/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet

ulimit -Sn 65535
echo "ulimit -Sn = $(ulimit -Sn)"

cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator
echo "=== BUILD ORCHESTRATOR ==="
dotnet build -c Release 2>&1 | tail -8
echo "=== BUILD EXIT=${PIPESTATUS[0]} ==="

cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator/scripts
STAMP=$(date +%Y%m%d-%H%M%S)
LOG="/home/yeluo/chatapp-perf/logs/memformal-$STAMP.log"
mkdir -p /home/yeluo/chatapp-perf/logs
echo "=== NOHUP RUN -> $LOG ==="
nohup pwsh -NoProfile -File ./Run-MemoryProfile.ps1 > "$LOG" 2>&1 &
echo "LAUNCHED_PID=$!"
echo "LOG=$LOG"