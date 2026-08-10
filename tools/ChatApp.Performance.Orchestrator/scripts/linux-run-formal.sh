#!/usr/bin/env bash
# 启动 TCP-MEM-1 正式测量（3 画像 x 3 轮 x 10 分钟），后台运行
set -e
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
ulimit -Sn 65535
ulimit -Hn 65535
echo "ulimit soft=$(ulimit -Sn) hard=$(ulimit -Hn)"
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/tools/ChatApp.Performance.Orchestrator/scripts
nohup pwsh -NoProfile -File ./Run-MemoryProfile.ps1 -SkipBuild -Repeats 3 -DurationSeconds 600 -TcpConnections 10000 > /home/yeluo/chatapp-perf/formal-run.log 2>&1 &
echo "PID=$!"
sleep 3
echo "--- running? ---"
pgrep -af 'Run-MemoryProfile' | head -3
echo "--- log head ---"
tail -5 /home/yeluo/chatapp-perf/formal-run.log