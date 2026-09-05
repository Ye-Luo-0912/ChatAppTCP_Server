#!/usr/bin/env bash
# 手动测试 dotnet-gcdump 是否能 attach 到任意 dotnet 进程
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
echo "=== find a dotnet process ==="
pgrep -af 'dotnet' | head -5
pid=$(pgrep -f 'dotnet' | head -1)
echo "target pid=$pid"
if [ -n "$pid" ]; then
  echo "=== attempt collect (10s timeout) ==="
  timeout 20 dotnet-gcdump collect -p "$pid" -o /home/yeluo/chatapp-perf/test-$pid.gcdump 2>&1
  echo "exit=$?"
  ls -la /home/yeluo/chatapp-perf/test-$pid.gcdump 2>&1
fi
echo "=== tool version ==="
dotnet-gcdump --version 2>&1 | head -3