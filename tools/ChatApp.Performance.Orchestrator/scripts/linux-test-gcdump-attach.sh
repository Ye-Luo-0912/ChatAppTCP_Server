#!/usr/bin/env bash
# 启动一个真实 Gateway 进程，然后测试 dotnet-gcdump attach
set -e
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server
echo "=== start gateway (background) ==="
nohup dotnet bin/Release/net10.0/ChatApp.TcpGateway.dll --urls http://127.0.0.1:18888 > /home/yeluo/chatapp-perf/gw-test.log 2>&1 &
echo "GW_PID=$!"
sleep 8
pid=$(pgrep -f 'ChatApp.TcpGateway.dll' | head -1)
echo "gateway pid=$pid"
if [ -n "$pid" ]; then
  echo "=== dotnet-gcdump collect ==="
  timeout 30 dotnet-gcdump collect -p "$pid" -o /home/yeluo/chatapp-perf/gw-test.gcdump 2>&1 || true
  echo "collect exit=$?"
  ls -la /home/yeluo/chatapp-perf/gw-test.gcdump 2>&1
else
  echo "gateway did not start; log:"
  tail -20 /home/yeluo/chatapp-perf/gw-test.log
fi
echo "=== stopping gateway ==="
pkill -f 'ChatApp.TcpGateway.dll' 2>/dev/null || true
echo done