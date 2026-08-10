#!/usr/bin/env bash
# 抓取当前运行中的 Gateway，手动运行 dotnet-gcdump 捕获完整错误输出
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
pid=$(pgrep -f 'ChatApp.TcpGateway.dll' | head -1)
echo "gateway pid=$pid"
if [ -n "$pid" ]; then
  echo "=== ps stats ==="
  ps -o pid,rss,vsz,stat,cmd -p "$pid" 2>&1
  echo "=== dotnet-gcdump collect (full stderr) ==="
  timeout 60 dotnet-gcdump collect -p "$pid" -o /home/yeluo/chatapp-perf/manual-$pid.gcdump 2>&1
  echo "exit=$?"
  echo "=== file ==="
  ls -la /home/yeluo/chatapp-perf/manual-$pid.gcdump 2>&1
else
  echo "no gateway running"
fi