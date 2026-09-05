#!/usr/bin/env bash
# TCP-MEM-1 正式测量前预检
set -u
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
echo "=== dotnet SDK (global.json 锁 10.0.301) ==="
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server
cat global.json 2>&1
dotnet --list-sdks 2>&1 | grep -E '^10\.'
echo "=== ulimit -Sn ==="
ulimit -Sn
echo "=== docker images ==="
docker images --format '{{.Repository}}:{{.Tag}}' 2>&1 | grep -E 'nats|postgres|garnet' || echo "MISSING IMAGES"
echo "=== ports in use ==="
ss -ltn 2>/dev/null | grep -E ':(18888|18080|4222|18222|15432|16379)\b' || echo "ALL PORTS FREE"
echo "=== Realtime sibling repo ==="
[ -d /home/yeluo/chatapp-perf/ChatApp.RealtimeServices ] && echo "Realtime present: yes" || echo "Realtime present: NO"
echo "=== gcdump ==="
which dotnet-gcdump 2>&1
echo "=== preflight done ==="