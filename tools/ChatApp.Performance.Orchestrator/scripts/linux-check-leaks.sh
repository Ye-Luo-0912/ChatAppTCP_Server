#!/usr/bin/env bash
# 检查残留容器与端口占用
echo "=== docker ps -a (chatapp-capacity) ==="
docker ps -a --format '{{.Names}}\t{{.Status}}' 2>&1 | grep -i chatapp || echo "no chatapp containers"
echo "=== port 4222/18222/15432/16379/18888/18080 listeners ==="
ss -ltnp 2>/dev/null | grep -E ':(4222|18222|15432|16379|18888|18080)\b' || echo "all free"
echo "=== pid of gateway/realtime ==="
pgrep -af 'ChatApp.TcpGateway|RealtimeServices' 2>&1 | head
echo "=== running Run-MemoryProfile? ==="
pgrep -af 'Run-MemoryProfile' 2>&1 | head