#!/usr/bin/env bash
# 清理残留测量进程与容器
pkill -f 'Run-MemoryProfile.ps1' 2>/dev/null || true
pkill -f 'CapacityCurve.ps1' 2>/dev/null || true
pkill -f 'ChatApp.TcpGateway.dll' 2>/dev/null || true
pkill -f 'RealtimeServices' 2>/dev/null || true
pkill -f 'tcp-load' 2>/dev/null || true
sleep 2
echo "=== processes (should be empty) ==="
pgrep -af 'Run-MemoryProfile|CapacityCurve|ChatApp.TcpGateway|RealtimeServices|tcp-load' | head || echo none
echo "=== stop+rm chatapp-capacity containers ==="
for c in $(docker ps -aq --filter name=codex-chatapp-capacity); do
  echo "removing $c"
  docker rm -f "$c" 2>&1
done
echo "=== ports ==="
ss -ltn 2>/dev/null | grep -E ':(4222|18222|15432|16379|18888|18080)\b' || echo "all free"
echo "cleanup done"