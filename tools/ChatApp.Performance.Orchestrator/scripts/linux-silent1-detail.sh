#!/usr/bin/env bash
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170933Z
echo "=== silent-1 tcp-load stdout tails ==="
for g in tcp-gateway-1 tcp-gateway-2; do
  f=$(find "$D/silent-1" -path "*$g*tcp-load*.json" | head -1)
  echo "--- $g: $f ---"
  [ -n "$f" ] && cat "$f" | head -40
done
echo "=== silent-1 gateway stderr ==="
GW=$(find "$D/silent-1" -name 'gateway-1.stderr.log' | head -1)
[ -n "$GW" ] && tail -20 "$GW"