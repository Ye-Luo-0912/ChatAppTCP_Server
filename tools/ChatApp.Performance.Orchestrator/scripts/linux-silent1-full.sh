#!/usr/bin/env bash
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170933Z
L1=$(find "$D/silent-1" -name 'tcp-load-1.stderr.log' | head -1)
echo "=== silent-1 tcp-load-1.stderr.log ==="
cat "$L1" 2>&1
echo "=== silent-1 tcp-load-1.stdout.log (full) ==="
O1=$(find "$D/silent-1" -name 'tcp-load-1.stdout.log' | head -1)
cat "$O1" 2>&1
echo "=== silent-1 gateway-1/stdout + stderr ==="
G1O=$(find "$D/silent-1" -name 'gateway-1.stdout.log' | head -1)
G1E=$(find "$D/silent-1" -name 'gateway-1.stderr.log' | head -1)
echo "--- gateway-1 stdout ---"; cat "$G1O" 2>&1 | tail -30
echo "--- gateway-1 stderr ---"; cat "$G1E" 2>&1 | tail -30