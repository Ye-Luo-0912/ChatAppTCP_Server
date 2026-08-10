#!/usr/bin/env bash
L=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170933Z/silent-2/capacity-curve-20260810-171521Z/rate-1/benchmark-20260810-171527Z/logs
echo "=== tcp-load-1.stderr.log ==="
tail -40 "$L/tcp-load-1.stderr.log" 2>&1
echo "=== tcp-load-2.stderr.log ==="
tail -40 "$L/tcp-load-2.stderr.log" 2>&1
echo "=== tcp-load-1.stdout.log tail ==="
tail -20 "$L/tcp-load-1.stdout.log" 2>&1
echo "=== gateway-1.stderr.log tail ==="
tail -15 "$L/gateway-1.stderr.log" 2>&1