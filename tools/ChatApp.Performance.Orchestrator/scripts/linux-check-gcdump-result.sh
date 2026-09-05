#!/usr/bin/env bash
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170620Z
echo "=== evidence dir ==="
ls -la "$D/silent-1/evidence/" 2>&1
echo "=== gcdump files ==="
find "$D" -name '*.gcdump' -exec ls -la {} \; 2>&1
echo "=== report gcdumps field ==="
grep -oE '"Gcdumps": *\[[^]]*\]' "$D/memory-profile-report.json" 2>&1