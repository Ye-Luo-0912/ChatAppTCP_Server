#!/usr/bin/env bash
set -u
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-163051Z
echo "=== silent-1 tree ==="
ls -R "$D/silent-1" | head -40
echo "=== invocation-1.json ==="
head -c 2500 "$D/silent-1/invocation-1.json"
echo
echo "=== any evidence dir? ==="
find "$D" -type d -name evidence 2>/dev/null
echo "=== any gcdump/ss files ==="
find "$D" -type f \( -name '*.gcdump' -o -name 'ss-tinm.txt' -o -name 'proc-net-sockstat.txt' \) 2>/dev/null