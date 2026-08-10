#!/usr/bin/env bash
# 汇总所有 memory-profile 报告的 OverallSucceeded 与各画像 CapacityExitCode
set -u
PERF=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance
cd "$PERF" || exit 1
for d in memory-profile-*; do
  [ -d "$d" ] || continue
  r="$d/memory-profile-report.json"
  [ -f "$r" ] || { echo "=== $d : NO REPORT ==="; continue; }
  echo "=== $d ==="
  grep -oE '"OverallSucceeded":[^,}]*' "$r" | head -1
  grep -oE '"Profile": *"[^"]*"' "$r" | head -3
  grep -oE '"CapacityExitCode":[0-9-]*' "$r" | sort | uniq -c
done