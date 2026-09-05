#!/usr/bin/env bash
# 查看 TCP-MEM-1 正式测量进度。远端默认 shell 为 fish，故用 bash。
set -euo pipefail
LOG="${1:-/home/yeluo/chatapp-perf/logs/memformal-20260811-071231.log}"
if pgrep -af 'Run-MemoryProfile.ps1' >/dev/null 2>&1; then
  echo "STATUS=RUNNING"
else
  echo "STATUS=NOT_RUNNING"
fi
echo "--- profile progress ---"
grep -aE '=== TCP-MEM-1 profile|Result: (PASSED|FAILED)' "$LOG" | tail -12
echo "--- last non-empty lines ---"
grep -a . "$LOG" | tail -3