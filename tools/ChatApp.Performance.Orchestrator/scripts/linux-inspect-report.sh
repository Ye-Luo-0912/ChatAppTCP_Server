#!/usr/bin/env bash
# 检查指定 memory-profile 报告的失败原因：exit code、错误、benchmark 报告路径
set -u
D="$1"
R="$D/memory-profile-report.json"
echo "=== $D ==="
echo "--- CapacityExitCode rows ---"
grep -oE '"Profile": *"[^"]*"[^}]*"CapacityExitCode": [^,]*' "$R"
echo "--- overall ---"
grep -oE '"OverallSucceeded": [^,]*' "$R"
echo "--- benchmark-report files ---"
find "$D" -name 'benchmark-report.json' 2>/dev/null
echo "--- invocation files ---"
find "$D" -name 'invocation-*.json' 2>/dev/null
echo "--- any RunnerError / error fields ---"
grep -oE '"(RunnerError|InvocationError|CapacityError|ErrorMessage|Error)": *[^,}]*' "$R" | head -20