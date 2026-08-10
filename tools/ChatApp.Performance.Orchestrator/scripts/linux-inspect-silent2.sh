#!/usr/bin/env bash
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170933Z
echo "=== silent-2 benchmark report errors ==="
BR=$(find "$D/silent-2" -name 'benchmark-report.json' | head -1)
[ -n "$BR" ] && grep -oE '"(Errors|Result|RunValid|Passed|Coverage|Rate)": *[^,}]]*' "$BR" | head -20
echo "=== silent-2 capacity report ==="
CR=$(find "$D/silent-2" -name 'capacity-curve-report.json' | head -1)
[ -n "$CR" ] && grep -oE '"(RunValid|CapacityExitCode|Error|Errors)": *[^,}]*' "$CR" | head -20
echo "=== silent-2 invocation ==="
INV=$(find "$D/silent-2" -maxdepth 1 -name 'invocation-*.json' | head -1)
[ -n "$INV" ] && grep -oE '"(ExitCode|RunValid|Errors): *[^,}]*' "$INV" | head -10
echo "=== overall log around silent-2 fail ==="
grep -nE 'silent-2|FAILED|exited with code|coverage|Sample' /home/yeluo/chatapp-perf/formal-run.log | head -30