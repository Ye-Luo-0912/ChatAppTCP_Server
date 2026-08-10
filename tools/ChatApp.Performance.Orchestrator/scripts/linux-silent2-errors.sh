#!/usr/bin/env bash
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170933Z
echo "=== silent-2 benchmark errors detail ==="
BR=$(find "$D/silent-2" -name 'benchmark-report.json' | head -1)
[ -n "$BR" ] && python3 -c "import json,sys; d=json.load(open('$BR')); print(json.dumps(d.get('Errors'),ensure_ascii=False,indent=2)); print('Result:',d.get('Result')); [print(r) for r in d.get('Results',[])]" 2>&1 | head -40
echo "=== load generator logs ==="
find "$D/silent-2" -name '*load*' -o -name '*.log' 2>/dev/null | head
echo "=== any stderr/log files in curve dir ==="
find "$D/silent-2" -type f 2>/dev/null | head -30