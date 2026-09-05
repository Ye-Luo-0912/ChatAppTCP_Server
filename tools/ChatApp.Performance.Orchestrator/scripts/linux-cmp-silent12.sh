#!/usr/bin/env bash
D=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.artifacts/performance/memory-profile-20260810-170933Z
echo "=== silent-1 capacity report RunValid/Exit ==="
CR1=$(find "$D/silent-1" -name 'capacity-curve-report.json' | head -1)
python3 -c "import json; d=json.load(open('$CR1')); print('RunValid:',d.get('RunValid')); print('ExitCode:',d.get('ExitCode')); print('Errors:',d.get('Errors')); print('Results:',json.dumps(d.get('Results'),ensure_ascii=False)[:500])" 2>&1
echo "=== silent-2 capacity report RunValid/Exit ==="
CR2=$(find "$D/silent-2" -name 'capacity-curve-report.json' | head -1)
python3 -c "import json; d=json.load(open('$CR2')); print('RunValid:',d.get('RunValid')); print('ExitCode:',d.get('ExitCode')); print('Errors:',d.get('Errors')); print('Results:',json.dumps(d.get('Results'),ensure_ascii=False)[:500])" 2>&1
echo "=== silent-1 benchmark report Result & Errors ==="
BR1=$(find "$D/silent-1" -name 'benchmark-report.json' | head -1)
python3 -c "import json; d=json.load(open('$BR1')); print('Result:',d.get('Result')); print('Errors:',json.dumps(d.get('Errors'),ensure_ascii=False)); print('RunValid:',d.get('RunValid'))" 2>&1