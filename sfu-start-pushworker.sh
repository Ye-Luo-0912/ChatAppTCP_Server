#!/bin/bash
# 启动 PushWorker（relgate，TestNoop 灰度模式）
cd ~/relgate/pushworker
pkill -f "ChatApp.PushWorker.dll" 2>/dev/null
sleep 1
export ASPNETCORE_ENVIRONMENT=Production
nohup dotnet ChatApp.PushWorker.dll > ~/relgate/pushworker.log 2>&1 &
echo "pushworker pid: $!"
sleep 5
grep -cE "PushDeliveryConsumer|PushWorker.*started|Now listening" ~/relgate/pushworker.log 2>/dev/null
