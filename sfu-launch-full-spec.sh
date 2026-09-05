#!/bin/bash
# 启动 SFU 全规格验收（在 relgate 上执行）
pkill -f "lk load-test" 2>/dev/null
sleep 1
mkdir -p ~/sfu-validation
cp /tmp/sfu-full-spec.sh ~/sfu-validation/full-spec.sh
chmod +x ~/sfu-validation/full-spec.sh
cd ~/sfu-validation
pgrep -f "livekit-server --config" > /dev/null || (nohup ./livekit-server --config livekit.yaml > livekit.log 2>&1 &)
sleep 3
rm -f full-spec-summary.log
(nohup setsid ./full-spec.sh > /dev/null 2>&1 &)
sleep 8
echo "bots=$(pgrep -cf 'lk load-test')"
tail -1 full-spec-summary.log
