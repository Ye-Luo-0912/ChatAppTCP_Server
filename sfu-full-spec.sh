#!/bin/bash
# SFU 全规格验收：30 房间 × (10 音频发布者 + 10 订阅者) × 30 分钟。
cd ~/sfu-validation
API_KEY=devkey
API_SECRET=smoke-secret-0123456789-abcdef-ghijkl
URL=http://127.0.0.1:7880
echo "=== full-spec 30rooms x 30min start $(date -Is)" >> full-spec-summary.log
pids=()
for i in $(seq 1 30); do
  ./lk load-test --room "spec-room-$i" --duration 30m --audio-publishers 10 --subscribers 10 \
    --api-key "$API_KEY" --api-secret "$API_SECRET" --url "$URL" --yes \
    > "spec-room-$i.log" 2>&1 &
  pids+=($!)
done
( while true; do ps -o %cpu=,rss= --no-headers -p "$(pgrep -f 'livekit-server --config')" 2>/dev/null >> spec-cpu-mem.log; sleep 30; done ) &
sampler=$!
for p in "${pids[@]}"; do wait "$p"; done
kill "$sampler" 2>/dev/null
echo "=== full-spec end $(date -Is)" >> full-spec-summary.log
