#!/bin/bash
# SFU 全规格容量验收：30 房间 × (10 音频发布者 + 10 订阅者)，分 10/20/30 三阶段爬坡，每阶段 10 分钟。
cd ~/sfu-validation
API_KEY=devkey
API_SECRET=smoke-secret-0123456789-abcdef-ghijkl
URL=http://127.0.0.1:7880
ACC=full-acceptance
mkdir -p "$ACC"
echo "=== full acceptance start $(date -Is)" >> "$ACC/summary.log"
for n in 10 20 30; do
  echo "=== STAGE rooms=$n begin $(date -Is)" >> "$ACC/summary.log"
  pids=()
  for i in $(seq 1 "$n"); do
    ./lk load-test --room "acc-room-$i" --duration 10m --audio-publishers 10 --subscribers 10 \
      --api-key "$API_KEY" --api-secret "$API_SECRET" --url "$URL" --yes \
      > "$ACC/room-$i.log" 2>&1 &
    pids+=($!)
  done
  ( while true; do ps -o %cpu=,rss= --no-headers -p "$(pgrep -f 'livekit-server --config')" 2>/dev/null >> "$ACC/cpu-mem.log"; sleep 20; done ) &
  sampler=$!
  for p in "${pids[@]}"; do wait "$p"; done
  kill "$sampler" 2>/dev/null
  echo "=== STAGE rooms=$n end $(date -Is)" >> "$ACC/summary.log"
done
echo "=== full acceptance end $(date -Is)" >> "$ACC/summary.log"
