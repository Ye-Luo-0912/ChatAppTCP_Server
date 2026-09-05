#!/bin/bash
tail -3 ~/sfu-validation/full-spec-summary.log 2>/dev/null
pgrep -cf "lk load-test" 2>/dev/null
ps -o %cpu=,rss= --no-headers -p "$(pgrep -f 'livekit-server --config')" 2>/dev/null | head -1
