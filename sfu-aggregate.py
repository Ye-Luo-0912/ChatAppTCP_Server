import re

subs = exp = loss = 0
tot_bw = 0.0
rooms = 0
peak_cpu = 0.0
peak_rss = 0

for i in range(1, 31):
    try:
        t = open(f"spec-room-{i}.log", encoding="utf-8").read()
    except FileNotFoundError:
        continue
    for line in t.splitlines():
        if not line.lstrip("│ ").startswith("Total"):
            continue
        rooms += 1
        cells = [c.strip() for c in line.split("│") if c.strip()]
        m = re.match(r"(\d+)/(\d+)", cells[1])
        if m:
            subs += int(m.group(1))
            exp += int(m.group(2))
        bw = cells[2].split(" ")[0]
        if bw.endswith("mbps"):
            tot_bw += float(bw[:-4]) * 1000
        elif bw.endswith("kbps"):
            tot_bw += float(bw[:-4])
        lm = re.match(r"(\d+)", cells[3])
        if lm:
            loss += int(lm.group(1))

try:
    for line in open("spec-cpu-mem.log"):
        parts = line.split()
        if len(parts) >= 2:
            peak_cpu = max(peak_cpu, float(parts[0]))
            peak_rss = max(peak_rss, int(parts[1]))
except Exception:
    pass

print(f"rooms={rooms} tracksubs={subs}/{exp} throughput={tot_bw/1000:.1f}mbps "
      f"loss={loss} peakCPU={peak_cpu}% peakRSS={peak_rss//1024}MB")
