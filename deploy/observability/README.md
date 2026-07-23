# ChatApp observability stack

This folder deploys Prometheus and Grafana for a RealtimeServices instance that exposes `/metrics`.
Both services use Linux host networking but explicitly bind their UI ports to `127.0.0.1` on purpose. Use SSH forwarding from an administrator workstation instead of exposing Grafana or Prometheus to the LAN.

## Linux deployment

```bash
cd /home/yeluo/chatapp-perf/ChatAppTCP_Server/deploy/observability
cp .env.example .env
# Set a long unique GRAFANA_ADMIN_PASSWORD in .env.
docker compose up -d
```

Grafana is available through `ssh -L 3000:127.0.0.1:3000 chatapp-linux`, then open `http://127.0.0.1:3000` locally. Prometheus uses the same pattern with port 9090.

## Metric targets

`prometheus/targets/chatapp-realtime.json` defaults to `127.0.0.1:18080`, suitable when RealtimeServices runs directly on the Linux host. Replace it with the stable service DNS/port for the target environment, then reload Prometheus without restart:

```bash
docker compose exec prometheus wget -qO- --post-data='' http://localhost:9090/-/reload
```

The rules encode the initial thresholds in `docs/observability-alerts.md`; production notification routing belongs in the organization-managed Alertmanager and is intentionally not supplied with credentials here.