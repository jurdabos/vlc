# secrel.md — Security & reliability baseline for **VLC**

> A tiny baseline focusing on sensible demonstration of healthy secrel habits.

---

## 1) System invariants ✅

- [✅] **No direct DB exposure.** Timescale/PostGIS is reachable only from internal services; no host‑published ports.
- [✅] **Auth in front of UIs.** Grafana and Kafka‑UI sit behind a reverse proxy with auth & rate‑limits.
- [✅] **Idempotent writes.** Upserts keyed by `(fiwareid, ts)`; replays never duplicate.
- [✅] **Backoff instead of crash.** Python producer sheds load and backs off on API slowness or DB pressure.
- [✅] **Config is versioned.** Connector JSON and producer offsets are tracked in VCS with rollbacks.

---

## 2) Compose: two networks, no DB exposure, UIs behind proxy ✅

> `compose/docker-compose.yml`.

```yaml
networks:
  vlc_internal:
    driver: bridge
    internal: true
  vlc_frontend:
    driver: bridge
[…]
```

### Cf. also `compose/nginx.conf` (auth + rate limits + IP allow‑list stub)


## 3) Python producer backoff & shed contract  ✅

- Use **exponential backoff with jitter** on HTTP 429/5xx and timeouts.
- Cap concurrent polls via `VLC_MAX_INFLIGHT_POLLS`.
- Detect DB pressure (slow inserts, connection pool saturation) → **reduce produce rate**.
- Never drop data silently; surface DLQ or on-disk queue for retries when Kafka is down.

---

## 4) CI checks ✅

- Lint + unit tests for Python producers run through gh actions.
- JSON schema validation for `connect/config/*.json`.

---

## 5) Notes & conventions ✅

- Topic names: `vlc.<domain>`
- Tables: `<domain>.hyper` where domain ∈ `{air, weather}`
- Primary key: `(fiwareid, ts)`; `ts` is UTC.
- Keep **`.env.example`** with non‑secret placeholders; commit it. Real secrets live in .env.

---

## 6) Config & producer versioning ✅

- For config rollbacks, we can use standard git operations.
- Each producer writes to /state/state.json inside its container, with separate volumes to prevent cross-contamination.
> For offset rollbacks, we need to: 1. Reset the state file in the volume, or 2. Use PG_BOOTSTRAP=true to re-bootstrap from TimescaleDB's max(ts)

## 7) Backups & restore drill (PROCRASTINATED INTO THE FUTURE BASED ON PROF FEEDBACK)

> Nightly dump; periodic restore proof.

```bash
# Backup (container shell or psql client from host)
pg_dump -h timescaledb -U vlc -d vlc -F c -f /backups/vlc_$(date +%F).dump
# Restore (wipe test DB, then)
pg_restore -h timescaledb -U vlc -d vlc --clean --if-exists /backups/vlc_YYYY-MM-DD.dump
```

Plan: Add a weekly GitHub Action to copy `/backups/*.dump` to durable storage.

---
*End. Keep file short; treat it as a living, breathing entity.

