# VLC Producer Services
## Overview
Two independent Python services poll the Valencia geoportal ArcGIS REST API
and produce Avro-encoded readings to Kafka:
- `air_producer.py` polls layer **156** (`Estacions contaminació atmosfèriques`)
  → `vlc.air`
- `weather_producer.py` polls layer **157** (`Estacions Atmosfèriques`)
  → `vlc.weather`
Both services share `arcgis_client.py` (HTTP + paging), `resilience.py`
(backoff, DLQ, throttling, broker-reconnect) and the Avro schemas in
`schemas/*.avsc`.
## Source endpoint
```
https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer/{LAYER_ID}/query
```
Fetched with:
```
where=1=1
outFields=<intersection of DESIRED_FIELDS with the layer's actual fields>
returnGeometry=true
outSR=4326
orderByFields=fecha_carg DESC
resultRecordCount=<PAGE_LIMIT, default 2000>
resultOffset=<page * PAGE_LIMIT>
f=json
```
The layers expose epoch-millisecond `fecha_carg` timestamps and
`feature.geometry = {x: lon, y: lat}` (after `outSR=4326`). The producers
forward `ts` as `timestamp-millis` directly and keep an ISO mirror
(`_ts_iso`) for the per-station offset state and Kafka key.
### Field renames at the producer boundary
Air:
- `calidad_am` → `air_quality_summary`
Weather:
- `viento_dir` → `wind_dir_deg`
- `viento_vel` → `wind_speed_ms`
- `temperatur` → `temperature_c`
- `humedad_re` → `humidity_pct`
- `presion_ba` → `pressure_hpa`
- `precipitac` → `precip_mm`
## Features
- **Polling**: every `POLL_EVERY_SECONDS` seconds (default `300`).
- **Pagination**: ArcGIS `resultRecordCount` / `resultOffset`, capped at the
  layer's `maxRecordCount` (`2000` for both layers).
- **Per-station incremental ingestion**: each station's last seen ISO
  timestamp + a SHA1 fingerprint of the value fields are kept in
  `/state/state.json`. A reading is emitted iff
  `ts > station_offset` _or_ `ts == station_offset and fingerprint != stored`.
- **Avro serialization**: defaults to Schema Registry (`AvroSerializer`);
  set `USE_SCHEMA_REGISTRY=false` for the lean-stack mode that uses local
  Avro with the Confluent wire-format header (schema id `0`).
- **Resilient HTTP**: exponential backoff with jitter on 429/5xx and
  timeouts (`producer/resilience.py`).
- **Resilient produce**: tracks delivery callbacks, DLQs failed messages,
  reconnects to Kafka after consecutive failures.
- **Loud upstream failures**: every fetch error logs the exception and the
  ArcGIS URL it failed against (no more silent `no new records` masking
  outages); after `VLC_STALE_WARN_EVERY` consecutive empty cycles the
  producer also logs how long it has been since the last successful emit.
- **Optional DB bootstrap**: `PG_BOOTSTRAP=true` + `START_OFFSET=latest_db`
  reads the per-table `max(ts)` to seed offsets on a fresh deploy.
- **Graceful shutdown**: SIGINT / SIGTERM flip a `running` flag and the
  current iteration completes cleanly.
## Environment variables
### Required
- `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`)
- `VLC_ARCGIS_BASE` (default `https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer`)
- `VLC_LAYER_ID` (set per producer service in compose: `156` for air, `157` for weather)
- `KAFKA_TOPIC` (set per producer service in compose: `vlc.air` / `vlc.weather`)
### Optional
- `POLL_EVERY_SECONDS` (default `300`)
- `PAGE_LIMIT` (default `2000`)
- `STATE_DIR` (default `/state`)
- `START_OFFSET` (default `1970-01-01T00:00:00Z`; set to `latest_db` together with `PG_BOOTSTRAP=true` to reseed from Timescale)
- `PG_BOOTSTRAP` (default `false`)
- `TIMESTAMP_FIELD` (default `fecha_carg`)
- `AUTO_TS_FIELD` (default `true`)
- `VLC_STALE_WARN_EVERY` (default `12`; with the default poll cadence this surfaces a warning roughly every hour of empty fetches)
- `USE_SCHEMA_REGISTRY` (default `true`)
- `SCHEMA_REGISTRY_URL` (default `http://schema-registry:8081`)
- `VLC_BACKOFF_BASE_MS`, `VLC_BACKOFF_MAX_MS`, `VLC_BACKOFF_MAX_RETRIES`, `VLC_BACKOFF_JITTER` (resilience tuning)
- `VLC_MAX_INFLIGHT_POLLS` (default `1`)
- `VLC_DLQ_DIR` (default `<STATE_DIR>/dlq`)
- `VLC_KAFKA_FAILURE_THRESHOLD`, `VLC_KAFKA_CHECK_INTERVAL_SECS`
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` (only when `PG_BOOTSTRAP=true`)
## Running
Build and start (full stack with Schema Registry + Connect):
```bash
docker compose -f compose/docker-compose.yml --profile infra --profile schema --profile producer up -d --build
```
Or lean stack with the Python alt-sink:
```bash
USE_SCHEMA_REGISTRY=false docker compose -f compose/docker-compose.yml --profile infra --profile producer --profile alt-sink up -d --build
```
Tail logs:
```bash
docker compose -f compose/docker-compose.yml logs -f air-producer
docker compose -f compose/docker-compose.yml logs -f weather-producer
```
Stop:
```bash
docker compose -f compose/docker-compose.yml --profile producer down
```
## Data flow
```
Valencia geoportal ArcGIS REST  (MedioAmbiente/MapServer/156, /157)
        ↓ poll every 5 min
Producers (air_producer + weather_producer)
        ↓ per-station offset + fingerprint dedup, Avro serialization
Kafka (vlc.air / vlc.weather topics)
        ↓ JDBC Sink Connector (Avro, upsert on (fiwareid, ts))
TimescaleDB (air.hyper / weather.hyper hypertables)
        ↓ SQL queries
Grafana dashboards
```
## Schema serialization
Schemas live in `schemas/air.avsc` and `schemas/weather.avsc`. Timestamps use
the Avro `timestamp-millis` logical type (epoch milliseconds). Kafka keys use
the format `{fiwareid}|{_ts_iso}` so the same logical record always hashes
to the same partition.
## Quality checks passed
- Field renames as specified
- Per-station offset persistence with fingerprint-based deduplication
- Avro serialization (Schema Registry or lean local mode)
- Graceful shutdown handling
- Non-root Docker user for security
- Separate state volumes prevent cross-contamination
- Fetch errors are logged with exception type and failing URL (no silent masking)
