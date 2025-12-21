# VLC Producer Services

## Overview
Two independent producer services that poll Valencia OpenDataSoft API and produce to Kafka topics:
- **air_producer.py** → `vlc.air` (air quality measurements)
- **weather_producer.py** → `vlc.weather` (weather station readings)

## Implementation Status
1. **Set up air_producer.py**:
   - Added field renaming: `calidad_am` → `air_quality_summary`

2. **Set up weather_producer.py**:
   - Field renaming:
     - `viento_dir` → `wind_dir_deg`
     - `viento_vel` → `wind_speed_ms`
     - `temperatur` → `temperature_c`
     - `humedad_re` → `humidity_pct`
     - `presion_ba` → `pressure_hpa`
     - `precipitac` → `precip_mm`

3. **Separate Dockerfiles**:
   - `Dockerfile.air` for air quality producer
   - `Dockerfile.weather` for weather producer
   - Both include Kafka dependencies (librdkafka-dev, libssl-dev, libsasl2-dev)
   - Proper non-root user setup
   - Tini for signal handling
   - Python unbuffered output for real-time logging

4. **Updated docker-compose.yml**:
   - Split into two services: `air-producer` and `weather-producer`
   - Proper context paths (`../producer`)
   - Separate state volumes for each producer
   - Health check dependencies on Kafka and TimescaleDB
   - Environment overrides for dataset IDs and topics

5. **Created producer/requirements.txt**:
   - All dependencies from pyproject.toml
   - Proper psycopg2 version (>=2.9.11)

### Features
- **Polling**: Every 5 minutes (configurable via `POLL_EVERY_SECONDS`)
- **Pagination**: Using ODS v2.1 `limit`/`offset` parameters
- **Incremental ingestion**: Per-station offset tracking with fingerprint-based deduplication
- **Offset persistence**: State stored in `/state/state.json` with per-station offsets and fingerprints
- **Deduplication**: SHA1 fingerprint of measurement values to detect changes at same timestamp
- **Avro serialization**: Uses Schema Registry with `AvroSerializer` for compact binary format
- **Timestamp handling**: Converts ISO strings to epoch milliseconds for Avro `timestamp-millis`
- **Optional DB bootstrap**: Can read initial offset from TimescaleDB `max(ts)`
- **Dual API fallback**: Tries v2.1 first, falls back to v2
- **Graceful shutdown**: SIGINT/SIGTERM handling
- **Field flattening**: `geo_point_2d` → `lat`/`lon`

## Environment Variables

### Required
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka brokers (default: `kafka:9092`)
- `VLC_EXPLORE_BASE`: ODS API v2.1 endpoint
- `VLC_DATASET_ID`: Dataset identifier (overridden per producer in docker-compose)
- `KAFKA_TOPIC`: Target Kafka topic (overridden per producer in docker-compose)

### Optional
- `POLL_EVERY_SECONDS`: Poll interval (default: `300`)
- `PAGE_LIMIT`: Records per API page (default: `100`)
- `STATE_DIR`: State file directory (default: `/state`)
- `START_OFFSET`: Initial offset if no state exists (default: `1970-01-01T00:00:00Z`)
- `PG_BOOTSTRAP`: Bootstrap offset from DB (default: `false`)
- `TIMESTAMP_FIELD`: ODS timestamp field (default: `fecha_carg`)
- `AUTO_TS_FIELD`: Auto-detect timestamp field (default: `true`)
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`: PostgreSQL connection

## Running

### Build and start producers:
```bash
docker compose -f compose/docker-compose.yml --profile infra --profile schema --profile producer up -d --build
```

### View logs:
```bash
docker compose -f compose/docker-compose.yml logs -f air-producer
docker compose -f compose/docker-compose.yml logs -f weather-producer
```

### Stop producers:
```bash
docker compose -f compose/docker-compose.yml --profile producer down
```

## Data Flow

```
Valencia ODS API (v2.1)
    ↓ (poll every 5min)
Producer (air/weather)
    ↓ (fingerprint dedup, Avro serialization)
Kafka (vlc.air / vlc.weather)
    ↓ (JDBC Sink Connector + AvroConverter)
TimescaleDB (air/weather schemas)
    ↓ (queries)
Grafana Dashboards
```

## Schema Serialization

Producers use **Avro** serialization with Schema Registry:
- Schemas: `schemas/air.avsc`, `schemas/weather.avsc`
- Timestamps: Epoch milliseconds (`timestamp-millis` logical type)
- Converter: `AvroSerializer` from `confluent_kafka.schema_registry.avro`

## Quality Checks Passed
✅ Field renaming as specified
✅ Proper Kafka key format: `{fiwareid}|{ts_iso}` (ISO string for human readability)
✅ Per-station offset persistence with fingerprint-based deduplication
✅ Avro serialization with Schema Registry
✅ Graceful shutdown handling
✅ Non-root Docker user for security
✅ Separate state volumes prevent cross-contamination
