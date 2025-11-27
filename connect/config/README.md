# Kafka Connect JDBC Sink Configurations

This directory contains JDBC Sink connector configurations for streaming data from Kafka to TimescaleDB.

## Connectors

| Config File 				| Topic 	| Target Table    | Description 	     |
|---------------------------------------|---------------|-----------------|--------------------------|
| `jdbc-sink.timescale.air.json` 	| `vlc.air` 	| `air.hyper`     | Air quality measurements |
| `jdbc-sink.timescale.weather.json` 	| `vlc.weather` | `weather.hyper` | Weather station readings |

## Registering Connectors

The Connect REST API runs on port 8083 inside the `connect` container. Since the container is on an internal-only network with no exposed ports (zero-trust bunker), all commands must use `docker exec`.

### Register connectors

```powershell
# Register air connector (from project root)
docker exec connect curl -s -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d (Get-Content connect/config/jdbc-sink.timescale.air.json -Raw)

# Register weather connector
docker exec connect curl -s -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d (Get-Content connect/config/jdbc-sink.timescale.weather.json -Raw)
```

Alternatively, since configs are mounted at `/config` inside the container:

```powershell
docker exec connect curl -s -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d '@/config/jdbc-sink.timescale.air.json'
```

## Managing Connectors

### List all connectors
```powershell
docker exec connect curl -s http://localhost:8083/connectors | ConvertFrom-Json
```

### Check connector status
```powershell
docker exec connect curl -s http://localhost:8083/connectors/jdbc-sink-timescale-air/status | ConvertFrom-Json
docker exec connect curl -s http://localhost:8083/connectors/jdbc-sink-timescale-weather/status | ConvertFrom-Json
```

### Pause/Resume connector
```powershell
docker exec connect curl -s -X PUT http://localhost:8083/connectors/jdbc-sink-timescale-air/pause
docker exec connect curl -s -X PUT http://localhost:8083/connectors/jdbc-sink-timescale-air/resume
```

### Delete connector
```powershell
docker exec connect curl -s -X DELETE http://localhost:8083/connectors/jdbc-sink-timescale-air
```

### Update connector config
```powershell
docker exec connect curl -s -X PUT http://localhost:8083/connectors/jdbc-sink-timescale-air/config `
  -H "Content-Type: application/json" `
  -d (Get-Content connect/config/jdbc-sink.timescale.air.json -Raw)
```

## Secrets

Database credentials are injected via the FileConfigProvider from `connect/secrets/secrets.properties`:

```
${file:secrets.properties:TS_JDBC_URL}
${file:secrets.properties:TS_USERNAME}
${file:secrets.properties:TS_PASSWORD}
```

The secrets file is mounted read-only at `/opt/kafka/connect/secrets/` in the container.

## Configuration Details

Both connectors use:
- **Upsert mode** with composite PK `(fiwareid, ts)` for idempotent writes
- **TimestampConverter** transform to parse ISO 8601 timestamps (`yyyy-MM-dd'T'HH:mm:ssX`)
- **JsonConverter** without schemas (producer emits schema-less JSON)
- **Batch size** of 3000 records for efficient bulk inserts

## Validation

Configs are validated in CI against `jdbc-sink.schema.json`. To validate locally:

```bash
uv run python -c "
import json, jsonschema
schema = json.load(open('jdbc-sink.schema.json'))
config = json.load(open('jdbc-sink.timescale.air.json'))
jsonschema.validate(config, schema)
print('Valid')
"
```
