# Python Sink Consumers (alt-sink)

Alternative to Kafka Connect JDBC Sink for lean-stack deployments.

## Overview

The sink consumers read from Kafka topics and write directly to TimescaleDB using psycopg2. They support Avro deserialization using local `.avsc` schemas (no Schema Registry required).

## Services

| Service | Topic | Target Table | Docker Profile |
|---------|-------|--------------|----------------|
| `air-sink` | `vlc.air` | `air.hyper` | `alt-sink` |
| `weather-sink` | `vlc.weather` | `weather.hyper` | `alt-sink` |

## Full-Stack vs Lean-Stack

| Component | Full-Stack | Lean-Stack |
|-----------|------------|------------|
| **Schema Registry** | Required | Not needed |
| **Kafka Connect** | JDBC Sink Connector | Not needed |
| **Sink** | Connect JDBC Sink | Python `sink.py` |
| **Profile** | `--profile schema` | `--profile alt-sink` |
| **Serialization** | Avro via Schema Registry | Avro via local `.avsc` files |

Both modes use the same Avro wire format. The Python sink strips the 5-byte Confluent header (magic byte + schema ID) and deserializes using fastavro with local schemas.

## Configuration

Environment variables (set via docker-compose):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `PG_HOST` | `timescaledb` | TimescaleDB host |
| `PG_PORT` | `5432` | TimescaleDB port |
| `PG_DB` | `vlc` | Database name |
| `PG_USER` | `vlc_dev` | Database user |
| `PG_PASSWORD` | (required) | Database password |
| `TOPICS` | `vlc.air` | Kafka topic(s) to consume |
| `SINK_TYPE` | `air` | Either `air` or `weather` |
| `GROUP_ID` | `vlc-sink-{SINK_TYPE}` | Kafka consumer group |
| `BATCH_SIZE` | `100` | Records to batch before insert |
| `SCHEMAS_DIR` | `/schemas` | Directory containing `.avsc` files |

## Running Locally

```bash
# Start infrastructure + producers + alt-sinks
docker compose -f compose/docker-compose.yml --profile infra --profile producer --profile alt-sink up -d

# Check sink logs
docker compose -f compose/docker-compose.yml logs air-sink --tail=50
docker compose -f compose/docker-compose.yml logs weather-sink --tail=50
```

## Avro Deserialization

The sink handles Confluent Schema Registry wire format:
- Magic byte (0x00) + 4-byte schema ID + Avro binary
- Schema ID is ignored; local `.avsc` schema is used for deserialization
- Timestamps are Avro `timestamp-millis` logical type (epoch milliseconds)

This allows switching between full-stack (Schema Registry) and lean-stack (local schemas) without changing producers.
