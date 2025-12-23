## Avro Schemas for VLC

### Current State

We are using **Avro** with Schema Registry. Schemas are located in `schemas/*.avsc`.

### Benefits of Avro Over JSON Schema

- **Message size**: Compact binary, ~40-60% smaller than JSON
- **Serialization speed**: ~2-5x faster than JSON
- **Schema evolution**: Stronger compatibility guarantees
- **Tooling**: Excellent (Kafka ecosystem native)

### Migration Completed on 2025-12-21

**Changes made:**
1. Schema files composed: `schemas/air.avsc`, `schemas/weather.avsc`
2. Producers use `AvroSerializer` with epoch milliseconds for timestamps
3. Connector configs use `io.confluent.connect.avro.AvroConverter`
4. Added `fastavro` dependency
5. Timestamp handling: Avro `timestamp-millis` (epoch ms) with ISO string offset tracking

**Removed:**
- `producer/schemas/air.json`
- `producer/schemas/weather.json`
- TimestampConverter transforms in connector configs (no longer needed)

