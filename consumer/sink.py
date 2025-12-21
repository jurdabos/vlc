"""
Consumes messages from Kafka topics and sinks them to TimescaleDB.

Supports both Avro (with Confluent Schema Registry header) and plain JSON payloads.
For lean-stack deployment (alt-sink profile), Avro schemas are loaded from local files,
so Schema Registry is not required.
"""

import io
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import fastavro
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG_HOST = os.getenv("PG_HOST", "timescaledb")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "vlc")
PG_USER = os.getenv("PG_USER", "vlc_dev")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
TOPICS = os.getenv("TOPICS", "vlc.air").split(",")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
SCHEMAS_DIR = os.getenv("SCHEMAS_DIR", "/schemas")
running = True

# Avro schemas loaded lazily at startup
AVRO_SCHEMAS = {}


def load_avro_schemas():
    """
    Loads Avro schemas from local .avsc files.

    Called once at startup to populate AVRO_SCHEMAS dict.
    """
    global AVRO_SCHEMAS
    schemas_path = Path(SCHEMAS_DIR)
    if not schemas_path.exists():
        logger.warning(f"Schemas directory not found: {SCHEMAS_DIR}")
        return
    for schema_file in schemas_path.glob("*.avsc"):
        try:
            schema = fastavro.schema.load_schema(str(schema_file))
            # Extracting schema name from namespace.name or just name
            schema_name = schema.get("name", schema_file.stem)
            namespace = schema.get("namespace", "")
            full_name = f"{namespace}.{schema_name}" if namespace else schema_name
            AVRO_SCHEMAS[schema_file.stem] = schema  # e.g., "air" -> schema
            AVRO_SCHEMAS[full_name] = schema  # e.g., "vlc.air.AirQualityReading" -> schema
            logger.info(f"Loaded Avro schema: {schema_file.name} ({full_name})")
        except Exception as e:
            logger.warning(f"Failed to load Avro schema {schema_file}: {e}")


def signal_handler(sig, frame):
    """Handles shutdown signals."""
    global running
    print("[sink] shutting down...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_pg_conn():
    """Returns a psycopg2 connection."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def parse_timestamp(ts_value):
    """
    Parses timestamp to datetime.

    Handles:
    - datetime objects (from fastavro with timestamp-millis logical type)
    - int (epoch milliseconds from raw Avro)
    - str (ISO format from JSON)
    """
    if isinstance(ts_value, datetime):
        # fastavro returns datetime for timestamp-millis logical type
        if ts_value.tzinfo is None:
            return ts_value.replace(tzinfo=timezone.utc)
        return ts_value
    if isinstance(ts_value, int):
        # Avro timestamp-millis: epoch milliseconds
        return datetime.fromtimestamp(ts_value / 1000, timezone.utc)
    if isinstance(ts_value, str):
        if ts_value.endswith("Z"):
            ts_value = ts_value[:-1] + "+00:00"
        return datetime.fromisoformat(ts_value)
    raise ValueError(f"Unexpected timestamp type: {type(ts_value)}")


def sink_air_batch(conn, records):
    """Inserts air records into air.hyper using upsert."""
    if not records:
        return 0
    sql = """
        INSERT INTO air.hyper (fiwareid, ts, no2, o3, so2, co, pm10, pm25, air_quality_summary, lat, lon)
        VALUES %s
        ON CONFLICT (fiwareid, ts) DO UPDATE SET
            no2 = EXCLUDED.no2,
            o3 = EXCLUDED.o3,
            so2 = EXCLUDED.so2,
            co = EXCLUDED.co,
            pm10 = EXCLUDED.pm10,
            pm25 = EXCLUDED.pm25,
            air_quality_summary = EXCLUDED.air_quality_summary,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon
    """
    values = []
    for r in records:
        values.append(
            (
                r.get("fiwareid"),
                parse_timestamp(r.get("ts")),
                r.get("no2"),
                r.get("o3"),
                r.get("so2"),
                r.get("co"),
                r.get("pm10"),
                r.get("pm25"),
                r.get("air_quality_summary"),
                r.get("lat"),
                r.get("lon"),
            )
        )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(values)


def sink_weather_batch(conn, records):
    """Inserts weather records into weather.hyper using upsert."""
    if not records:
        return 0
    sql = """
        INSERT INTO weather.hyper (
            fiwareid, ts, wind_dir_deg, wind_speed_ms, temperature_c,
            humidity_pct, pressure_hpa, precip_mm, lat, lon
        )
        VALUES %s
        ON CONFLICT (fiwareid, ts) DO UPDATE SET
            wind_dir_deg = EXCLUDED.wind_dir_deg,
            wind_speed_ms = EXCLUDED.wind_speed_ms,
            temperature_c = EXCLUDED.temperature_c,
            humidity_pct = EXCLUDED.humidity_pct,
            pressure_hpa = EXCLUDED.pressure_hpa,
            precip_mm = EXCLUDED.precip_mm,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon
    """
    values = []
    for r in records:
        values.append(
            (
                r.get("fiwareid"),
                parse_timestamp(r.get("ts")),
                r.get("wind_dir_deg"),
                r.get("wind_speed_ms"),
                r.get("temperature_c"),
                r.get("humidity_pct"),
                r.get("pressure_hpa"),
                r.get("precip_mm"),
                r.get("lat"),
                r.get("lon"),
            )
        )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(values)


SINK_TYPE = os.getenv("SINK_TYPE", "air")  # "air" or "weather"
GROUP_ID = os.getenv("GROUP_ID", f"vlc-sink-{SINK_TYPE}")


def get_schema_for_sink(sink_type: str):
    """Returns the Avro schema for the given sink type."""
    # Trying direct match first (e.g., "air" -> air.avsc)
    if sink_type in AVRO_SCHEMAS:
        return AVRO_SCHEMAS[sink_type]
    # Trying with vlc. prefix
    for key, schema in AVRO_SCHEMAS.items():
        if sink_type in key.lower():
            return schema
    return None


def avro_deserializer(raw_bytes, schema):
    """
    Deserializes Avro bytes with Confluent Schema Registry wire format.

    The wire format is: magic byte (0x00) + 4-byte schema ID + Avro binary.
    This function strips the header and deserializes using the provided local schema.
    """
    if raw_bytes is None or len(raw_bytes) == 0:
        logger.warning("Received empty or null message, skipping")
        return None
    # Checking for Confluent Schema Registry header (magic byte 0x00)
    if len(raw_bytes) > 5 and raw_bytes[0] == 0:
        # Stripping 5-byte header: magic byte + 4-byte schema ID
        avro_payload = raw_bytes[5:]
    else:
        # No header, assume raw Avro
        avro_payload = raw_bytes
    try:
        reader = io.BytesIO(avro_payload)
        return fastavro.schemaless_reader(reader, schema)
    except Exception as e:
        logger.warning(f"Failed to decode Avro message: {e}, raw={raw_bytes[:100]!r}")
        return None


def main():
    """Main consumer loop."""
    # Loading Avro schemas from local files
    load_avro_schemas()
    sink_func = sink_air_batch if SINK_TYPE == "air" else sink_weather_batch
    # Getting Avro schema for this sink type
    avro_schema = get_schema_for_sink(SINK_TYPE)
    if avro_schema:
        logger.info(f"[{SINK_TYPE}-sink] using Avro schema for deserialization")
    else:
        logger.warning(f"[{SINK_TYPE}-sink] no Avro schema found, messages will fail to deserialize")
    logger.info(f"[{SINK_TYPE}-sink] connecting to Kafka at {KAFKA_BOOTSTRAP}")
    logger.info(f"[{SINK_TYPE}-sink] topics: {TOPICS}, group: {GROUP_ID}")
    # Not using value_deserializer — deserializing manually with Avro schema
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
    )
    logger.info(f"[{SINK_TYPE}-sink] connecting to TimescaleDB at {PG_HOST}:{PG_PORT}/{PG_DB}")
    conn = get_pg_conn()
    logger.info(f"[{SINK_TYPE}-sink] ready, consuming...")
    batch = []
    while running:
        # Polling with timeout to allow graceful shutdown
        msg_pack = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        for tp, messages in msg_pack.items():
            for msg in messages:
                # Deserializing Avro message
                record = avro_deserializer(msg.value, avro_schema)
                if record is not None:
                    batch.append(record)
        if batch:
            try:
                count = sink_func(conn, batch)
                logger.info(f"[{SINK_TYPE}-sink] inserted {count} records")
                batch = []
            except Exception as e:
                logger.error(f"[{SINK_TYPE}-sink] error inserting batch: {e}")
                conn.rollback()
                # Reconnecting on error
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_pg_conn()
    consumer.close()
    conn.close()
    logger.info(f"[{SINK_TYPE}-sink] shutdown complete")


if __name__ == "__main__":
    main()
