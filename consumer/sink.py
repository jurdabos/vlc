"""
Consumes messages from Kafka topics and sinks them to TimescaleDB.
"""

import json
import os
import signal
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG_HOST = os.getenv("PG_HOST", "timescaledb")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "vlc")
PG_USER = os.getenv("PG_USER", "vlc_dev")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
TOPICS = os.getenv("TOPICS", "vlc.air").split(",")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
running = True


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


def parse_timestamp(ts_str):
    """Parses ISO timestamp string to datetime."""
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)


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


def main():
    """Main consumer loop."""
    sink_func = sink_air_batch if SINK_TYPE == "air" else sink_weather_batch
    print(f"[{SINK_TYPE}-sink] connecting to Kafka at {KAFKA_BOOTSTRAP}")
    print(f"[{SINK_TYPE}-sink] topics: {TOPICS}, group: {GROUP_ID}")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    print(f"[{SINK_TYPE}-sink] connecting to TimescaleDB at {PG_HOST}:{PG_PORT}/{PG_DB}")
    conn = get_pg_conn()
    print(f"[{SINK_TYPE}-sink] ready, consuming...")
    batch = []
    while running:
        # Polling with timeout to allow graceful shutdown
        msg_pack = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        for tp, messages in msg_pack.items():
            for msg in messages:
                batch.append(msg.value)
        if batch:
            try:
                count = sink_func(conn, batch)
                print(f"[{SINK_TYPE}-sink] inserted {count} records")
                batch = []
            except Exception as e:
                print(f"[{SINK_TYPE}-sink] error inserting batch: {e}")
                conn.rollback()
                # Reconnecting on error
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_pg_conn()
    consumer.close()
    conn.close()
    print(f"[{SINK_TYPE}-sink] shutdown complete")


if __name__ == "__main__":
    main()
