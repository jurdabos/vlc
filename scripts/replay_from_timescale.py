#!/usr/bin/env python3
"""
Replay historical data from TimescaleDB to Kafka.

Reads air and/or weather data from TimescaleDB and produces to Kafka topics
using JSON Schema serialization. Useful for:
- Testing the pipeline with historical data
- Backfilling after schema changes
- Development and debugging
- Replaying data to new Kafka cluster

Usage:
    uv run scripts/replay_from_timescale.py --help
    uv run scripts/replay_from_timescale.py --dataset air --since 2025-11-01
    uv run scripts/replay_from_timescale.py --dataset weather --since 2025-11-01 --until 2025-11-15
    uv run scripts/replay_from_timescale.py --dataset both --dry-run

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS - Kafka broker (default: kafka:9092)
    SCHEMA_REGISTRY_URL     - Schema Registry URL (default: http://schema-registry:8081)
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD - Database connection
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import psycopg2
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# --- Configuration ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

PG_HOST = os.getenv("PG_HOST", "timescaledb")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "vlc")
PG_USER = os.getenv("PG_USER", "vlc_dev")
PG_PASSWORD = os.getenv("PG_PASSWORD", os.getenv("VLC_DEV_PASSWORD", "postgres"))

# Topics
AIR_TOPIC = "vlc.air"
WEATHER_TOPIC = "vlc.weather"

# Batch and rate limiting
DEFAULT_BATCH_SIZE = 1000
DEFAULT_RATE_LIMIT = 0  # messages per second (0 = unlimited)


def get_db_connection():
    """Creates a connection to TimescaleDB."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def load_schema(schema_name: str) -> str:
    """Loads JSON schema from producer/schemas/ directory."""
    # Finding schema file relative to this script or in producer/schemas
    script_dir = Path(__file__).parent
    schema_paths = [
        script_dir.parent / "producer" / "schemas" / f"{schema_name}.json",
        script_dir / ".." / "producer" / "schemas" / f"{schema_name}.json",
        Path(f"producer/schemas/{schema_name}.json"),
    ]
    for path in schema_paths:
        if path.exists():
            return path.read_text(encoding="utf-8")
    raise FileNotFoundError(f"Schema not found: {schema_name}.json")


def format_ts(dt: datetime) -> str:
    """Formats datetime as ISO 8601 string with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_air_data(
    conn, since: Optional[datetime], until: Optional[datetime], batch_size: int
) -> Generator[dict, None, None]:
    """Fetches air quality data from TimescaleDB."""
    query = """
        SELECT fiwareid, ts, no2, o3, so2, co, pm10, pm25,
               air_quality_summary, lat, lon
        FROM air.hyper
        WHERE 1=1
    """
    params = []
    if since:
        query += " AND ts >= %s"
        params.append(since)
    if until:
        query += " AND ts < %s"
        params.append(until)
    query += " ORDER BY ts ASC"

    with conn.cursor(name="air_replay") as cur:
        cur.itersize = batch_size
        cur.execute(query, params)
        for row in cur:
            yield {
                "fiwareid": row[0],
                "ts": format_ts(row[1]),
                "no2": row[2],
                "o3": row[3],
                "so2": row[4],
                "co": row[5],
                "pm10": row[6],
                "pm25": row[7],
                "air_quality_summary": row[8],
                "lat": row[9],
                "lon": row[10],
            }


def fetch_weather_data(
    conn, since: Optional[datetime], until: Optional[datetime], batch_size: int
) -> Generator[dict, None, None]:
    """Fetches weather data from TimescaleDB."""
    query = """
        SELECT fiwareid, ts, wind_dir_deg, wind_speed_ms, temperature_c,
               humidity_pct, pressure_hpa, precip_mm, lat, lon
        FROM weather.hyper
        WHERE 1=1
    """
    params = []
    if since:
        query += " AND ts >= %s"
        params.append(since)
    if until:
        query += " AND ts < %s"
        params.append(until)
    query += " ORDER BY ts ASC"

    with conn.cursor(name="weather_replay") as cur:
        cur.itersize = batch_size
        cur.execute(query, params)
        for row in cur:
            yield {
                "fiwareid": row[0],
                "ts": format_ts(row[1]),
                "wind_dir_deg": row[2],
                "wind_speed_ms": row[3],
                "temperature_c": row[4],
                "humidity_pct": row[5],
                "pressure_hpa": row[6],
                "precip_mm": row[7],
                "lat": row[8],
                "lon": row[9],
            }


def count_records(conn, table: str, since: Optional[datetime], until: Optional[datetime]) -> int:
    """Counts records in the given time range."""
    query = f"SELECT COUNT(*) FROM {table} WHERE 1=1"
    params = []
    if since:
        query += " AND ts >= %s"
        params.append(since)
    if until:
        query += " AND ts < %s"
        params.append(until)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


class ReplayProducer:
    """Produces messages to Kafka with JSON Schema serialization."""

    def __init__(self, topic: str, schema_name: str, dry_run: bool = False):
        self.topic = topic
        self.dry_run = dry_run
        self.produced = 0
        self.failed = 0

        if not dry_run:
            # Setting up Schema Registry and serializer
            schema_str = load_schema(schema_name)
            sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            self.serializer = JSONSerializer(schema_str, sr_client)
            self.producer = Producer(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "linger.ms": 50,
                    "batch.num.messages": 1000,
                    "queue.buffering.max.messages": 100000,
                }
            )
        else:
            self.serializer = None
            self.producer = None

    def _delivery_callback(self, err, msg):
        """Handles delivery reports."""
        if err:
            self.failed += 1
        else:
            self.produced += 1

    def produce(self, record: dict[str, Any]) -> None:
        """Produces a single record to Kafka."""
        if self.dry_run:
            self.produced += 1
            return

        key = f"{record['fiwareid']}|{record['ts']}"
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        value = self.serializer(record, ctx)

        self.producer.produce(
            self.topic,
            key=key.encode("utf-8"),
            value=value,
            callback=self._delivery_callback,
        )
        # Polling for delivery reports
        self.producer.poll(0)

    def flush(self) -> None:
        """Flushes pending messages."""
        if self.producer:
            self.producer.flush(30)

    @property
    def stats(self) -> tuple[int, int]:
        """Returns (produced, failed) counts."""
        return self.produced, self.failed


def replay_dataset(
    conn,
    dataset: str,
    since: Optional[datetime],
    until: Optional[datetime],
    batch_size: int,
    rate_limit: float,
    dry_run: bool,
) -> tuple[int, int]:
    """Replays a dataset from TimescaleDB to Kafka."""
    if dataset == "air":
        topic = AIR_TOPIC
        schema_name = "air"
        table = "air.hyper"
        fetch_fn = fetch_air_data
    else:
        topic = WEATHER_TOPIC
        schema_name = "weather"
        table = "weather.hyper"
        fetch_fn = fetch_weather_data

    # Counting records
    total = count_records(conn, table, since, until)
    if total == 0:
        print(f"[{dataset}] No records found in range.")
        return 0, 0

    print(f"[{dataset}] Replaying {total:,} records to {topic} ...")
    if dry_run:
        print(f"[{dataset}] DRY RUN - no messages will be sent.")

    producer = ReplayProducer(topic, schema_name, dry_run)
    start_time = time.time()
    last_report = start_time
    count = 0

    for record in fetch_fn(conn, since, until, batch_size):
        producer.produce(record)
        count += 1

        # Rate limiting
        if rate_limit > 0:
            expected_time = count / rate_limit
            elapsed = time.time() - start_time
            if elapsed < expected_time:
                time.sleep(expected_time - elapsed)

        # Progress reporting every 10 seconds
        now = time.time()
        if now - last_report >= 10:
            pct = (count / total) * 100
            rate = count / (now - start_time)
            print(f"[{dataset}] Progress: {count:,}/{total:,} ({pct:.1f}%) - {rate:.0f} msg/s")
            last_report = now

    producer.flush()
    elapsed = time.time() - start_time
    produced, failed = producer.stats

    print(f"[{dataset}] Completed: {produced:,} produced, {failed:,} failed in {elapsed:.1f}s")
    if elapsed > 0:
        print(f"[{dataset}] Average rate: {produced / elapsed:.0f} msg/s")

    return produced, failed


def parse_datetime(s: str) -> datetime:
    """Parses datetime string in various formats."""
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {s}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Replay historical data from TimescaleDB to Kafka.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Replay air data from November:
    %(prog)s --dataset air --since 2025-11-01

  Replay weather data for a specific day:
    %(prog)s --dataset weather --since 2025-11-15 --until 2025-11-16

  Dry run to check record counts:
    %(prog)s --dataset both --since 2025-11-01 --dry-run

  Replay with rate limiting (1000 msg/s):
    %(prog)s --dataset air --since 2025-11-01 --rate-limit 1000
""",
    )
    parser.add_argument(
        "--dataset",
        choices=["air", "weather", "both"],
        default="both",
        help="Dataset to replay (default: both)",
    )
    parser.add_argument(
        "--since",
        type=parse_datetime,
        help="Start timestamp (inclusive), e.g. 2025-11-01",
    )
    parser.add_argument(
        "--until",
        type=parse_datetime,
        help="End timestamp (exclusive), e.g. 2025-11-30",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Database fetch batch size (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help="Max messages per second, 0 for unlimited (default: 0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count records without producing to Kafka",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("TimescaleDB → Kafka Replay")
    print("=" * 60)
    print(f"Kafka:           {KAFKA_BOOTSTRAP}")
    print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Database:        {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"Dataset:         {args.dataset}")
    print(f"Since:           {args.since or 'all time'}")
    print(f"Until:           {args.until or 'now'}")
    print(f"Dry run:         {args.dry_run}")
    print("=" * 60)

    # Connecting to database
    print("Connecting to TimescaleDB ...")
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)

    total_produced = 0
    total_failed = 0

    try:
        if args.dataset in ("air", "both"):
            produced, failed = replay_dataset(
                conn,
                "air",
                args.since,
                args.until,
                args.batch_size,
                args.rate_limit,
                args.dry_run,
            )
            total_produced += produced
            total_failed += failed

        if args.dataset in ("weather", "both"):
            produced, failed = replay_dataset(
                conn,
                "weather",
                args.since,
                args.until,
                args.batch_size,
                args.rate_limit,
                args.dry_run,
            )
            total_produced += produced
            total_failed += failed

    finally:
        conn.close()

    print("=" * 60)
    print(f"Total: {total_produced:,} produced, {total_failed:,} failed")
    if total_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
