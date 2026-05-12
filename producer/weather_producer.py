"""Polls the Valencia geoportal ArcGIS REST endpoint for weather station
readings and produces them to Kafka topic `vlc.weather`.

Source layer:
    https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer/157

Field semantics:
    - `fecha_carg`        : esriFieldTypeDate (epoch milliseconds, UTC)
    - `fiwareid`          : station identifier (stable, e.g. ``W01_AVFRANCIA_10m``)
    - `viento_dir/_vel`   : wind direction (deg) / speed (m/s)
    - `temperatur`        : air temperature in Celsius
    - `humedad_re`        : relative humidity (%)
    - `presion_ba`        : barometric pressure (hPa)
    - `precipitac`        : precipitation (mm) accumulated over the station's
      reporting interval (≈10 min for ``*_10m`` stations)
    - feature.geometry    : ``{x: lon, y: lat}`` after we ask for ``outSR=4326``

Output (Avro on Kafka topic ``vlc.weather``):
    See ``schemas/weather.avsc``. Field renames vs. the source:
    ``viento_dir`` -> ``wind_dir_deg``, ``viento_vel`` -> ``wind_speed_ms``,
    ``temperatur`` -> ``temperature_c``, ``humedad_re`` -> ``humidity_pct``,
    ``presion_ba`` -> ``pressure_hpa``, ``precipitac`` -> ``precip_mm``.
"""

import hashlib
import io
import json
import os
import signal
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import fastavro
import requests
from arcgis_client import (
    DEFAULT_BASE,
    fetch_one_feature,
    fetch_page,
    get_field_names,
    get_layer_metadata,
    query_url,
)
from confluent_kafka import Producer
from resilience import (
    InflightLimiter,
    ResilientProducer,
    RetryConfig,
)

# --------- env ---------
BASE = os.getenv("VLC_ARCGIS_BASE", DEFAULT_BASE)
LAYER_ID = int(os.getenv("VLC_LAYER_ID", "157"))
TOPIC = os.getenv("KAFKA_TOPIC", "vlc.weather")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
POLL_SECS = int(os.getenv("POLL_EVERY_SECONDS", "300"))
LIMIT = int(os.getenv("PAGE_LIMIT", "2000"))

# Lean-stack mode: skip Schema Registry, use local Avro serialization
# Set USE_SCHEMA_REGISTRY=false for lean-stack deployment
USE_SCHEMA_REGISTRY = os.getenv("USE_SCHEMA_REGISTRY", "true").lower() == "true"

# Loading Avro schema (used by both modes)
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "weather.avsc"
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    WEATHER_SCHEMA_STR = f.read()
WEATHER_SCHEMA = fastavro.schema.parse_schema(json.loads(WEATHER_SCHEMA_STR))

STATE_DIR = os.getenv("STATE_DIR", "/state")
OFFSET_FILE = os.path.join(STATE_DIR, "offset.txt")
START_OFFSET = os.getenv("START_OFFSET", "1970-01-01T00:00:00Z")  # or 'latest_db'

# Timestamp field config
TIMESTAMP_FIELD = os.getenv("TIMESTAMP_FIELD", "fecha_carg")
AUTO_TS_FIELD = os.getenv("AUTO_TS_FIELD", "true").lower() == "true"

# Stale-upstream surfacing: log a louder warning every N consecutive empty cycles
STALE_WARN_EVERY = int(os.getenv("VLC_STALE_WARN_EVERY", "12"))  # default ~1h at 5min cadence

# Optional DB bootstrap for initial offset
PG_BOOTSTRAP = os.getenv("PG_BOOTSTRAP", "false").lower() == "true"
PG_HOST = os.getenv("PGHOST", "timescaledb")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "vlc")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PW = os.getenv("PGPASSWORD", "postgres")

# Desired attribute fields (we'll intersect with what the layer actually exposes)
DESIRED_FIELDS = [
    "objectid",
    "nombre",
    "direccion",
    "viento_dir",
    "viento_vel",
    "temperatur",
    "humedad_re",
    "presion_ba",
    "precipitac",
    "fiwareid",
]

# Which fields define a change if ts is the same?
CHANGE_FIELDS = ["viento_dir", "viento_vel", "temperatur", "humedad_re", "presion_ba", "precipitac"]

session = requests.Session()
session.headers.update({"User-Agent": "vlc-python-producer/2.0"})
# Note: requests.Session has no session-level timeout. The actual timeout
# is enforced inside resilience.http_request_with_retry via RetryConfig
# (VLC_HTTP_CONNECT_TIMEOUT_SECS / VLC_HTTP_READ_TIMEOUT_SECS).

# Resilience configuration
RETRY_CONFIG = RetryConfig.from_env()
INFLIGHT_LIMITER = InflightLimiter()
DLQ_DIR = os.getenv("VLC_DLQ_DIR", os.path.join(STATE_DIR, "dlq"))

running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


# ------------- utilities -------------
def load_offset() -> str:
    if os.path.exists(OFFSET_FILE):
        return open(OFFSET_FILE, "r", encoding="utf-8").read().strip()
    if START_OFFSET == "latest_db" and PG_BOOTSTRAP:
        try:
            import psycopg2

            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PW)
            with conn, conn.cursor() as cur:
                cur.execute(
                    "select coalesce(to_char(max(ts) at time zone 'UTC', "
                    '\'YYYY-MM-DD"T"HH24:MI:SS"Z"\'), %s) from weather.weather_station_readings',
                    (START_OFFSET,),
                )
                ts = cur.fetchone()[0]
                return ts or START_OFFSET
        except Exception:
            return START_OFFSET
    return START_OFFSET


STATE_JSON = os.path.join(STATE_DIR, "state.json")


def load_state() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns per-station offsets and fingerprints.

    Returns:
        - station_offsets: dict mapping fiwareid → last seen timestamp ISO string
        - station_fingerprints: dict mapping fiwareid → last seen fingerprint hash
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    if os.path.exists(STATE_JSON):
        try:
            d = json.load(open(STATE_JSON, "r", encoding="utf-8"))
            if "station_offsets" in d:
                return dict(d.get("station_offsets", {})), dict(d.get("station_fingerprints", {}))
            old_offset = d.get("offset", START_OFFSET)
            old_seen = d.get("seen_for_offset", {})
            station_offsets = {sid: old_offset for sid in old_seen.keys()}
            return station_offsets, dict(old_seen)
        except Exception:
            pass
    load_offset()  # side effect: may bootstrap from DB
    return {}, {}


def save_state(station_offsets: Dict[str, str], station_fingerprints: Dict[str, str]) -> None:
    """Saves per-station offsets and fingerprints to JSON."""
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump({"station_offsets": station_offsets, "station_fingerprints": station_fingerprints}, f)


def save_offset(iso: str) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(iso)


def extract_lat_lon(geo: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Extracts lat/lon from either:
        - ArcGIS geometry shape: ``{"x": lon, "y": lat}``
        - Legacy `geo_point_2d` dict: ``{"lat": ..., "lon": ...}``
    """
    if isinstance(geo, dict):
        try:
            if "lat" in geo and "lon" in geo:
                lat, lon = float(geo.get("lat")), float(geo.get("lon"))
            elif "x" in geo and "y" in geo:
                lat, lon = float(geo.get("y")), float(geo.get("x"))
            else:
                return (None, None)
            return round(lat, 6), round(lon, 6)
        except Exception:
            return (None, None)
    return (None, None)


def epoch_ms_to_iso(ts_ms: int) -> str:
    """Converts epoch milliseconds (UTC) to ``YYYY-MM-DDTHH:MM:SSZ``."""
    dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def value_fingerprint(rec: dict) -> str:
    """Creates a fingerprint of the value fields to detect data changes."""
    payload = {k: rec.get(k) for k in CHANGE_FIELDS}
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def map_record(r: Dict[str, Any], ts_field: str) -> Dict[str, Any]:
    """Maps a single ArcGIS row (attributes + lat/lon) to the Avro shape."""
    lat: Optional[float]
    lon: Optional[float]
    if r.get("lat") is not None and r.get("lon") is not None:
        try:
            lat = round(float(r["lat"]), 6)
            lon = round(float(r["lon"]), 6)
        except (TypeError, ValueError):
            lat, lon = extract_lat_lon(r.get("geo_point_2d"))
    else:
        lat, lon = extract_lat_lon(r.get("geo_point_2d"))
    ts_raw = r.get(ts_field)
    ts_ms: Optional[int] = None
    ts_iso: Optional[str] = None
    if ts_raw is not None:
        try:
            ts_ms = int(ts_raw)
            ts_iso = epoch_ms_to_iso(ts_ms)
        except (TypeError, ValueError):
            ts_ms = None
    out = {
        "fiwareid": r.get("fiwareid") or f"obj{r.get('objectid') or 'na'}",
        "ts": ts_ms,
        "_ts_iso": ts_iso,
        "wind_dir_deg": r.get("viento_dir"),
        "wind_speed_ms": r.get("viento_vel"),
        "temperature_c": r.get("temperatur"),
        "humidity_pct": r.get("humedad_re"),
        "pressure_hpa": r.get("presion_ba"),
        "precip_mm": r.get("precipitac"),
        # location
        "lat": lat,
        "lon": lon,
    }
    out["_fp"] = value_fingerprint(
        {
            "viento_dir": out["wind_dir_deg"],
            "viento_vel": out["wind_speed_ms"],
            "temperatur": out["temperature_c"],
            "humedad_re": out["humidity_pct"],
            "presion_ba": out["pressure_hpa"],
            "precipitac": out["precip_mm"],
        }
    )
    return out


def local_avro_serializer(record: Dict[str, Any], schema: dict) -> bytes:
    """Serializes a record to Avro binary with Confluent wire format header."""
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, 0))
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def produce_all(p: ResilientProducer, events: Iterable[Dict[str, Any]], serializer: Callable) -> None:
    """Produces events to Kafka with resilience (DLQ on failure)."""
    for ev in events:
        if not ev.get("ts"):
            continue
        kafka_ev = {k: v for k, v in ev.items() if k not in ("_fp", "_ts_iso")}
        key = f"{ev['fiwareid']}|{ev['_ts_iso']}"
        value_bytes = serializer(kafka_ev)
        p.produce(key=key.encode("utf-8"), value=value_bytes)
    p.flush()


# ------------- metadata helpers -------------
def choose_ts_field(avail_fields: List[str], sample: Optional[Dict[str, Any]]) -> Optional[str]:
    if TIMESTAMP_FIELD in avail_fields:
        return TIMESTAMP_FIELD
    if not AUTO_TS_FIELD:
        return TIMESTAMP_FIELD
    candidates = [
        "fecha_carg",
        "update_jcd",
        "timestamp",
        "fechahora",
        "fecha",
        "updated_at",
        "date",
        "data",
        "last_update",
    ]
    for c in candidates:
        if c in avail_fields:
            return c
    if sample:
        for k, v in sample.items():
            if isinstance(v, (int, float)) and v > 10**10:
                return k
            if isinstance(v, str) and ("T" in v and ":" in v):
                return k
    return None


def compute_select(avail_fields: List[str], ts_field: str) -> str:
    fields = [f for f in DESIRED_FIELDS if f in avail_fields]
    if ts_field not in fields:
        fields = fields + [ts_field]
    return ",".join(fields)


def bootstrap_schema() -> Tuple[str, str]:
    """Returns ``(out_fields, ts_field)`` for the configured ArcGIS layer."""
    avail_fields: List[str] = []
    sample: Optional[Dict[str, Any]] = None
    meta = get_layer_metadata(session, BASE, LAYER_ID, RETRY_CONFIG)
    if meta:
        avail_fields = get_field_names(meta)
    if not avail_fields:
        sample = fetch_one_feature(session, BASE, LAYER_ID, RETRY_CONFIG)
        if sample:
            avail_fields = list(sample.keys())
    ts_field = choose_ts_field(avail_fields, sample)
    if not ts_field:
        ts_field = TIMESTAMP_FIELD
    out_fields = compute_select(avail_fields, ts_field)
    return out_fields, ts_field


# ------------- fetching loop -------------
def fetch_since(
    station_offsets: Dict[str, str],
    station_fingerprints: Dict[str, str],
    out_fields: str,
    ts_field: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, str]]:
    """Fetches new/changed records using per-station offset tracking.

    Pages through ArcGIS layer ``LAYER_ID`` ordered by ``ts_field DESC``,
    applies per-station offset + fingerprint logic, and emits whatever
    is strictly newer (or has a changed fingerprint at the same timestamp).
    """
    out: List[Dict[str, Any]] = []
    new_offsets = dict(station_offsets)
    new_fingerprints = dict(station_fingerprints)
    page = 0
    target_url = query_url(BASE, LAYER_ID)
    while True:
        try:
            rows = fetch_page(
                session,
                BASE,
                LAYER_ID,
                out_fields=out_fields,
                ts_field=ts_field,
                limit=LIMIT,
                offset=page * LIMIT,
                config=RETRY_CONFIG,
            )
        except Exception as e:
            print(f"[weather] fetch failed against {target_url} (offset={page * LIMIT}): {type(e).__name__}: {e}")
            return out, new_offsets, new_fingerprints
        if not rows:
            break
        for r in rows:
            ev = map_record(r, ts_field)
            ts_iso = ev.get("_ts_iso")
            sid = ev.get("fiwareid")
            fp = ev.get("_fp")
            if not (ts_iso and sid and fp):
                continue
            station_offset = station_offsets.get(sid, START_OFFSET)
            station_fp = station_fingerprints.get(sid)
            should_emit = False
            if ts_iso > station_offset:
                should_emit = True
            elif ts_iso == station_offset and fp != station_fp:
                should_emit = True
            if should_emit:
                out.append(ev)
                new_offsets[sid] = ts_iso
                new_fingerprints[sid] = fp
        if len(rows) < LIMIT:
            break
        page += 1
    return out, new_offsets, new_fingerprints


def main():
    """Main loop with resilience: backoff, inflight limiting, DLQ retry."""
    station_offsets, station_fingerprints = load_state()
    out_fields, ts_field = bootstrap_schema()
    print(f"[weather] using ts_field='{ts_field}', outFields='{out_fields}'")
    print(f"[weather] source: {query_url(BASE, LAYER_ID)}")
    print(f"[weather] per-station offsets: {len(station_offsets)} stations tracked")
    if station_offsets:
        min_off = min(station_offsets.values())
        max_off = max(station_offsets.values())
        print(f"[weather] offset range: {min_off} to {max_off}")
    print(
        f"[weather] resilience: max_inflight={INFLIGHT_LIMITER.max_inflight}, "
        f"backoff_base={RETRY_CONFIG.base_delay_ms}ms, "
        f"max_retries={RETRY_CONFIG.max_retries}"
    )
    if USE_SCHEMA_REGISTRY:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        sr_serializer = AvroSerializer(schema_registry_client, WEATHER_SCHEMA_STR)
        ctx = SerializationContext(TOPIC, MessageField.VALUE)

        def avro_serializer(rec):
            """Serializes a record using Schema Registry."""
            return sr_serializer(rec, ctx)

        print(f"[weather] using Schema Registry at {SCHEMA_REGISTRY_URL} (Avro)")
    else:

        def avro_serializer(rec):
            """Serializes a record using local Avro."""
            return local_avro_serializer(rec, WEATHER_SCHEMA)

        print("[weather] using local Avro serialization (no Schema Registry)")
    producer_config = {
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 50,
        "enable.idempotence": True,
    }
    raw_producer = Producer(producer_config)
    producer = ResilientProducer(raw_producer, TOPIC, dlq_dir=DLQ_DIR, producer_config=producer_config)
    empty_cycles = 0
    last_emit_at = time.monotonic()
    while running:
        try:
            producer.check_health()
            dlq_retried = producer.retry_dlq()
            if dlq_retried:
                producer.flush()
            with INFLIGHT_LIMITER:
                items, new_offsets, new_fps = fetch_since(station_offsets, station_fingerprints, out_fields, ts_field)
            if items:
                produce_all(producer, items, avro_serializer)
                save_state(new_offsets, new_fps)
                station_offsets, station_fingerprints = new_offsets, new_fps
                stats = producer.stats
                stats_str = ""
                if stats:
                    stats_str = f" (ok={stats.success_count}, fail={stats.failure_count})"
                updated_stations = {ev["fiwareid"] for ev in items}
                print(
                    f"[weather] produced {len(items)} from {len(updated_stations)} stations; "
                    f"tracking {len(station_offsets)} stations{stats_str}"
                )
                empty_cycles = 0
                last_emit_at = time.monotonic()
            else:
                empty_cycles += 1
                msg = "[weather] no new records"
                if empty_cycles % STALE_WARN_EVERY == 0:
                    minutes = int((time.monotonic() - last_emit_at) // 60)
                    msg += f" (warn: {empty_cycles} consecutive empty cycles, {minutes} min since last emit)"
                print(msg)
            dlq_size = producer.dlq_size
            if dlq_size > 0:
                print(f"[weather] DLQ has {dlq_size} pending messages")
        except Exception as e:
            print(f"[weather] ERROR: {type(e).__name__}: {e}")
        for _ in range(POLL_SECS):
            if not running:
                break
            time.sleep(1)


if __name__ == "__main__":
    main()
