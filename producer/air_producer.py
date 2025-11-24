import os, time, json, re, signal, hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, Tuple, List, Optional

import requests
from confluent_kafka import Producer

# --------- env ---------
BASE1 = os.getenv("VLC_EXPLORE_BASE",  "https://valencia.opendatasoft.com/api/explore/v2.1")
BASE2 = os.getenv("VLC_EXPLORE_BASE2", "https://valencia.opendatasoft.com/api/v2")
BASES = [BASE1, BASE2]

DATASET_ID = os.getenv("VLC_DATASET_ID", "estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas")
TOPIC = os.getenv("KAFKA_TOPIC", "vlc.air")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POLL_SECS = int(os.getenv("POLL_EVERY_SECONDS", "300"))
LIMIT = int(os.getenv("PAGE_LIMIT", "100"))

STATE_DIR = os.getenv("STATE_DIR", "/state")
OFFSET_FILE = os.path.join(STATE_DIR, "offset.txt")
START_OFFSET = os.getenv("START_OFFSET", "1970-01-01T00:00:00Z")  # or 'latest_db'

# Timestamp field config
TIMESTAMP_FIELD = os.getenv("TIMESTAMP_FIELD", "fecha_carg")
AUTO_TS_FIELD = os.getenv("AUTO_TS_FIELD", "true").lower() == "true"

# Optional DB bootstrap for initial offset
PG_BOOTSTRAP = os.getenv("PG_BOOTSTRAP", "false").lower() == "true"
PG_HOST = os.getenv("PGHOST", "timescaledb")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "vlc")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PW = os.getenv("PGPASSWORD", "postgres")

# Desired fields (we'll intersect with what's available)
DESIRED_FIELDS = [
    "objectid","nombre","direccion",
    "viento_dir","viento_vel","temperatur","humedad_re","presion_ba","precipitac",
    "fiwareid",
    "geo_point_2d"  # ts field added at runtime
]

# Which fields define a change if ts is the same?
CHANGE_FIELDS = ["viento_dir","viento_vel","temperatur","humedad_re","presion_ba","precipitac"]

session = requests.Session()
session.headers.update({"User-Agent": "vlc-python-producer/1.3"})
session.timeout = (10, 60)  # connect, read

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
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PW
            )
            with conn, conn.cursor() as cur:
                cur.execute(
                    "select coalesce(to_char(max(ts) at time zone 'UTC', "
                    "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), %s) from air.air_station_readings",
                    (START_OFFSET,)
                )
                ts = cur.fetchone()[0]
                return ts or START_OFFSET
        except Exception:
            return START_OFFSET
    return START_OFFSET


STATE_JSON = os.path.join(STATE_DIR, "state.json")


def load_state() -> Tuple[str, dict]:
    """Returns the last committed offset and a mapping of station_id to fingerprint for that offset."""
    os.makedirs(STATE_DIR, exist_ok=True)
    if os.path.exists(STATE_JSON):
        try:
            d = json.load(open(STATE_JSON, "r", encoding="utf-8"))
            return d.get("offset", START_OFFSET), dict(d.get("seen_for_offset", {}))
        except Exception:
            pass
    # fallback to your old offset.txt if present
    off = load_offset()
    return off, {}  # fallback if migrating


def save_state(offset_iso: str, seen_map: dict) -> None:
    """Saves the current offset and station → fingerprint map to JSON."""
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump({"offset": offset_iso, "seen_for_offset": seen_map}, f)


def save_offset(iso: str) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(iso)

POINT_RX = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

def extract_lat_lon(geo: Any) -> Tuple[Optional[float], Optional[float]]:
    if isinstance(geo, dict):
        # ODS v2.1 often returns {"lat":..., "lon":...}
        try:
            return float(geo.get("lat")), float(geo.get("lon"))
        except Exception:
            return (None, None)
    if isinstance(geo, str):
        m = POINT_RX.match(geo)
        if m:
            lon, lat = float(m.group(1)), float(m.group(2))
            return lat, lon
    return (None, None)

def normalize_ts(s: str) -> str:
    # Ensure "YYYY-MM-DDTHH:MM:SSZ" (no millis)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            # Last resort: let requests/ODS give RFC3339 with subsec: strip subsec
            # Example: 2025-10-17T10:11:12.345Z
            s2 = s.split(".")[0].replace("Z","")
            dt = datetime.fromisoformat(s2).replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def value_fingerprint(rec: dict) -> str:
    """Creates a fingerprint of the value fields to detect data changes."""
    payload = {k: rec.get(k) for k in CHANGE_FIELDS}
    s = json.dumps(payload, sort_keys=True, separators=(",",":"))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def map_record(r: Dict[str, Any], ts_field: str) -> Dict[str, Any]:
    lat, lon = extract_lat_lon(r.get("geo_point_2d"))
    ts_raw = r.get(ts_field)
    out = {
        "fiwareid": r.get("fiwareid") or f"obj{r.get('objectid') or 'na'}",
        "ts": normalize_ts(ts_raw) if ts_raw else None,
        "objectid": r.get("objectid"),
        "nombre": r.get("nombre"),
        "direccion": r.get("direccion"),
        # meteo
        "wind_dir_deg": r.get("viento_dir"),
        "wind_speed_ms": r.get("viento_vel"),
        "temperature_c": r.get("temperatur"),
        "humidity_pct": r.get("humedad_re"),
        "pressure_hpa": r.get("presion_ba"),
        "precip_mm": r.get("precipitac"),
        # location
        "lat": lat, "lon": lon
    }
    # Add fingerprint based on mapped values
    out["_fp"] = value_fingerprint({
        "viento_dir": out["wind_dir_deg"],
        "viento_vel": out["wind_speed_ms"],
        "temperatur": out["temperature_c"],
        "humedad_re": out["humidity_pct"],
        "presion_ba": out["pressure_hpa"],
        "precipitac": out["precip_mm"]
    })
    return out


def produce_all(p: Producer, events: Iterable[Dict[str, Any]]) -> None:
    for ev in events:
        if not ev.get("ts"):  # skip malformed rows without timestamp
            continue
        key = f"{ev['station_fiwareid']}|{ev['ts']}"
        p.produce(TOPIC, key=key.encode("utf-8"), value=json.dumps(ev).encode("utf-8"))
    p.flush()

# ------------- metadata helpers -------------
def get_meta(base: str) -> Optional[Dict[str, Any]]:
    url = f"{base}/catalog/datasets/{DATASET_ID}"
    try:
        r = session.get(url)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None

def get_fields_from_meta(meta: Dict[str, Any]) -> List[str]:
    try:
        fields = meta.get("dataset", {}).get("fields", [])
        return [f.get("name") for f in fields if "name" in f]
    except Exception:
        return []

def fetch_one_record(base: str) -> Optional[Dict[str, Any]]:
    url = f"{base}/catalog/datasets/{DATASET_ID}/records"
    try:
        r = session.get(url, params={"limit": "1"})
        r.raise_for_status()
        arr = r.json().get("results", [])
        return arr[0] if arr else None
    except Exception:
        return None

def choose_ts_field(avail_fields: List[str], sample: Optional[Dict[str, Any]]) -> Optional[str]:
    # 1) honor env if present in dataset
    if TIMESTAMP_FIELD in avail_fields:
        return TIMESTAMP_FIELD
    if not AUTO_TS_FIELD:
        return TIMESTAMP_FIELD  # let it fail fast later if missing

    # 2) try meta-typed date-like names
    candidates = [
        "fecha_carg","update_jcd","timestamp","fechahora","fecha",
        "updated_at","date","data","last_update"
    ]
    for c in candidates:
        if c in avail_fields:
            return c

    # 3) infer from sample: pick first key containing plausible date text
    if sample:
        for k, v in sample.items():
            if isinstance(v, str) and ("T" in v and ":" in v):
                return k
    return None

def compute_select(avail_fields: List[str], ts_field: str) -> str:
    fields = [f for f in DESIRED_FIELDS if f in avail_fields]
    if ts_field not in fields:
        fields = fields + [ts_field]
    return ",".join(fields)

# ------------- fetching loop -------------
def fetch_since(offset_iso: str, seen_for_offset: dict, bases: List[str],
                select: str, ts_field: str
               ) -> Tuple[List[Dict[str, Any]], str, dict]:
    """Fetches records since offset, using fingerprint-based deduplication.
    
    Returns:
        - List of new/changed records to emit
        - New offset (advances only when newer timestamps found)
        - Updated seen_map for the current offset timestamp
    """
    out: List[Dict[str, Any]] = []
    max_ts = offset_iso
    seen_map = dict(seen_for_offset)  # copy to track current window
    for base in bases:
        page = 0
        while True:
            params = {
                "order_by": ts_field,
                "limit": str(LIMIT),
                "offset": str(page * LIMIT),
                "select": select,
                "where": f"{ts_field}>=date'{offset_iso}'"
            }
            try:
                resp = session.get(f"{base}/catalog/datasets/{DATASET_ID}/records", params=params)
                resp.raise_for_status()
                rows = resp.json().get("results", [])
            except Exception:
                # Try next base if nothing collected yet
                if not out: 
                    break
                else: 
                    return out, max_ts, seen_map
            if not rows: 
                break
            for r in rows:
                ev = map_record(r, ts_field)
                ts = ev.get("ts")
                sid = ev.get("station_fiwareid")
                fp = ev.get("_fp")
                if not (ts and sid and fp): 
                    continue
                if ts > max_ts:
                    # New timestamp watermark - reset the seen map
                    max_ts = ts
                    seen_map = {}
                # Decide to emit:
                # - Newer timestamp than offset: always emit
                # - Equal to offset timestamp: emit if station unseen OR fingerprint changed
                # - Equal to max timestamp: emit if not yet seen OR fingerprint different
                should_emit = False
                if ts > offset_iso:
                    # Strictly newer - always emit
                    should_emit = True
                elif ts == offset_iso:
                    # Same as offset - emit if value changed
                    if seen_for_offset.get(sid) != fp:
                        should_emit = True
                elif ts == max_ts:
                    # Same as current max - emit if not yet tracked
                    if seen_map.get(sid) != fp:
                        should_emit = True
                if should_emit:
                    out.append(ev)
                    if ts == max_ts:
                        # Track this station's fingerprint for current timestamp
                        seen_map[sid] = fp
            if len(rows) < LIMIT: 
                break
            page += 1
        if out: 
            break  # Got data from this base, don't try others
    # Determine new offset and seen map to persist
    # Only advance offset if we found strictly newer timestamps
    new_offset = max_ts if max_ts > offset_iso else offset_iso
    # Return the seen map for the current offset timestamp
    # If offset advanced, seen_map contains stations for new timestamp
    # If offset stayed same, seen_map contains merged view of all seen stations
    return out, new_offset, seen_map


def bootstrap_schema() -> Tuple[str, str]:
    """Return (SELECT, ts_field) choosing bases, fields, and timestamp column robustly."""
    avail_fields: List[str] = []
    sample: Optional[Dict[str, Any]] = None

    # Try meta v2.1 then v2
    meta = get_meta(BASES[0]) or get_meta(BASES[1])
    if meta:
        avail_fields = get_fields_from_meta(meta)

    # If no meta fields, try to get one sample row (either base)
    if not avail_fields:
        sample = fetch_one_record(BASES[0]) or fetch_one_record(BASES[1])
        if sample:
            avail_fields = list(sample.keys())

    ts_field = choose_ts_field(avail_fields, sample)
    if not ts_field:
        # fall back to env even if not present — fetch will fail loudly, which is OK
        ts_field = TIMESTAMP_FIELD

    select = compute_select(avail_fields, ts_field)
    return select, ts_field

def main():
    offset, seen = load_state()
    select, ts_field = bootstrap_schema()
    print(f"[sidecar] using ts_field='{ts_field}', SELECT='{select}'")
    print(f"[sidecar] starting with offset {offset}, seen_for_offset={len(seen)}")
    
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "enable.idempotence": True})
    
    while running:
        try:
            items, new_offset, new_seen = fetch_since(offset, seen, BASES, select, ts_field)
            if items:
                produce_all(producer, items)
                save_state(new_offset, new_seen)
                offset, seen = new_offset, new_seen
                print(f"[sidecar] produced {len(items)}; offset={offset}; seen={len(seen)}")
            else:
                print("[sidecar] no new records")
        except Exception as e:
            print(f"[sidecar] ERROR: {e}")
        
        for _ in range(POLL_SECS):
            if not running: 
                break
            time.sleep(1)


if __name__ == "__main__":
    main()
