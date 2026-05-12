"""Polls the Valencia geoportal ArcGIS REST air-pollution layer (id 156) once
and reports whether the snapshot tick (`fecha_carg`) advanced relative to the
last observed value persisted to ``./state/last_tick.txt``.

Note: this script previously used the Opendatasoft Explore v2.1 API; the
geoportal ArcGIS endpoint replaces it 1:1 since 2026-05.
"""

import os
from datetime import datetime, timezone

import requests

BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "156"))
URL = f"{BASE.rstrip('/')}/{LAYER_ID}/query"
STATE = os.environ.get("STATE_FILE", os.path.join(".", "state", "last_tick.txt"))

# ArcGIS layers cap at maxRecordCount=2000; the air layer holds ~11 features
MAX_LIMIT = 2000


def epoch_ms_to_utc(ts_ms: int) -> datetime:
    """Converts ArcGIS epoch-ms `fecha_carg` to a tz-aware UTC datetime."""
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)


def _query(extra_params: dict) -> dict:
    """Fetches the query endpoint with sane defaults; raises on protocol errors."""
    params = {"where": "1=1", "f": "json", "outFields": "*", "returnGeometry": "false"}
    params.update(extra_params)
    r = requests.get(URL, params=params, headers={"Accept": "application/json"}, timeout=(10, 60))
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        snippet = r.text[:600].replace("\n", " ")
        raise SystemExit(f"HTTP {r.status_code} {r.reason}. Params={params}. Payload head: {snippet}") from e
    payload = r.json()
    if isinstance(payload, dict) and "error" in payload:
        raise SystemExit(f"ArcGIS error: {payload['error']}")
    return payload


def fetch_total_count() -> int:
    data = _query({"returnCountOnly": "true"})
    return int(data.get("count", 0))


def fetch_all_rows() -> list[dict]:
    """Returns every feature's attributes (lean projection of fecha_carg + fiwareid)."""
    rows: list[dict] = []
    offset = 0
    while True:
        data = _query(
            {
                "outFields": "fecha_carg,fiwareid",
                "resultRecordCount": str(MAX_LIMIT),
                "resultOffset": str(offset),
            }
        )
        feats = data.get("features", []) or []
        if not feats:
            break
        rows.extend(f.get("attributes") or {} for f in feats)
        if len(feats) < MAX_LIMIT:
            break
        offset += MAX_LIMIT
    return rows


def main() -> None:
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    rows = fetch_all_rows()
    if not rows:
        raise SystemExit("No rows returned.")
    # Snapshot semantics: typically all rows share the same fecha_carg.
    # Picking max keeps us robust if some stations lag.
    ticks = [r.get("fecha_carg") for r in rows if r.get("fecha_carg") is not None]
    if not ticks:
        raise SystemExit("Rows lacked 'fecha_carg' (epoch ms).")
    tick_ms = max(ticks)
    now_dt = epoch_ms_to_utc(tick_ms)
    stations = {r.get("fiwareid") for r in rows if r.get("fiwareid")}
    print(f"Snapshot tick: {now_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  (rows={len(rows)}, stations={len(stations)})")
    prev_iso = None
    if os.path.exists(STATE):
        prev_iso = open(STATE, "r", encoding="utf-8").read().strip() or None
    if prev_iso:
        prev_dt = datetime.fromisoformat(prev_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        if now_dt > prev_dt:
            gap_h = (now_dt - prev_dt).total_seconds() / 3600.0
            print(f"Tick advanced by {gap_h:.2f} h \u2192 emit all stations.")
        else:
            print("Same tick as last run \u2192 emit nothing.")
    else:
        print("No prior state \u2192 initialize without emitting.")
    open(STATE, "w", encoding="utf-8").write(now_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))


if __name__ == "__main__":
    main()
