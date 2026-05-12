#!/usr/bin/env python3
"""weather_snapshot_watcher.py

Polls the Valencia weather snapshot (geoportal ArcGIS REST, layer 157 by
default) and prints station values *only* when updates land.

What it does:
- Detects a dataset-level advance (max tick across stations increases).
- Detects partial advances (some stations catch up to the same max tick).
- Helps verify whether each station publishes fresh values
  (dir/vel/temp/hum/press/rain) at the new tick or whether some rows lag.

Usage:
    uv run python scripts/weather_snapshot_watcher.py            # default 60s interval
    uv run python scripts/weather_snapshot_watcher.py -i 120     # poll every 2 minutes
    uv run python scripts/weather_snapshot_watcher.py -l 157
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests

DEFAULT_BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
DEFAULT_LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "157"))
MAX_LIMIT = 2000
UA = "vlc-weather-watcher/2.0 (+github.com/jurdabos)"
FIELDS = "objectid,nombre,fiwareid,fecha_carg,viento_dir,viento_vel,temperatur,humedad_re,presion_ba,precipitac"


def query_url(base: str, layer_id: int) -> str:
    return f"{base.rstrip('/')}/{layer_id}/query"


def epoch_ms_to_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)


def _query(url: str, params: dict) -> dict:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": UA})
    r = s.get(url, params=params, timeout=(10, 60))
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict) and "error" in payload:
        raise SystemExit(f"ArcGIS error: {payload['error']}")
    return payload


def fetch_snapshot(url: str) -> List[dict]:
    rows: List[dict] = []
    offset = 0
    while True:
        data = _query(
            url,
            {
                "where": "1=1",
                "outFields": FIELDS,
                "returnGeometry": "false",
                "resultRecordCount": str(MAX_LIMIT),
                "resultOffset": str(offset),
                "f": "json",
            },
        )
        feats = data.get("features", []) or []
        if not feats:
            break
        rows.extend(f.get("attributes") or {} for f in feats)
        if len(feats) < MAX_LIMIT:
            break
        offset += MAX_LIMIT
    return rows


def fmt_num(x, d=1):
    if x is None:
        return "null"
    try:
        if float(x).is_integer() and d == 0:
            return f"{int(float(x))}"
        return f"{float(x):.{d}f}"
    except Exception:
        return str(x)


def station_key(row: dict) -> str:
    return row.get("fiwareid") or f"objectid:{row.get('objectid')}"


def short_name(full: str) -> str:
    if not full:
        return ""
    return full.replace("ESTACI\u00d3N", "EST.").replace("JARDINES DE ", "JD ")


def collect_ticks(rows: List[dict]) -> Tuple[datetime, List[datetime]]:
    ticks = [epoch_ms_to_utc(r["fecha_carg"]) for r in rows if r.get("fecha_carg") is not None]
    if not ticks:
        raise SystemExit("Snapshot rows lacked 'fecha_carg'.")
    return max(ticks), sorted(set(ticks))


def print_block(
    title: str,
    rows: List[dict],
    last_seen: Dict[str, datetime],
    show_only_updated: bool,
) -> None:
    header = (
        "objectid  fiwareid                 tick(UTC)            advanced  "
        "dir(\u00b0)  vel(m/s)  temp(\u00b0C)  hum(%)  press(hPa)  rain(mm)  name"
    )
    print("\n" + "=" * len(header))
    print(title)
    print(header)
    print("-" * len(header))
    for r in sorted(rows, key=lambda x: x.get("objectid", 0) or 0):
        fid = station_key(r)
        tick = epoch_ms_to_utc(r["fecha_carg"])
        prev = last_seen.get(fid)
        advanced = (prev is None) or (tick > prev)
        if show_only_updated and not advanced:
            continue
        line = (
            f"{str(r.get('objectid')).rjust(8)}  "
            f"{(r.get('fiwareid') or '').ljust(22)}  "
            f"{tick.strftime('%Y-%m-%d %H:%M:%S').ljust(20)}  "
            f"{'Y' if advanced else 'N':>8}  "
            f"{fmt_num(r.get('viento_dir'), 0).rjust(5)}  "
            f"{fmt_num(r.get('viento_vel'), 1).rjust(8)}  "
            f"{fmt_num(r.get('temperatur'), 1).rjust(8)}  "
            f"{fmt_num(r.get('humedad_re'), 0).rjust(6)}  "
            f"{fmt_num(r.get('presion_ba'), 1).rjust(10)}  "
            f"{fmt_num(r.get('precipitac'), 1).rjust(8)}  "
            f"{short_name(r.get('nombre', ''))}"
        )
        print(line)
    print("=" * len(header))


def main():
    ap = argparse.ArgumentParser(description="Watch weather snapshot; print station values on updates.")
    ap.add_argument("-b", "--base", default=DEFAULT_BASE)
    ap.add_argument("-l", "--layer-id", type=int, default=DEFAULT_LAYER_ID)
    ap.add_argument("-i", "--interval", type=int, default=60, help="Poll interval in seconds (default: 60).")
    args = ap.parse_args()
    url = query_url(args.base, args.layer_id)
    last_seen: Dict[str, datetime] = {}
    last_max_tick: datetime | None = None
    print("Starting weather snapshot watcher\u2026 (Ctrl+C to stop)")
    try:
        while True:
            try:
                rows = fetch_snapshot(url)
            except requests.RequestException as e:
                now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{now}] transient network error: {e}; retry next cycle\u2026")
                time.sleep(max(1, args.interval))
                continue
            if not rows:
                now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{now}] no rows; retry next cycle\u2026")
                time.sleep(max(1, args.interval))
                continue
            max_tick, tick_set = collect_ticks(rows)
            wall = datetime.now(timezone.utc).strftime("%H:%M:%S")
            if last_max_tick is None or max_tick > last_max_tick:
                if len(tick_set) > 1:
                    print(f"[warn] Multiple tick values in snapshot: {[t.isoformat() for t in tick_set]} (using max).")
                title = f"[{wall}] DATASET ADVANCE \u2192 max tick {max_tick.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                print_block(title, rows, last_seen, show_only_updated=False)
                for r in rows:
                    last_seen[station_key(r)] = epoch_ms_to_utc(r["fecha_carg"])
                last_max_tick = max_tick
            else:
                updated_rows = []
                for r in rows:
                    fid = station_key(r)
                    tick = epoch_ms_to_utc(r["fecha_carg"])
                    prev = last_seen.get(fid)
                    if prev is None or tick > prev:
                        updated_rows.append(r)
                if updated_rows:
                    title = (
                        f"[{wall}] PARTIAL ADVANCE \u2192 stations caught up to "
                        f"{max_tick.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    )
                    print_block(title, rows=updated_rows, last_seen=last_seen, show_only_updated=True)
                    for r in updated_rows:
                        last_seen[station_key(r)] = epoch_ms_to_utc(r["fecha_carg"])
            time.sleep(max(1, args.interval))
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
