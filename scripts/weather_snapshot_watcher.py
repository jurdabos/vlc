#!/usr/bin/env python3
"""
weather_snapshot_watcher.py
Poll the Valencia WEATHER live snapshot and print station values *only* when updates land.

What it does
- Polls the Explore v2.1 records endpoint for the weather dataset (5-row live snapshot).
- Detects:
    1) A dataset-level advance: the *max* tick (fecha_carg) in the snapshot increases.
       → Prints a full block with all stations + marks which ones actually advanced.
    2) A partial advance: max tick unchanged, but some stations caught up to that max.
       → Prints a smaller block for those newly-advanced stations.
- Helps verify whether *each* station publishes fresh values (dir/vel/temp/hum/press/rain)
  at the new tick, or whether some rows lag.

Usage
  uv run python weather_snapshot_watcher.py             # default dataset + 60s interval
  uv run python weather_snapshot_watcher.py -i 120      # poll every 2 minutes
  uv run python weather_snapshot_watcher.py -u estacions-atmosferiques-estaciones-atmosfericas
  uv run python weather_snapshot_watcher.py -u https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-atmosferiques-estaciones-atmosfericas/records
"""

from __future__ import annotations
import argparse, json, math, os, sys, time
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import requests

BASE = "https://valencia.opendatasoft.com/api/explore/v2.1"
DATASET_DEFAULT = "estacions-atmosferiques-estaciones-atmosfericas"
MAX_LIMIT = 100
UA = "vlc-weather-watcher/1.0 (+github.com/acidvuca)"

FIELDS = (
    "objectid,nombre,fiwareid,fecha_carg,"
    "viento_dir,viento_vel,temperatur,humedad_re,presion_ba,precipitac,geo_point_2d"
)

def build_url(spec: str) -> str:
    if spec.startswith(("http://", "https://")):
        return spec
    return f"{BASE}/catalog/datasets/{spec}/records"

def iso_to_utc(ts: str) -> datetime:
    # Normalize e.g. "2025-10-19T13:50:00+00:00" and "…Z" to aware UTC
    ts = ts.replace("Z", "+00:00")
    if "." in ts:
        left, right = ts.split(".", 1)
        if "+" in right or "-" in right:
            sign = "+" if "+" in right else "-"
            tz = sign + right.split(sign, 1)[1]
        else:
            tz = ""
        ts = left + tz
    return datetime.fromisoformat(ts).astimezone(timezone.utc)

def _get(url: str, params: dict, tolerate_400_fixups: bool = True) -> dict:
    p = dict(params)
    try:
        p["limit"] = str(min(int(p.get("limit", MAX_LIMIT)), MAX_LIMIT))
    except Exception:
        p["limit"] = str(MAX_LIMIT)
    s = requests.Session()
    s.headers.update({"Accept":"application/json","User-Agent":UA})
    r = s.get(url, params=p, timeout=(10, 60))
    if r.status_code == 400 and tolerate_400_fixups:
        p.pop("order_by", None)
        p["limit"] = str(MAX_LIMIT)
        r = s.get(url, params=p, timeout=(10, 60))
    r.raise_for_status()
    try:
        return r.json()
    except json.JSONDecodeError:
        raise SystemExit(f"Non-JSON response. Payload head: {r.text[:600]}")

def fetch_snapshot(url: str) -> List[dict]:
    # count(*) first so we don’t miss rows if that ever changes from 5
    data = _get(url, {"select":"count(*) as n", "limit":"1"})
    total = int(data.get("results", [{}])[0].get("n", 0)) if data.get("results") else 0
    if total == 0:
        return []
    pages = max(1, math.ceil(total / MAX_LIMIT))
    rows: List[dict] = []
    for i in range(pages):
        off = i * MAX_LIMIT
        data = _get(url, {"select": FIELDS, "limit": str(MAX_LIMIT), "offset": str(off)})
        rows.extend(data.get("results", []))
    return rows

def fmt_num(x, d=1):
    if x is None:
        return "null"
    try:
        # integers should display cleanly (e.g., 110°), floats with 1 decimal
        if float(x).is_integer() and d == 0:
            return f"{int(float(x))}"
        return f"{float(x):.{d}f}"
    except Exception:
        return str(x)

def station_key(row: dict) -> str:
    # Prefer fiwareid, fallback to objectid
    return row.get("fiwareid") or f"objectid:{row.get('objectid')}"

def short_name(full: str) -> str:
    if not full:
        return ""
    # keep it compact for printing blocks
    return full.replace("ESTACIÓN", "EST.").replace("JARDINES DE ", "JD ")

def collect_ticks(rows: List[dict]) -> Tuple[datetime, List[datetime]]:
    ticks = []
    for r in rows:
        ts = r.get("fecha_carg")
        if ts:
            ticks.append(iso_to_utc(ts))
    if not ticks:
        raise SystemExit("Snapshot rows lacked 'fecha_carg'.")
    return max(ticks), sorted(set(ticks))

def print_block(
    title: str,
    rows: List[dict],
    last_seen: Dict[str, datetime],
    show_only_updated: bool,
) -> None:
    # Prepare a friendly table
    header = (
        "objectid  fiwareid                 tick(UTC)            advanced  "
        "dir(°)  vel(m/s)  temp(°C)  hum(%)  press(hPa)  rain(mm)  name"
    )
    print("\n" + "=" * len(header))
    print(title)
    print(header)
    print("-" * len(header))
    for r in sorted(rows, key=lambda x: (x.get("objectid", 0) or 0)):
        fid = station_key(r)
        tick = iso_to_utc(r["fecha_carg"])
        prev = last_seen.get(fid)
        advanced = (prev is None) or (tick > prev)
        if show_only_updated and not advanced:
            continue

        line = (
            f"{str(r.get('objectid')).rjust(8)}  "
            f"{(r.get('fiwareid') or '').ljust(22)}  "
            f"{tick.strftime('%Y-%m-%d %H:%M:%S').ljust(20)}  "
            f"{'Y' if advanced else 'N':>8}  "
            f"{fmt_num(r.get('viento_dir'),0).rjust(5)}  "
            f"{fmt_num(r.get('viento_vel'),1).rjust(8)}  "
            f"{fmt_num(r.get('temperatur'),1).rjust(8)}  "
            f"{fmt_num(r.get('humedad_re'),0).rjust(6)}  "
            f"{fmt_num(r.get('presion_ba'),1).rjust(10)}  "
            f"{fmt_num(r.get('precipitac'),1).rjust(8)}  "
            f"{short_name(r.get('nombre',''))}"
        )
        print(line)
    print("=" * len(header))

def main():
    ap = argparse.ArgumentParser(description="Watch weather snapshot; print station values on updates.")
    ap.add_argument("-u","--url_or_dataset", default=DATASET_DEFAULT,
                    help="Dataset id or full records URL.")
    ap.add_argument("-i","--interval", type=int, default=60,
                    help="Poll interval in seconds (default: 60).")
    args = ap.parse_args()

    url = build_url(args.url_or_dataset)

    # State: last seen tick per station + last max tick
    last_seen: Dict[str, datetime] = {}
    last_max_tick: datetime | None = None

    print("Starting weather snapshot watcher… (Ctrl+C to stop)")
    try:
        while True:
            try:
                rows = fetch_snapshot(url)
            except requests.RequestException as e:
                now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{now}] transient network error: {e}; retry next cycle…")
                time.sleep(max(1, args.interval))
                continue

            if not rows:
                now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{now}] no rows; retry next cycle…")
                time.sleep(max(1, args.interval))
                continue

            max_tick, tick_set = collect_ticks(rows)
            wall = datetime.now(timezone.utc).strftime("%H:%M:%S")

            # Dataset-level advance
            if last_max_tick is None or max_tick > last_max_tick:
                if len(tick_set) > 1:
                    print(f"[warn] Multiple tick values in snapshot: {[t.isoformat() for t in tick_set]} (using max).")
                title = f"[{wall}] DATASET ADVANCE → max tick {max_tick.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                print_block(title, rows, last_seen, show_only_updated=False)
                # update per-station last_seen
                for r in rows:
                    fid = station_key(r)
                    last_seen[fid] = iso_to_utc(r["fecha_carg"])
                last_max_tick = max_tick
            else:
                # No new max tick; detect stations that newly caught up (partial advance)
                updated_rows = []
                for r in rows:
                    fid = station_key(r)
                    tick = iso_to_utc(r["fecha_carg"])
                    prev = last_seen.get(fid)
                    if prev is None or tick > prev:
                        updated_rows.append(r)
                if updated_rows:
                    title = f"[{wall}] PARTIAL ADVANCE → stations caught up to {max_tick.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    print_block(title, rows=updated_rows, last_seen=last_seen, show_only_updated=True)
                    for r in updated_rows:
                        fid = station_key(r)
                        last_seen[fid] = iso_to_utc(r["fecha_carg"])

            time.sleep(max(1, args.interval))
    except KeyboardInterrupt:
        print("\nStopped by user.")

if __name__ == "__main__":
    main()
