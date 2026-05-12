#!/usr/bin/env python3
"""weather_frequency_check.py

Polls the Valencia geoportal ArcGIS REST weather snapshot (layer 157 by
default) and counts how often the live tick advances.

- Prints a one-line status each poll.
- Persists last observed tick to ./state/weather_last_tick.txt across runs.
- Appends a CSV row on every tick advance to ./state/weather_ticks.csv.
- Tolerates transient HTTP errors and prints a graceful Ctrl+C summary.

Usage:
    uv run python scripts/weather_frequency_check.py            # default 300s interval
    uv run python scripts/weather_frequency_check.py -i 120     # poll every 2 minutes
    uv run python scripts/weather_frequency_check.py -l 157     # explicit layer id
"""

from __future__ import annotations

import argparse
import csv
import os
import signal
import sys
import time
from datetime import datetime, timezone
from statistics import mean

import requests

DEFAULT_BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
DEFAULT_LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "157"))
MAX_LIMIT = 2000
STATE_DIR = os.path.join(".", "state")
STATE_FILE = os.path.join(STATE_DIR, "weather_last_tick.txt")
CSV_FILE = os.path.join(STATE_DIR, "weather_ticks.csv")
UA = "vlc-weather-frequency/2.0 (+github.com/jurdabos)"


def query_url(base: str, layer_id: int) -> str:
    return f"{base.rstrip('/')}/{layer_id}/query"


def epoch_ms_to_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)


def _query(url: str, params: dict) -> dict:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": UA})
    r = s.get(url, params=params, timeout=(10, 60))
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        snippet = r.text[:600].replace("\n", " ")
        raise SystemExit(f"HTTP {r.status_code} {r.reason}. Params={params}. Payload head: {snippet}") from e
    payload = r.json()
    if isinstance(payload, dict) and "error" in payload:
        raise SystemExit(f"ArcGIS error: {payload['error']}")
    return payload


def fetch_all_rows(url: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        data = _query(
            url,
            {
                "where": "1=1",
                "outFields": "fecha_carg,fiwareid",
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


def ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)


def read_last_tick() -> str | None:
    if os.path.exists(STATE_FILE):
        s = open(STATE_FILE, "r", encoding="utf-8").read().strip()
        return s or None
    return None


def write_last_tick(tick: str) -> None:
    ensure_state_dir()
    open(STATE_FILE, "w", encoding="utf-8").write(tick)


def append_csv_row(wall_utc: datetime, tick_utc: datetime, rows: int, stations: int) -> None:
    ensure_state_dir()
    existed = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not existed:
            w.writerow(["wall_time_utc", "tick_utc", "rows", "stations"])
        w.writerow([wall_utc.isoformat(), tick_utc.isoformat(), rows, stations])


def summarize_deltas(deltas_h: list[float]) -> str:
    if not deltas_h:
        return "no advances observed"
    return (
        f"{len(deltas_h)} advances | mean {mean(deltas_h):.2f} h | "
        f"min {min(deltas_h):.2f} h | max {max(deltas_h):.2f} h"
    )


def main():
    ap = argparse.ArgumentParser(description="Count live-tick advances for the WEATHER snapshot.")
    ap.add_argument("-b", "--base", default=DEFAULT_BASE)
    ap.add_argument("-l", "--layer-id", type=int, default=DEFAULT_LAYER_ID)
    ap.add_argument("-i", "--interval", type=int, default=300, help="Poll interval in seconds. Default: 300")
    args = ap.parse_args()
    url = query_url(args.base, args.layer_id)
    stop = {"flag": False}

    def _sigint(_sig, _frm):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sigint)

    try:
        rows = fetch_all_rows(url)
    except requests.RequestException as e:
        raise SystemExit(f"[Network] {e}")
    if not rows:
        raise SystemExit("No rows returned (weather endpoint empty?).")
    ticks = [r.get("fecha_carg") for r in rows if r.get("fecha_carg") is not None]
    if not ticks:
        raise SystemExit("Rows lacked 'fecha_carg' (epoch ms).")
    if len(set(ticks)) > 1:
        sys.stderr.write(f"[warn] Multiple tick values in snapshot: {sorted(set(ticks))}; using max.\n")
    tick_dt = epoch_ms_to_utc(max(ticks))
    stations = len({r.get("fiwareid") for r in rows if r.get("fiwareid")})
    write_last_tick(tick_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    polls = 1
    advances = 0
    deltas_h: list[float] = []
    last_tick_dt = tick_dt
    start_wall = datetime.now(timezone.utc)
    print(f"Started {start_wall.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Initial tick: {tick_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  (rows={len(rows)}, stations={stations})")

    while not stop["flag"]:
        time.sleep(max(1, args.interval))
        polls += 1
        try:
            rows = fetch_all_rows(url)
        except Exception as e:
            now = datetime.now(timezone.utc)
            print(f"[{now.strftime('%H:%M:%S')}] transient error: {e}; retrying next cycle\u2026")
            continue
        if not rows:
            now = datetime.now(timezone.utc)
            print(f"[{now.strftime('%H:%M:%S')}] no rows; retrying next cycle\u2026")
            continue
        ticks_now = [r.get("fecha_carg") for r in rows if r.get("fecha_carg") is not None]
        if not ticks_now:
            now = datetime.now(timezone.utc)
            print(f"[{now.strftime('%H:%M:%S')}] rows without 'fecha_carg'; retrying next cycle\u2026")
            continue
        tick_dt_now = epoch_ms_to_utc(max(ticks_now))
        stations_now = len({r.get("fiwareid") for r in rows if r.get("fiwareid")})
        wall = datetime.now(timezone.utc)
        if tick_dt_now > last_tick_dt:
            gap_h = (tick_dt_now - last_tick_dt).total_seconds() / 3600.0
            advances += 1
            deltas_h.append(gap_h)
            print(
                f"[{wall.strftime('%H:%M:%S')}] tick advanced by {gap_h:.2f} h \u2192 "
                f"{tick_dt_now.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"(rows={len(rows)}, stations={stations_now}); advances={advances}"
            )
            write_last_tick(tick_dt_now.strftime("%Y-%m-%dT%H:%M:%SZ"))
            append_csv_row(wall, tick_dt_now, len(rows), stations_now)
            last_tick_dt = tick_dt_now
        else:
            print(
                f"[{wall.strftime('%H:%M:%S')}] same tick ({last_tick_dt.strftime('%H:%M:%S')} UTC); "
                f"polls={polls}, advances={advances}"
            )

    stop_wall = datetime.now(timezone.utc)
    elapsed_h = (stop_wall - start_wall).total_seconds() / 3600.0
    print("\n" + "=" * 80)
    print("Session summary")
    print(
        f"  Duration        : {elapsed_h:.2f} h "
        f"(from {start_wall.strftime('%Y-%m-%d %H:%M:%S')} to {stop_wall.strftime('%Y-%m-%d %H:%M:%S')} UTC)"
    )
    print(f"  Polls / Advances: {polls} / {advances}")
    print(f"  Last tick       : {last_tick_dt.isoformat()}")
    print(f"  Tick gaps (h)   : {summarize_deltas(deltas_h)}")
    print(f"  State file      : {os.path.abspath(STATE_FILE)}")
    print(f"  CSV (advances)  : {os.path.abspath(CSV_FILE)}")
    print("=" * 80)


if __name__ == "__main__":
    main()
