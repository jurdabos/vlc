#!/usr/bin/env python3
"""
weather_frequency_check.py
Poll the Valencia Opendatasoft WEATHER snapshot and count how often the live tick advances.

- Prints a one-line status each poll.
- Persists last observed tick in ./state/weather_last_tick.txt so restarts "remember".
- Appends a CSV row on every tick advance to ./state/weather_ticks.csv.
- Robust HTTP (clamps limit, retries light 400s like your air script) and graceful Ctrl+C summary.

Usage:
  uv run python weather_frequency_check.py               # default 300s interval
  uv run python weather_frequency_check.py -i 120        # poll every 2 minutes
  uv run python weather_frequency_check.py -u estacions-atmosferiques-estaciones-atmosfericas
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import sys
import time
from datetime import datetime, timezone
from statistics import mean

import requests

# Defaults for the WEATHER dataset (5 stations, live snapshot)
DATASET_DEFAULT = "estacions-atmosferiques-estaciones-atmosfericas"
BASE = "https://valencia.opendatasoft.com/api/explore/v2.1"
MAX_LIMIT = 100  # Explore v2.1 hard cap; clamp just like our air checker.  :contentReference[oaicite:1]{index=1}

STATE_DIR = os.path.join(".", "state")
STATE_FILE = os.path.join(STATE_DIR, "weather_last_tick.txt")
CSV_FILE = os.path.join(STATE_DIR, "weather_ticks.csv")

UA = "vlc-weather-frequency/1.0 (+github.com/acidvuca)"


def build_url(spec: str) -> str:
    if spec.startswith(("http://", "https://")):
        return spec
    return f"{BASE}/catalog/datasets/{spec}/records"


def iso_to_utc(ts: str) -> datetime:
    """
    Parse ODS timestamp strings like '2025-10-19T13:50:00+00:00' to aware UTC.
    Strips subseconds if present (same fixup pattern as the air checker).  :contentReference[oaicite:2]{index=2}
    """
    ts = ts.replace("Z", "+00:00")
    if "." in ts:
        left, right = ts.split(".", 1)
        # preserve timezone portion if present after '.'
        if "+" in right or "-" in right:
            sign = "+" if "+" in right else "-"
            tz = sign + right.split(sign, 1)[1]
        else:
            tz = ""
        ts = left + tz
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def _get(url: str, params: dict, tolerate_400_fixups: bool = True) -> dict:
    """
    GET with niceties:
      - clamp limit to MAX_LIMIT,
      - retry once without 'order_by' on HTTP 400 (mirrors your air script).  :contentReference[oaicite:3]{index=3}
    """
    p = dict(params)
    try:
        p["limit"] = str(min(int(p.get("limit", MAX_LIMIT)), MAX_LIMIT))
    except Exception:
        p["limit"] = str(MAX_LIMIT)

    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": UA})
    r = s.get(url, params=p, timeout=(10, 60))
    if r.status_code == 400 and tolerate_400_fixups:
        p.pop("order_by", None)
        p["limit"] = str(MAX_LIMIT)
        r = s.get(url, params=p, timeout=(10, 60))

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        snippet = r.text[:600].replace("\n", " ")
        raise SystemExit(f"HTTP {r.status_code} {r.reason}. Params={p}. Payload head: {snippet}") from e

    try:
        return r.json()
    except json.JSONDecodeError:
        raise SystemExit(f"Non-JSON response. Params={p}. Payload head: {r.text[:600]}")


def fetch_total_count(url: str) -> int:
    data = _get(url, {"select": "count(*) as n", "limit": "1"})
    try:
        return int(data.get("results", [{}])[0].get("n", 0))
    except Exception:
        return 0


def fetch_all_rows(url: str) -> list[dict]:
    """
    Pull the whole snapshot with a lean projection; 5 rows expected for weather.  :contentReference[oaicite:4]{index=4}
    """
    total = fetch_total_count(url)
    if total == 0:
        return []
    pages = max(1, math.ceil(total / MAX_LIMIT))
    rows: list[dict] = []
    for i in range(pages):
        offset = i * MAX_LIMIT
        data = _get(
            url,
            {
                # Keep payload tiny; we only need these for cadence checks.
                "select": "fecha_carg,fiwareid",
                "limit": str(MAX_LIMIT),
                "offset": str(offset),
            },
        )
        rows.extend(data.get("results", []))
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
    ap.add_argument(
        "-u",
        "--url_or_dataset",
        default=DATASET_DEFAULT,
        help="Full records URL or dataset id. Default: estacions-atmosferiques-estaciones-atmosfericas",
    )
    ap.add_argument("-i", "--interval", type=int, default=300, help="Poll interval in seconds. Default: 300")
    args = ap.parse_args()
    url = build_url(args.url_or_dataset)
    # graceful Ctrl+C
    stop = {"flag": False}

    def _sigint(_sig, _frm):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sigint)
    # Warm-up: fetch one snapshot
    try:
        rows = fetch_all_rows(url)
    except requests.RequestException as e:
        raise SystemExit(f"[Network] {e}")
    if not rows:
        raise SystemExit("No rows returned (weather endpoint empty?).")
    # Snapshot semantics: all rows share the same fecha_carg; if not, pick the max and warn.
    ticks = {r.get("fecha_carg") for r in rows if r.get("fecha_carg")}
    if not ticks:
        raise SystemExit("Rows lacked 'fecha_carg'—cannot track frequency.")
    if len(ticks) > 1:
        sys.stderr.write(f"[warn] Multiple tick values in snapshot: {sorted(ticks)}; using max.\n")
    tick_str = max(ticks)
    tick_dt = iso_to_utc(tick_str)
    stations = len({r.get("fiwareid") for r in rows if r.get("fiwareid")})
    write_last_tick(tick_str)
    polls = 1
    advances = 0
    deltas_h: list[float] = []
    last_tick_dt = tick_dt
    start_wall = datetime.now(timezone.utc)
    start_msg = f"Started {start_wall.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    print(start_msg)
    print(f"Initial tick: {tick_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  (rows={len(rows)}, stations={stations})")
    # Main loop
    while not stop["flag"]:
        time.sleep(max(1, args.interval))
        polls += 1
        try:
            rows = fetch_all_rows(url)
        except Exception as e:
            now = datetime.now(timezone.utc)
            print(f"[{now.strftime('%H:%M:%S')}] transient error: {e}; retrying next cycle…")
            continue
        if not rows:
            now = datetime.now(timezone.utc)
            print(f"[{now.strftime('%H:%M:%S')}] no rows; retrying next cycle…")
            continue
        ticks = {r.get("fecha_carg") for r in rows if r.get("fecha_carg")}
        if not ticks:
            now = datetime.now(timezone.utc)
            print(f"[{now.strftime('%H:%M:%S')}] rows without 'fecha_carg'; retrying next cycle…")
            continue
        tick_str_now = max(ticks)
        tick_dt_now = iso_to_utc(tick_str_now)
        stations_now = len({r.get("fiwareid") for r in rows if r.get("fiwareid")})
        wall = datetime.now(timezone.utc)
        if tick_dt_now > last_tick_dt:
            gap_h = (tick_dt_now - last_tick_dt).total_seconds() / 3600.0
            advances += 1
            deltas_h.append(gap_h)
            print(
                f"[{wall.strftime('%H:%M:%S')}] tick advanced by {gap_h:.2f} h → "
                f"{tick_dt_now.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"(rows={len(rows)}, stations={stations_now}); advances={advances}"
            )
            write_last_tick(tick_str_now)
            append_csv_row(wall, tick_dt_now, len(rows), stations_now)
            last_tick_dt = tick_dt_now
        else:
            # unchanged
            print(
                f"[{wall.strftime('%H:%M:%S')}] same tick ({last_tick_dt.strftime('%H:%M:%S')} UTC); "
                f"polls={polls}, advances={advances}"
            )
    # Summary on Ctrl+C
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
