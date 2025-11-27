#!/usr/bin/env python3
"""Station Weather Metrics Report for Valencia Opendatasoft datasets."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

# Default to the WEATHER dataset (you can still override with -u).
DEFAULT_URL = (
    "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "estacions-atmosferiques-estaciones-atmosfericas/records"
)
ODS_DATASETS_BASE = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets"

# Default weather fields (observed in this dataset)
DEFAULT_METRICS = [
    ("viento_dir", "Wind Dir", "°", 0),
    ("viento_vel", "Wind Spd", "m/s", 1),
    ("temperatur", "Temp", "°C", 1),
    ("humedad_re", "Humidity", "%", 0),
    ("presion_ba", "Pressure", "hPa", 1),
    ("precipitac", "Rain", "mm", 1),
]


def expand_url(value: str) -> str:
    """Accept a full URL or a dataset id and return a records URL."""
    if value.startswith(("http://", "https://")):
        return value
    return f"{ODS_DATASETS_BASE}/{value}/records"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print weather metrics per station from a Valencia ODS dataset.")
    p.add_argument(
        "-u",
        "--url",
        default=DEFAULT_URL,
        help=(
            "Full '.../records' URL OR just a dataset id to expand. "
            "Default: estacions-atmosferiques-estaciones-atmosfericas"
        ),
    )
    p.add_argument(
        "-l",
        "--limit",
        type=int,
        default=100,
        help="Records to request (per page). Default: 100.",
    )
    p.add_argument(
        "-m",
        "--metrics",
        default=",".join(k for k, *_ in DEFAULT_METRICS),
        help=(
            "Comma-separated field keys to print (labels/units will fall back to key/blank). "
            f"Default: {','.join(k for k, *_ in DEFAULT_METRICS)}"
        ),
    )
    return p.parse_args()


def fetch_records(url: str, limit: int) -> Dict[str, Any]:
    try:
        r = requests.get(url, params={"limit": str(limit)}, timeout=(10, 60))
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        where = getattr(r, "url", url)
        print(f"[HTTP] {e} — request was: {where}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"[Network] {e}", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError:
        print("[Parse] Response was not valid JSON.", file=sys.stderr)
        sys.exit(3)


def extract_latlon(rec: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    gp = rec.get("geo_point_2d")
    if isinstance(gp, dict) and "lat" in gp and "lon" in gp:
        return float(gp["lat"]), float(gp["lon"])
    gs = rec.get("geo_shape", {})
    geom = gs.get("geometry", {})
    if geom.get("type") == "Point":
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            lon, lat = coords
            try:
                return float(lat), float(lon)
            except (TypeError, ValueError):
                return None, None
    return None, None


def metric_catalog() -> Dict[str, Tuple[str, str, int]]:
    """Map field key -> (label, unit, decimals)."""
    return {k: (label, unit, dec) for k, label, unit, dec in DEFAULT_METRICS}


def fmt_value(v: Any, decimals: int, unit: str) -> str:
    if v is None:
        return "null"
    try:
        f = float(v)
        return f"{f:.{decimals}f} {unit}".strip()
    except (TypeError, ValueError):
        return str(v)


def main() -> None:
    args = parse_args()
    url = expand_url(args.url)
    requested = [s.strip() for s in args.metrics.split(",") if s.strip()]
    catalog = metric_catalog()

    data = fetch_records(url, args.limit)
    results: List[Dict[str, Any]] = data.get("results", [])
    total_count = data.get("total_count", len(results))

    print("Station Weather Metrics Report")
    print("=" * 80)
    print(f"Total count from API: {total_count}")
    print(f"Records returned: {len(results)}")

    # Sort safely even if objectid is absent
    def sort_key(x: Dict[str, Any]):
        oid = x.get("objectid")
        return (oid is None, oid)

    for record in sorted(results, key=sort_key):
        oid = record.get("objectid", "NA")
        name = record.get("nombre", "N/A")
        fid = record.get("fiwareid", "N/A")
        ts = record.get("fecha_carg", "N/A")
        lat, lon = extract_latlon(record)
        addr = record.get("direccion")

        print(f"\nObjectID {oid}: {name:<30} ({fid})")
        print(f"  Timestamp : {ts}")
        if lat is not None and lon is not None:
            print(f"  Location  : lat {lat:.6f}, lon {lon:.6f}")
        if addr not in (None, "", "None"):
            print(f"  Address   : {addr}")

        print("  Measurements:")
        for key in requested:
            label, unit, dec = catalog.get(key, (key, "", 1))
            val = record.get(key)
            print(f"    {label:<10}: {fmt_value(val, dec, unit)}")


if __name__ == "__main__":
    main()
