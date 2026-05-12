#!/usr/bin/env python3
"""Station Weather Metrics Report for the Valencia geoportal ArcGIS REST
weather layer (default id 157)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

DEFAULT_BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
DEFAULT_LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "157"))

DEFAULT_METRICS = [
    ("viento_dir", "Wind Dir", "\u00b0", 0),
    ("viento_vel", "Wind Spd", "m/s", 1),
    ("temperatur", "Temp", "\u00b0C", 1),
    ("humedad_re", "Humidity", "%", 0),
    ("presion_ba", "Pressure", "hPa", 1),
    ("precipitac", "Rain", "mm", 1),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print weather metrics per station from a Valencia ArcGIS layer.")
    p.add_argument("-b", "--base", default=DEFAULT_BASE)
    p.add_argument("-l", "--layer-id", type=int, default=DEFAULT_LAYER_ID)
    p.add_argument("-n", "--limit", type=int, default=2000)
    p.add_argument(
        "-m",
        "--metrics",
        default=",".join(k for k, *_ in DEFAULT_METRICS),
        help=(
            "Comma-separated field keys to print (labels/units fall back to key/blank). "
            f"Default: {','.join(k for k, *_ in DEFAULT_METRICS)}"
        ),
    )
    return p.parse_args()


def fetch_records(base: str, layer_id: int, limit: int, metrics: List[str]) -> Dict[str, Any]:
    url = f"{base.rstrip('/')}/{layer_id}/query"
    out_fields = ["objectid", "nombre", "fiwareid", "fecha_carg", "direccion"] + metrics
    params = {
        "where": "1=1",
        "outFields": ",".join(dict.fromkeys(out_fields)),
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": str(limit),
        "f": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=(10, 60))
        r.raise_for_status()
        payload = r.json()
        if isinstance(payload, dict) and "error" in payload:
            print(f"[ArcGIS] {payload['error']}", file=sys.stderr)
            sys.exit(1)
        return payload
    except requests.HTTPError as e:
        print(f"[HTTP] {e} \u2014 request was: {getattr(r, 'url', url)}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"[Network] {e}", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError:
        print("[Parse] Response was not valid JSON.", file=sys.stderr)
        sys.exit(3)


def extract_latlon(feat: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Extracts lat/lon from a feature's geometry (outSR=4326)."""
    geom = feat.get("geometry") or {}
    try:
        return float(geom["y"]), float(geom["x"])
    except (KeyError, TypeError, ValueError):
        return None, None


def metric_catalog() -> Dict[str, Tuple[str, str, int]]:
    """Maps field key -> (label, unit, decimals)."""
    return {k: (label, unit, dec) for k, label, unit, dec in DEFAULT_METRICS}


def fmt_value(v: Any, decimals: int, unit: str) -> str:
    if v is None:
        return "null"
    try:
        return f"{float(v):.{decimals}f} {unit}".strip()
    except (TypeError, ValueError):
        return str(v)


def fmt_ts(ts_ms: Any) -> str:
    if ts_ms is None:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return str(ts_ms)


def main() -> None:
    args = parse_args()
    requested = [s.strip() for s in args.metrics.split(",") if s.strip()]
    catalog = metric_catalog()
    data = fetch_records(args.base, args.layer_id, args.limit, requested)
    feats = data.get("features", []) or []
    print("Station Weather Metrics Report")
    print("=" * 80)
    print(f"Layer {args.layer_id} on {args.base}")
    print(f"Records returned: {len(feats)}")

    def sort_key(f: Dict[str, Any]):
        oid = (f.get("attributes") or {}).get("objectid")
        return (oid is None, oid)

    for feat in sorted(feats, key=sort_key):
        attrs = feat.get("attributes") or {}
        oid = attrs.get("objectid", "NA")
        name = attrs.get("nombre", "N/A")
        fid = attrs.get("fiwareid", "N/A")
        ts = fmt_ts(attrs.get("fecha_carg"))
        lat, lon = extract_latlon(feat)
        addr = attrs.get("direccion")
        print(f"\nObjectID {oid}: {name:<30} ({fid})")
        print(f"  Timestamp : {ts}")
        if lat is not None and lon is not None:
            print(f"  Location  : lat {lat:.6f}, lon {lon:.6f}")
        if addr not in (None, "", "None"):
            print(f"  Address   : {addr}")
        print("  Measurements:")
        for key in requested:
            label, unit, dec = catalog.get(key, (key, "", 1))
            print(f"    {label:<10}: {fmt_value(attrs.get(key), dec, unit)}")


if __name__ == "__main__":
    main()
