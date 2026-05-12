#!/usr/bin/env python3
"""Station Environmental Metrics Report for the Valencia geoportal
ArcGIS REST air-pollution layer (default id 156)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

import requests

DEFAULT_BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
DEFAULT_LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "156"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print pollutant availability per station.")
    p.add_argument("-b", "--base", default=DEFAULT_BASE)
    p.add_argument("-l", "--layer-id", type=int, default=DEFAULT_LAYER_ID)
    p.add_argument("-n", "--limit", type=int, default=2000)
    p.add_argument(
        "-p",
        "--pollutants",
        default="so2,no2,o3,co,pm10,pm25",
        help="Comma-separated pollutant fields to check.",
    )
    return p.parse_args()


def fetch_records(base: str, layer_id: int, limit: int, pollutants: List[str]) -> Dict[str, Any]:
    url = f"{base.rstrip('/')}/{layer_id}/query"
    out_fields = ["objectid", "nombre", "fiwareid", "parametros", "calidad_am"] + pollutants
    params = {
        "where": "1=1",
        "outFields": ",".join(dict.fromkeys(out_fields)),  # dedupe while keeping order
        "returnGeometry": "false",
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


def fmt_value(v: Any) -> str:
    """Formats a pollutant reading; tries float with 1 decimal, else raw string/null."""
    if v is None:
        return "null"
    try:
        return f"{float(v):6.1f} \u00b5g/m\u00b3"
    except (ValueError, TypeError):
        return str(v)


def main() -> None:
    args = parse_args()
    pollutants = [p.strip() for p in args.pollutants.split(",") if p.strip()]
    data = fetch_records(args.base, args.layer_id, args.limit, pollutants)
    rows: List[Dict[str, Any]] = [f.get("attributes") or {} for f in data.get("features", []) or []]
    print("Station Environmental Metrics Report")
    print("=" * 80)
    print(f"Layer {args.layer_id} on {args.base}")
    print(f"Records returned: {len(rows)}")
    rows.sort(key=lambda x: (x.get("objectid") is None, x.get("objectid")))
    for record in rows:
        oid = record.get("objectid", "NA")
        name = record.get("nombre", "N/A")
        fid = record.get("fiwareid", "N/A")
        print(f"\nObjectID {oid}: {name:<25} ({fid})")
        print("  Measurements:")
        for pollutant in pollutants:
            print(f"    {pollutant.upper():<6}: {fmt_value(record.get(pollutant))}")
        params_str = record.get("parametros", "") or ""
        if params_str:
            if len(params_str) > 60:
                print(f"  Declared parameters: {params_str[:60]}...")
            else:
                print(f"  Declared parameters: {params_str}")
        print(f"  Air Quality: {record.get('calidad_am', 'N/A')}")


if __name__ == "__main__":
    main()
