#!/usr/bin/env python3
"""Station Environmental Metrics Report for Valencia Opendatasoft datasets."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List

import requests

DEFAULT_URL = (
    "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records"
)
ODS_DATASETS_BASE = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets"


def expand_url(value: str) -> str:
    """Accept a full URL or a dataset id and return a records URL."""
    if value.startswith(("http://", "https://")):
        return value
    return f"{ODS_DATASETS_BASE}/{value}/records"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print pollutant availability per station from a Valencia ODS dataset.")
    p.add_argument(
        "-u",
        "--url",
        default=DEFAULT_URL,
        help=(
            "Full '.../records' URL OR just a dataset id to expand. "
            "Defaults to the official air-quality stations dataset."
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
        "-p",
        "--pollutants",
        default="so2,no2,o3,co,pm10,pm25",
        help="Comma-separated pollutant fields to check. Default: so2,no2,o3,co,pm10,pm25",
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


def fmt_value(v: Any) -> str:
    """Format a pollutant reading; try float with 1 decimal, else raw string/null."""
    if v is None:
        return "null"
    try:
        f = float(v)
        return f"{f:6.1f} µg/m³"
    except (ValueError, TypeError):
        return str(v)


def main() -> None:
    args = parse_args()
    url = expand_url(args.url)
    pollutants = [p.strip() for p in args.pollutants.split(",") if p.strip()]
    data = fetch_records(url, args.limit)
    results: List[Dict[str, Any]] = data.get("results", [])
    total_count = data.get("total_count", len(results))
    print("Station Environmental Metrics Report")
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
        print(f"\nObjectID {oid}: {name:<25} ({fid})")
        print("  Measurements:")
        for pollutant in pollutants:
            value = record.get(pollutant)
            print(f"    {pollutant.upper():<6}: {fmt_value(value)}")
        params_str = record.get("parametros", "")
        if params_str:
            if len(params_str) > 60:
                print(f"  Declared parameters: {params_str[:60]}...")
            else:
                print(f"  Declared parameters: {params_str}")
        quality = record.get("calidad_am", "N/A")
        print(f"  Air Quality: {quality}")


if __name__ == "__main__":
    main()
