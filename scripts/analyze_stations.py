#!/usr/bin/env python3

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
ODS_DATASETS_BASE = (
    "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets"
)


def expand_url(value: str) -> str:
    """Accept a full URL or a dataset id and return a records URL."""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    # Treat as dataset id; build the standard /records endpoint
    return f"{ODS_DATASETS_BASE}/{value}/records"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print station info from a Valencia Opendatasoft dataset."
    )
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


def main() -> None:
    args = parse_args()
    url = expand_url(args.url)
    data = fetch_records(url, args.limit)
    total_count = data.get("total_count", 0)
    results: List[Dict[str, Any]] = data.get("results", [])
    print(f"Total count from API: {total_count}")
    print(f"Records returned: {len(results)}")
    stations = []
    for r in results:
        stations.append(
            {
                "objectid": r.get("objectid"),
                "nombre": r.get("nombre", "N/A"),
                "fiwareid": r.get("fiwareid", "N/A"),
                "direccion": r.get("direccion", "N/A"),
            }
        )
    # Sort safely even if objectid is missing
    stations.sort(key=lambda x: (x["objectid"] is None, x["objectid"]))
    print(f"\nUnique stations: {len(stations)}")
    print("\nStation details:")
    print("-" * 60)
    for s in stations:
        oid = s["objectid"]
        print(f"ObjectID {oid if oid is not None else 'NA':>2}: {s['nombre']:<20} ({s['fiwareid']})")
        print(f"           Address: {s['direccion']}")
    print("\nObject IDs list:", [s["objectid"] for s in stations])


if __name__ == "__main__":
    main()
