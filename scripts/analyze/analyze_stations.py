#!/usr/bin/env python3
"""Prints station info from a Valencia geoportal ArcGIS REST layer.

Defaults to the air-pollution layer (id 156). Use ``-l 157`` for weather.
"""

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
    p = argparse.ArgumentParser(description="Print station info from a Valencia ArcGIS layer.")
    p.add_argument(
        "-b",
        "--base",
        default=DEFAULT_BASE,
        help=f"MapServer base URL. Default: {DEFAULT_BASE}",
    )
    p.add_argument(
        "-l",
        "--layer-id",
        type=int,
        default=DEFAULT_LAYER_ID,
        help=f"Layer id (156 = air, 157 = weather). Default: {DEFAULT_LAYER_ID}",
    )
    p.add_argument(
        "-n",
        "--limit",
        type=int,
        default=2000,
        help="resultRecordCount per page (capped at the layer's maxRecordCount). Default: 2000.",
    )
    return p.parse_args()


def fetch_records(base: str, layer_id: int, limit: int) -> Dict[str, Any]:
    url = f"{base.rstrip('/')}/{layer_id}/query"
    params = {
        "where": "1=1",
        "outFields": "objectid,nombre,fiwareid,direccion",
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
        where = getattr(r, "url", url)
        print(f"[HTTP] {e} \u2014 request was: {where}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"[Network] {e}", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError:
        print("[Parse] Response was not valid JSON.", file=sys.stderr)
        sys.exit(3)


def main() -> None:
    args = parse_args()
    data = fetch_records(args.base, args.layer_id, args.limit)
    feats = data.get("features", []) or []
    rows: List[Dict[str, Any]] = [f.get("attributes") or {} for f in feats]
    print(f"Layer {args.layer_id} on {args.base}")
    print(f"Records returned: {len(rows)}")
    rows.sort(key=lambda x: (x.get("objectid") is None, x.get("objectid")))
    print(f"\nUnique stations: {len(rows)}")
    print("\nStation details:")
    print("-" * 60)
    for s in rows:
        oid = s.get("objectid")
        print(
            f"ObjectID {oid if oid is not None else 'NA':>2}: {s.get('nombre', 'N/A'):<24} ({s.get('fiwareid', 'N/A')})"
        )
        addr = s.get("direccion")
        if addr:
            print(f"           Address: {addr}")
    print("\nObject IDs list:", [s.get("objectid") for s in rows])


if __name__ == "__main__":
    main()
