#!/usr/bin/env python3
"""Finds the k closest features matching a filter criterion in a JSON dataset.

Extends the basic proximity search by adding field-based filtering. It accepts a JSON file containing
feature-like objects, filters them by a specified field and pattern, computes great-circle distances,
and writes the k closest matches to an output directory.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional, Tuple

# Default reference coordinates (València)
DEFAULT_LAT = 39.493804279841314
DEFAULT_LON = -0.4026670632153834

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Finds the k closest features matching a filter criterion in a JSON dataset. "
            "Writes results to the specified output directory."
        )
    )
    parser.add_argument("input_json", type=Path, help="Path to the input JSON file")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write output JSON file",
    )
    parser.add_argument(
        "--filter-field",
        type=str,
        help="Field name to filter on (e.g., 'nom_botanico')",
    )
    parser.add_argument(
        "--filter-pattern",
        type=str,
        help="Pattern to match in filter field (case-insensitive substring or regex)",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=10,
        help="Number of closest features to return (default: 10)",
    )
    parser.add_argument(
        "--lat",
        type=float,
        default=DEFAULT_LAT,
        help=f"Reference latitude (default: {DEFAULT_LAT})",
    )
    parser.add_argument(
        "--lon",
        type=float,
        default=DEFAULT_LON,
        help=f"Reference longitude (default: {DEFAULT_LON})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def extract_dataset_id(input_path: Path) -> str:
    """Extracts the dataset identifier from the file name."""
    stem = input_path.stem
    match = re.match(r"^(.*?)(?:_(?:full|metadata).*)$", stem, flags=re.IGNORECASE)
    return match.group(1) if match else stem


def coerce_float(value: Any) -> Optional[float]:
    """Coerces a value into float if possible."""
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return None
            return float(v)
        return None
    except (ValueError, TypeError):
        return None


def is_valid_lat_lon(lat: float, lon: float) -> bool:
    """Returns whether latitude and longitude are within valid bounds."""
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


def extract_lat_lon(item: dict, idx: int) -> Optional[Tuple[float, float]]:
    """Extracts latitude and longitude from a feature-like dict."""
    gp = item.get("geo_point_2d")
    if isinstance(gp, dict):
        lat = coerce_float(gp.get("lat"))
        lon = coerce_float(gp.get("lon"))
        if lat is not None and lon is not None and is_valid_lat_lon(lat, lon):
            return lat, lon
        logger.debug(f"Skipping entry at index {idx}: invalid lat/lon in geo_point_2d ({gp})")
        return None
    # Trying GeoJSON-like fallback
    geom = item.get("geometry")
    if isinstance(geom, dict):
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon = coerce_float(coords[0])
            lat = coerce_float(coords[1])
            if lat is not None and lon is not None and is_valid_lat_lon(lat, lon):
                return lat, lon
    logger.debug(f"Skipping entry at index {idx}: missing geo_point_2d")
    return None


def load_items(input_path: Path) -> list[dict]:
    """Loads and returns the list of items from the JSON file."""
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("results", "records", "data", "features", "items"):
            val = data.get(key)
            if isinstance(val, list):
                return val
        result = data.get("result")
        if isinstance(result, dict):
            records = result.get("records")
            if isinstance(records, list):
                return records
    raise ValueError(
        "Unsupported JSON structure. Expected a list or an object with a list under 'results', 'records', "
        "'data', 'features', or 'items'."
    )


def filter_items(items: list[dict], filter_field: Optional[str], filter_pattern: Optional[str]) -> list[dict]:
    """Filters items by field and pattern if specified."""
    if not filter_field or not filter_pattern:
        return items
    filtered = []
    pattern_lower = filter_pattern.lower()
    for item in items:
        value = item.get(filter_field)
        if value is None:
            continue
        value_str = str(value).lower()
        if pattern_lower in value_str:
            filtered.append(item)
    logger.info(f"Filtered {len(filtered)} items matching '{filter_field}' containing '{filter_pattern}'")
    return filtered


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Returns the great-circle distance in meters between two WGS84 points using the haversine formula."""
    r = 6371000.0  # to have mean Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def compute_closest(items: list[dict], ref_lat: float, ref_lon: float, k: int) -> list[dict]:
    """Returns the k closest items to the reference point, adding 'distance_meters' to each returned item."""
    annotated: list[dict] = []
    for idx, item in enumerate(items):
        latlon = extract_lat_lon(item, idx)
        if not latlon:
            continue
        lat, lon = latlon
        dist = haversine_meters(ref_lat, ref_lon, lat, lon)
        # Creating a shallow copy to avoid mutating the original
        new_item = dict(item)
        new_item["distance_meters"] = dist
        annotated.append(new_item)
    annotated.sort(key=lambda x: x["distance_meters"])
    return annotated[:k]


def build_output_path(output_dir: Path, dataset_id: str, filter_pattern: Optional[str], k: int) -> Path:
    """Builds the output file path with filter information in the filename."""
    filter_suffix = f"_{filter_pattern.replace(' ', '_')}" if filter_pattern else ""
    return output_dir / f"{dataset_id}{filter_suffix}_top{k}_closest.json"


def main() -> int:
    """Orchestrates argument parsing, data loading, filtering, distance computation, and result writing."""
    args = parse_args()
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO), format="%(levelname)s: %(message)s")
    input_path: Path = args.input_json
    output_dir: Path = args.output_dir
    # Validating paths before operations
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 2
    if not input_path.is_file():
        logger.error(f"Input path is not a file: {input_path}")
        return 2
    if not output_dir.exists():
        logger.error(f"Output directory not found: {output_dir}")
        return 2
    if not output_dir.is_dir():
        logger.error(f"Output path is not a directory: {output_dir}")
        return 2
    ref_lat: float = args.lat
    ref_lon: float = args.lon
    k: int = args.k
    if not is_valid_lat_lon(ref_lat, ref_lon):
        logger.error(f"Reference coordinates are out of bounds: lat={ref_lat}, lon={ref_lon}")
        return 2
    if k < 1:
        logger.error(f"k must be at least 1, got {k}")
        return 2
    try:
        items = load_items(input_path)
    except json.JSONDecodeError as exc:
        logger.error(f"Failed loading JSON: {exc}")
        return 2
    except Exception as exc:
        logger.error(f"Failed loading items: {exc}")
        return 2
    logger.info(f"Loaded {len(items)} items from {input_path}")
    # Filtering items
    filtered_items = filter_items(items, args.filter_field, args.filter_pattern)
    if not filtered_items:
        logger.warning("No items match the filter criteria")
        return 1
    dataset_id = extract_dataset_id(input_path)
    logger.info(f"Detected dataset_id: {dataset_id}")
    logger.info(f"Computing distances from ref lat={ref_lat}, lon={ref_lon} for {len(filtered_items)} items")
    top_k = compute_closest(filtered_items, ref_lat, ref_lon, k)
    if not top_k:
        logger.warning("No valid features with geo_point_2d found to compute distances")
        return 1
    logger.info(f"Selected {len(top_k)} closest features")
    out_path = build_output_path(output_dir, dataset_id, args.filter_pattern, k)
    payload = {
        "dataset_id": dataset_id,
        "filter_field": args.filter_field,
        "filter_pattern": args.filter_pattern,
        "reference_point": {"lat": ref_lat, "lon": ref_lon},
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "count": len(top_k),
        "features": top_k,
    }
    try:
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info(f"Wrote output JSON: {out_path}")
    except Exception as exc:
        logger.error(f"Failed writing output file: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
