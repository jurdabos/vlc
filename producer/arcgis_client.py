"""ArcGIS REST `query` client used by the air and weather producers.

Encapsulates the small protocol-specific bits so air_producer.py and
weather_producer.py can share fetch + paging + metadata logic.

Source endpoint:
    https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer/{LAYER_ID}

Layers:
    156 -> Estacions contaminació atmosfèriques (air pollution)
    157 -> Estacions Atmosfèriques (weather)

Each row has the same attribute names the legacy Opendatasoft endpoint exposed
(`fecha_carg`, `fiwareid`, `so2`, ..., `viento_dir`, ...), so downstream
producer logic only changes in how rows are fetched and unpacked.

ArcGIS quirks worth knowing:
    - `fecha_carg` is `esriFieldTypeDate`, returned as epoch milliseconds.
    - `feature.geometry` is `{x: lon, y: lat}` once we ask for `outSR=4326`.
    - Per-layer page cap is `maxRecordCount` (2000 for these layers); we
      paginate via `resultOffset`/`resultRecordCount`.
"""

from typing import Any, Dict, List, Optional

import requests
from resilience import RetryConfig, http_request_with_retry

DEFAULT_BASE = "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer"


def layer_url(base: str, layer_id: int) -> str:
    """Returns the absolute URL for a specific MapServer layer."""
    return f"{base.rstrip('/')}/{layer_id}"


def query_url(base: str, layer_id: int) -> str:
    """Returns the absolute URL for a layer's `/query` endpoint."""
    return f"{layer_url(base, layer_id)}/query"


def get_layer_metadata(
    session: requests.Session,
    base: str,
    layer_id: int,
    config: Optional[RetryConfig] = None,
) -> Optional[Dict[str, Any]]:
    """Fetches a layer's metadata document (fields, geometryType, etc.).

    Returns None on persistent failure so callers can fall back gracefully.
    """
    try:
        r = http_request_with_retry(session, "GET", layer_url(base, layer_id), config=config, params={"f": "json"})
        if r.ok:
            payload = r.json()
            # ArcGIS reports protocol-level errors with HTTP 200 + {"error": {...}}
            if isinstance(payload, dict) and "error" in payload:
                return None
            return payload
    except Exception:
        pass
    return None


def get_field_names(meta: Dict[str, Any]) -> List[str]:
    """Returns the list of attribute field names from a layer metadata doc."""
    try:
        return [f["name"] for f in meta.get("fields", []) if "name" in f]
    except Exception:
        return []


def fetch_one_feature(
    session: requests.Session,
    base: str,
    layer_id: int,
    config: Optional[RetryConfig] = None,
) -> Optional[Dict[str, Any]]:
    """Fetches a single feature so callers can infer fields from a sample."""
    try:
        r = http_request_with_retry(
            session,
            "GET",
            query_url(base, layer_id),
            config=config,
            params={
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "false",
                "resultRecordCount": "1",
                "f": "json",
            },
        )
        r.raise_for_status()
        feats = r.json().get("features", [])
        if not feats:
            return None
        attrs = dict(feats[0].get("attributes") or {})
        return attrs
    except Exception:
        return None


def fetch_page(
    session: requests.Session,
    base: str,
    layer_id: int,
    *,
    out_fields: str,
    ts_field: str,
    limit: int,
    offset: int,
    config: Optional[RetryConfig] = None,
) -> List[Dict[str, Any]]:
    """Returns a page of features with merged geometry (or raises on error).

    The returned list contains one dict per feature, where each dict is
    `feature["attributes"]` extended with `lat` / `lon` derived from
    `feature["geometry"]` (when geometry is present and SR is 4326).
    """
    params = {
        "where": "1=1",
        "outFields": out_fields,
        "returnGeometry": "true",
        "outSR": "4326",
        "orderByFields": f"{ts_field} DESC",
        "resultRecordCount": str(limit),
        "resultOffset": str(offset),
        "f": "json",
    }
    r = http_request_with_retry(session, "GET", query_url(base, layer_id), config=config, params=params)
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict) and "error" in payload:
        # ArcGIS returns 200 OK + an `error` envelope for protocol-level errors
        raise RuntimeError(f"ArcGIS error: {payload['error']}")
    rows: List[Dict[str, Any]] = []
    for feat in payload.get("features", []) or []:
        attrs = dict(feat.get("attributes") or {})
        geom = feat.get("geometry") or {}
        # With outSR=4326, x is longitude and y is latitude
        if "x" in geom and "y" in geom:
            try:
                attrs["lon"] = float(geom["x"])
                attrs["lat"] = float(geom["y"])
            except (TypeError, ValueError):
                pass
        rows.append(attrs)
    return rows
