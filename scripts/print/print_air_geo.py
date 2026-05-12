"""Prints `fiwareid: lat, lon` for the air-pollution stations whose objectid
falls in the range 12..22 (the historical objectid range for layer 156).

Source: Valencia geoportal ArcGIS REST, layer 156. With ``outSR=4326`` the
feature geometry comes back as ``{x: lon, y: lat}``.
"""

import os

import requests

BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "156"))
URL = f"{BASE.rstrip('/')}/{LAYER_ID}/query"


def fetch_features():
    """Yields features whose objectid is in the configured range."""
    params = {
        "where": "objectid >= 12 AND objectid <= 22",
        "outFields": "objectid,fiwareid",
        "returnGeometry": "true",
        "outSR": "4326",
        "orderByFields": "objectid",
        "resultRecordCount": "2000",
        "f": "json",
    }
    r = requests.get(URL, params=params, timeout=(10, 60))
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict) and "error" in payload:
        raise SystemExit(f"ArcGIS error: {payload['error']}")
    yield from payload.get("features", []) or []


rows = []
for feat in fetch_features():
    attrs = feat.get("attributes") or {}
    geom = feat.get("geometry") or {}
    fiwareid = attrs.get("fiwareid")
    if fiwareid and "x" in geom and "y" in geom:
        # outSR=4326 -> x=lon, y=lat
        rows.append((attrs.get("objectid"), fiwareid, float(geom["x"]), float(geom["y"])))

# Sort by objectid (stable, predictable output)
rows.sort(key=lambda x: (x[0] is None, x[0]))
for _, fiwareid, lon, lat in rows:
    print(f"{fiwareid}: {lat}, {lon}")
