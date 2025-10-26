import requests
import re

BASE = "https://valencia.opendatasoft.com/api/explore/v2.1"
DATASET = "estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas"
IDS = set(range(12, 23))
LIMIT = 100


def extract_lon_lat(geo):
    """
    geo_point_2d can be a dict: {"lon": ..., "lat": ...}
    or (rarely) a WKT string like "POINT (lon lat)". Handle both.
    """
    if isinstance(geo, dict):
        lon = geo.get("lon")
        lat = geo.get("lat")
        return (float(lon), float(lat)) if lon is not None and lat is not None else (None, None)
    if isinstance(geo, str):
        m = re.search(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", geo)
        if m:
            return float(m.group(1)), float(m.group(2))
    return (None, None)


def fetch_records():
    params = {
        "limit": LIMIT,
        "offset": 0,
        # Narrow to the ID range server-side; we’ll still filter precisely client-side
        "where": "objectid >= 12 AND objectid <= 22",
        "order_by": "objectid"
    }
    while True:
        r = requests.get(f"{BASE}/catalog/datasets/{DATASET}/records", params=params, timeout=(10, 60))
        r.raise_for_status()
        page = r.json().get("results", [])
        if not page:
            break
        for rec in page:
            yield rec
        if len(page) < LIMIT:
            break
        params["offset"] += LIMIT


# Collect (objectid, fiwareid, lon, lat) and print as "fiwareid: lon, lat"
rows = []
for rec in fetch_records():
    oid = rec.get("objectid")
    if oid in IDS:
        lon, lat = extract_lon_lat(rec.get("geo_point_2d"))
        fiwareid = rec.get("fiwareid")
        if fiwareid and lon is not None and lat is not None:
            rows.append((oid, fiwareid, lon, lat))
# Sort by objectid (stable, predictable output)
rows.sort(key=lambda x: x[0])
for _, fiwareid, lon, lat in rows:
    print(f"{fiwareid}: {lat}, {lon}")
