"""Prints a station x pollutant coverage table for the air-pollution layer
(geoportal ArcGIS REST, layer 156).
"""

import os

import requests

BASE = os.environ.get(
    "VLC_ARCGIS_BASE",
    "https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer",
)
LAYER_ID = int(os.environ.get("VLC_LAYER_ID", "156"))
URL = f"{BASE.rstrip('/')}/{LAYER_ID}/query"

params = {
    "where": "1=1",
    "outFields": "objectid,nombre,so2,no2,o3,co,pm10,pm25",
    "returnGeometry": "false",
    "resultRecordCount": "2000",
    "f": "json",
}
response = requests.get(URL, params=params, timeout=(10, 60))
response.raise_for_status()
payload = response.json()
if isinstance(payload, dict) and "error" in payload:
    raise SystemExit(f"ArcGIS error: {payload['error']}")

rows = [(f.get("attributes") or {}) for f in payload.get("features", []) or []]
pollutants = ["so2", "no2", "o3", "co", "pm10", "pm25"]
print("\nSUMMARY: Which stations report which pollutants")
print("=" * 80)
print(f"{'Station':<30} SO2  NO2  O3   CO   PM10 PM2.5")
print("-" * 80)
for record in sorted(rows, key=lambda x: (x.get("objectid") is None, x.get("objectid"))):
    name = (record.get("nombre") or "")[:28]
    row = f"{name:<30}"
    for pollutant in pollutants:
        value = record.get(pollutant)
        row += " \u2713   " if value is not None else " -   "
    print(row)
print("\n" + "=" * 80)
print("\nPollutant coverage:")
n_rows = len(rows) or 1
for pollutant in pollutants:
    count = sum(1 for r in rows if r.get(pollutant) is not None)
    pct = (count / n_rows) * 100
    print(f"  {pollutant.upper():<6}: {count:2}/{n_rows} stations ({pct:5.1f}%)")
print("\nKey insights (historic, may drift over time):")
print("\u2022 ALL 11 stations measure NO2 (nitrogen dioxide)")
print("\u2022 8 stations measure PM10 and PM2.5 (particulate matter)")
print("\u2022 6 stations measure O3 (ozone) and SO2 (sulfur dioxide)")
print("\u2022 Only 3 stations measure CO (carbon monoxide)")
print("\u2022 Weather data lives on the sibling layer 157, not here.")
