"""LEGACY: lists every dataset_id available on the Valencia Opendatasoft
catalog (``valencia.opendatasoft.com``).

This host was decommissioned in 2026-05; the script is retained only as a
reference snapshot. The replacement source for the air and weather datasets
this project ingests is the geoportal ArcGIS REST endpoint described in
``producer/README.md``. Other Opendatasoft-only datasets (RVVCCA history,
noise sensors, etc.) have no automatic replacement.

Run with ``--allow-legacy`` to attempt the request anyway (it will fail with
a TLS handshake error against the dead host).
"""

import sys

if "--allow-legacy" not in sys.argv:
    print(
        "This script targets the decommissioned valencia.opendatasoft.com host. "
        "Pass --allow-legacy to attempt the request anyway."
    )
    sys.exit(0)

import requests

base = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets"
limit, offset, ids = 100, 0, set()
while True:
    r = requests.get(base, params={"limit": limit, "offset": offset})
    r.raise_for_status()
    res = r.json().get("results", [])
    if not res:
        break
    ids |= {x["dataset_id"] for x in res}
    offset += limit
print(f"Found {len(ids)} dataset_ids")
for i in sorted(ids):
    print(i)
