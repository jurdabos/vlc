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
