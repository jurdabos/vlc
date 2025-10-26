# check_frequency.py
import os, json, math, requests
from datetime import datetime, timezone

DATASET = "estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas"
BASE = "https://valencia.opendatasoft.com/api/explore/v2.1"
URL  = f"{BASE}/catalog/datasets/{DATASET}/records"

MAX_LIMIT = 100  # Opendatasoft hard cap for Explore v2.1
STATE = os.environ.get("STATE_FILE", os.path.join(".", "state", "last_tick.txt"))


def iso(ts: str):
    ts = ts.replace("Z", "+00:00")
    if "." in ts:  # strip subseconds but keep tz
        left, right = ts.split(".", 1)
        if "+" in right or "-" in right:
            sign = "+" if "+" in right else "-"
            tz = sign + right.split(sign, 1)[1]
        else:
            tz = ""
        ts = left + tz
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def _get(params, *, tolerate_400_fixups=True):
    """
    Do a GET with common fixups:
      - clamp limit to MAX_LIMIT
      - remove order_by on 400
    """
    params = dict(params)
    if "limit" in params:
        try:
            params["limit"] = str(min(int(params["limit"]), MAX_LIMIT))
        except Exception:
            params["limit"] = str(MAX_LIMIT)
    else:
        params["limit"] = str(MAX_LIMIT)
    r = requests.get(URL, params=params, headers={"Accept":"application/json"}, timeout=(10,60))
    if r.status_code == 400 and tolerate_400_fixups:
        # Try removing order_by and clamping limit again
        params.pop("order_by", None)
        params["limit"] = str(MAX_LIMIT)
        r = requests.get(URL, params=params, headers={"Accept":"application/json"}, timeout=(10,60))
    # If still bad, show diagnostic snippet
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        snippet = r.text[:600].replace("\n"," ")
        raise SystemExit(f"HTTP {r.status_code} {r.reason}. Params={params}. Payload head: {snippet}") from e
    try:
        return r.json()
    except json.JSONDecodeError:
        raise SystemExit(f"Non-JSON response. Params={params}. Payload head: {r.text[:600]}")


def fetch_total_count():
    data = _get({"select":"count(*) as n"})
    n = 0
    if isinstance(data, dict) and "results" in data and data["results"]:
        n = int(data["results"][0].get("n", 0))
    return n


def fetch_all_rows():
    total = fetch_total_count()
    if total == 0:
        return []
    pages = max(1, math.ceil(total / MAX_LIMIT))
    rows = []
    for i in range(pages):
        offset = i * MAX_LIMIT
        data = _get({
            "select": "fecha_carg,fiwareid",  # lean payload
            "limit": str(MAX_LIMIT),
            "offset": str(offset),
        })
        rows.extend(data.get("results", []))
    return rows


if __name__ == "__main__":
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    rows = fetch_all_rows()
    if not rows:
        raise SystemExit("No rows returned.")
    # snapshot semantics: all rows share the same fecha_carg
    tick = rows[0]["fecha_carg"]
    now_dt = iso(tick)
    stations = {r.get("fiwareid") for r in rows if r.get("fiwareid")}
    print(f"Snapshot tick: {now_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  (rows={len(rows)}, stations={len(stations)})")
    prev = None
    if os.path.exists(STATE):
        prev = (open(STATE, "r", encoding="utf-8").read().strip()) or None
    if prev:
        prev_dt = iso(prev)
        if now_dt > prev_dt:
            gap_h = (now_dt - prev_dt).total_seconds() / 3600.0
            print(f"Tick advanced by {gap_h:.2f} h → emit all stations.")
        else:
            print("Same tick as last run → emit nothing.")
    else:
        print("No prior state → initialize without emitting.")
    open(STATE, "w", encoding="utf-8").write(tick)
