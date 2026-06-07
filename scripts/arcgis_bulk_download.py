"""Bulk dumper for the Valencia geoportal ArcGIS REST catalog.

ArcGIS replacement for the legacy ``scripts/bulk_download.py`` (which targeted
the decommissioned ``valencia.opendatasoft.com`` v2.1 catalog). The geoportal
exposes a hierarchical tree of folders -> services -> layers/tables rather
than a flat ``dataset_id`` namespace, so this script:

  1. Recursively walks ``<base>/services?f=json`` to discover every service.
  2. Enumerates ``layers[]`` and ``tables[]`` of every MapServer / FeatureServer.
  3. For each (service, layer), writes a per-layer subfolder to ``--output``
     containing:
       - ``<slug>_full_<ts>.json``     -> array of feature dicts
                                         (``attributes`` extended with
                                         ``lat`` / ``lon`` when present, or
                                         ``_geometry`` for non-point shapes)
       - ``<slug>_metadata_<ts>.json`` -> manifest with download timestamp,
                                         feature count, source URL, and the
                                         layer's full ArcGIS JSON.

Datasets that lived only as Opendatasoft uploads (RVVCCA history CSVs, noise
sensors, budget tables, ...) are NOT on the geoportal and simply won't appear
in the output - the geoportal is per-domain, not a single global catalog.

Examples (PowerShell)::

    uv run python scripts/arcgis_bulk_download.py `
        --output $env:TEMP\\vlc-arcgis-takeout

    uv run python scripts/arcgis_bulk_download.py `
        --output $env:TEMP\\vlc-arcgis-takeout `
        --folder OPENDATA/MedioAmbiente

    uv run python scripts/arcgis_bulk_download.py `
        --folder OPENDATA/MedioAmbiente --service MedioAmbiente `
        --service-type MapServer --layer 156 `
        --output $env:TEMP\\vlc-arcgis-takeout
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

DEFAULT_BASE = "https://geoportal.valencia.es/server/rest/services"
DEFAULT_PAGE = 2000
DATA_SERVICE_TYPES = {"MapServer", "FeatureServer"}
USER_AGENT = "vlc-arcgis-bulk-download/1.0"


def _http_get_json(url: str, *, timeout: int = 60, retries: int = 4) -> dict | list:
    """Fetches the URL and returns the parsed JSON body.

    Retries on transient network/HTTP failures with exponential backoff.
    Treats ArcGIS protocol errors (HTTP 200 + ``{"error": ...}``) as failures.
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                charset = r.headers.get_content_charset() or "utf-8"
                payload = json.loads(r.read().decode(charset, errors="replace"))
                if isinstance(payload, dict) and payload.get("error"):
                    raise RuntimeError(f"ArcGIS error: {payload['error']}")
                return payload
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            OSError,
            json.JSONDecodeError,
            RuntimeError,
        ) as exc:
            last_exc = exc
            sleep = min(2**attempt, 16)
            print(f"  ! {url} -> {type(exc).__name__}: {exc}; retry in {sleep}s")
            time.sleep(sleep)
    assert last_exc is not None
    raise last_exc


def _slugify(value: str) -> str:
    """Returns a filesystem-safe ASCII slug derived from ``value``."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "_"


def discover_services(base: str, root_folder: str = "") -> Iterator[tuple[str, str, str]]:
    """Yields ``(folder, short_service_name, service_type)`` for data services.

    Recursively walks every sub-folder under ``root_folder`` (whole catalog
    when empty). Only services with a type in :data:`DATA_SERVICE_TYPES` are
    yielded; non-data services such as GPServer toolboxes are skipped.
    """
    base = base.rstrip("/")
    queue: list[str] = [root_folder]
    seen: set[str] = set()
    while queue:
        folder = queue.pop(0)
        if folder in seen:
            continue
        seen.add(folder)
        url_root = f"{base}/{folder}" if folder else base
        url = f"{url_root}?{urllib.parse.urlencode({'f': 'json'})}"
        print(f"-> discovering {folder or '<root>'}")
        try:
            doc = _http_get_json(url)
        except Exception as exc:
            print(f"  ! cannot enumerate folder {folder!r}: {exc}")
            continue
        if not isinstance(doc, dict):
            continue
        for sub in doc.get("folders", []) or []:
            queue.append(f"{folder}/{sub}".strip("/"))
        for svc in doc.get("services", []) or []:
            stype = svc.get("type")
            if stype not in DATA_SERVICE_TYPES:
                continue
            full_name = svc.get("name") or ""
            short = full_name.split("/")[-1]
            yield folder, short, stype


def enumerate_layers(base: str, folder: str, name: str, stype: str) -> list[dict]:
    """Returns a list of per-layer descriptors for one service.

    Each descriptor carries everything :func:`dump_layer` needs to fetch and
    persist the layer: catalog coordinates, full layer metadata document,
    and whether the entry came from ``tables[]`` (geometry-less) or
    ``layers[]``.
    """
    base = base.rstrip("/")
    svc_root = f"{base}/{(folder + '/') if folder else ''}{name}/{stype}"
    url = f"{svc_root}?{urllib.parse.urlencode({'f': 'json'})}"
    try:
        svc_doc = _http_get_json(url)
    except Exception as exc:
        print(f"  ! cannot read service {folder}/{name}/{stype}: {exc}")
        return []
    if not isinstance(svc_doc, dict):
        return []
    raw_layers = list(svc_doc.get("layers") or [])
    raw_tables = list(svc_doc.get("tables") or [])
    out: list[dict] = []
    for entry, is_table in [(le, False) for le in raw_layers] + [(te, True) for te in raw_tables]:
        layer_id = entry.get("id")
        if layer_id is None:
            continue
        layer_url = f"{svc_root}/{layer_id}"
        try:
            layer_doc = _http_get_json(f"{layer_url}?{urllib.parse.urlencode({'f': 'json'})}")
        except Exception as exc:
            print(f"  ! cannot read layer {folder}/{name}/{stype}/{layer_id}: {exc}")
            continue
        if not isinstance(layer_doc, dict):
            continue
        out.append(
            {
                "folder": folder,
                "service_name": name,
                "service_type": stype,
                "service_root": svc_root,
                "layer_id": layer_id,
                "layer_url": layer_url,
                "layer_meta": layer_doc,
                "is_table": is_table or layer_doc.get("type") == "Table",
            }
        )
    return out


def _attach_geometry(attrs: dict, geom: dict, geom_type: str | None) -> None:
    """Merges ``geom`` back onto the row's attribute dict.

    Points (under ``outSR=4326``) are flattened to ``lon`` / ``lat`` to match
    the legacy Opendatasoft takeout shape; other geometry types are preserved
    raw under ``_geometry`` so polygons / polylines are not lost.
    """
    if not geom:
        return
    if geom_type == "esriGeometryPoint" and "x" in geom and "y" in geom:
        try:
            attrs["lon"] = float(geom["x"])
            attrs["lat"] = float(geom["y"])
            return
        except (TypeError, ValueError):
            pass
    attrs["_geometry"] = geom


def _fetch_by_offset(layer: dict, *, page_size: int) -> list[dict]:
    """Pages features via ``resultOffset`` / ``resultRecordCount``."""
    layer_url = layer["layer_url"]
    is_table = layer["is_table"]
    geom_type = layer["layer_meta"].get("geometryType")
    rows: list[dict] = []
    offset = 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false" if is_table else "true",
            "outSR": "4326",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
            "f": "json",
        }
        url = f"{layer_url}/query?{urllib.parse.urlencode(params)}"
        page = _http_get_json(url)
        if not isinstance(page, dict):
            break
        feats = page.get("features") or []
        for feat in feats:
            attrs = dict(feat.get("attributes") or {})
            _attach_geometry(attrs, feat.get("geometry") or {}, geom_type)
            rows.append(attrs)
        exceeded = bool(page.get("exceededTransferLimit"))
        if not feats or (len(feats) < page_size and not exceeded):
            break
        offset += len(feats)
    return rows


def _fetch_by_oid_window(layer: dict, *, batch: int = 1000) -> list[dict]:
    """Fallback paging via ``returnIdsOnly`` + ``objectIds=...`` windows.

    Used when the layer's metadata reports ``supportsPagination=false``
    (some legacy MapServer layers still don't accept ``resultOffset``).
    """
    layer_url = layer["layer_url"]
    is_table = layer["is_table"]
    geom_type = layer["layer_meta"].get("geometryType")
    ids_doc = _http_get_json(
        f"{layer_url}/query?{urllib.parse.urlencode({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})}"
    )
    oids: list[int] = list(ids_doc.get("objectIds") or []) if isinstance(ids_doc, dict) else []
    rows: list[dict] = []
    for i in range(0, len(oids), batch):
        chunk = oids[i : i + batch]
        params = {
            "objectIds": ",".join(str(x) for x in chunk),
            "outFields": "*",
            "returnGeometry": "false" if is_table else "true",
            "outSR": "4326",
            "f": "json",
        }
        url = f"{layer_url}/query?{urllib.parse.urlencode(params)}"
        page = _http_get_json(url)
        if not isinstance(page, dict):
            continue
        for feat in page.get("features") or []:
            attrs = dict(feat.get("attributes") or {})
            _attach_geometry(attrs, feat.get("geometry") or {}, geom_type)
            rows.append(attrs)
    return rows


def fetch_features(layer: dict, *, page_size: int) -> list[dict]:
    """Returns every feature of ``layer`` using the best available paging mode.

    Honors the layer's ``maxRecordCount`` cap. Falls back to OID-window paging
    when the layer reports ``supportsPagination=false``.
    """
    meta = layer["layer_meta"]
    max_record = meta.get("maxRecordCount") or DEFAULT_PAGE
    effective = max(1, min(page_size, max_record or page_size))
    advanced = meta.get("advancedQueryCapabilities") or {}
    supports_pagination = (
        advanced.get("supportsPagination") if "supportsPagination" in advanced else meta.get("supportsPagination", True)
    )
    if supports_pagination is False:
        return _fetch_by_oid_window(layer)
    return _fetch_by_offset(layer, page_size=effective)


def dump_layer(layer: dict, output_dir: Path, *, page_size: int, timestamp: str) -> dict:
    """Writes the per-layer feature dump + manifest and returns the manifest."""
    folder = layer["folder"] or "_root"
    name = layer["service_name"]
    stype = layer["service_type"]
    layer_id = layer["layer_id"]
    layer_meta = layer["layer_meta"]
    layer_name = layer_meta.get("name") or f"layer_{layer_id}"
    slug_parts = [
        _slugify(folder.replace("/", "_")),
        _slugify(name),
        str(layer_id),
        _slugify(layer_name),
    ]
    slug = "__".join(slug_parts)
    layer_dir = output_dir / slug
    layer_dir.mkdir(parents=True, exist_ok=True)
    full_path = layer_dir / f"{slug}_full_{timestamp}.json"
    meta_path = layer_dir / f"{slug}_metadata_{timestamp}.json"
    print(f"[{slug}] paging features (maxRecordCount={layer_meta.get('maxRecordCount')})")
    rows = fetch_features(layer, page_size=page_size)
    full_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{slug}] wrote {len(rows):,} features -> {full_path}")
    manifest = {
        "folder": folder if folder != "_root" else "",
        "service_name": name,
        "service_type": stype,
        "layer_id": layer_id,
        "layer_name": layer_name,
        "source_url": layer["layer_url"],
        "query_url": f"{layer['layer_url']}/query",
        "download_timestamp": datetime.now(UTC).isoformat(),
        "total_records": len(rows),
        "page_size": page_size,
        "layer_meta": layer_meta,
    }
    meta_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{slug}] wrote manifest             -> {meta_path}")
    return manifest


def main() -> None:
    """Entry point for the bulk ArcGIS takeout."""
    parser = argparse.ArgumentParser(
        description="Bulk-dump the Valencia geoportal ArcGIS REST catalog.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--base", default=DEFAULT_BASE, help="ArcGIS REST root (default: %(default)s)")
    parser.add_argument("--output", required=True, help="Output directory; will be created if missing.")
    parser.add_argument(
        "--folder",
        default="",
        help="Restrict to a specific catalog subtree (e.g. OPENDATA/MedioAmbiente).",
    )
    parser.add_argument("--service", default=None, help="Restrict to a specific service short-name within --folder.")
    parser.add_argument(
        "--service-type",
        default=None,
        choices=sorted(DATA_SERVICE_TYPES),
        help="Restrict to MapServer or FeatureServer.",
    )
    parser.add_argument(
        "--layer",
        type=int,
        default=None,
        help="Restrict to a single layer id within the chosen service.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE,
        help="resultRecordCount per page (default: %(default)s; clamped to layer's maxRecordCount).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk the catalog and print services + layers without downloading features.",
    )
    args = parser.parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    print("== ArcGIS bulk takeout ==")
    print(f"base   : {args.base}")
    print(f"output : {output_dir.resolve()}")
    print(f"started: {timestamp}")
    services = list(discover_services(args.base, args.folder))
    if args.service:
        services = [s for s in services if s[1] == args.service]
    if args.service_type:
        services = [s for s in services if s[2] == args.service_type]
    print(f"=> {len(services)} data services to scrape")
    catalog_path = output_dir / f"_catalog_{timestamp}.json"
    catalog_path.write_text(
        json.dumps(
            [{"folder": f, "service_name": n, "service_type": t} for f, n, t in services],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    manifests: list[dict] = []
    failures: list[tuple[str, str]] = []
    for idx, (folder, name, stype) in enumerate(services, 1):
        print(f"\n[{idx}/{len(services)}] {folder}/{name}/{stype}")
        try:
            layers = enumerate_layers(args.base, folder, name, stype)
        except Exception as exc:
            failures.append((f"{folder}/{name}/{stype}", str(exc)))
            print(f"  ! failed to enumerate: {exc}")
            continue
        if args.layer is not None:
            layers = [layer for layer in layers if layer["layer_id"] == args.layer]
        for layer in layers:
            if args.dry_run:
                print(
                    f"  - layer {layer['layer_id']:>3}: "
                    f"{layer['layer_meta'].get('name')!r} "
                    f"({'Table' if layer['is_table'] else layer['layer_meta'].get('geometryType')})"
                )
                continue
            try:
                manifests.append(dump_layer(layer, output_dir, page_size=args.page_size, timestamp=timestamp))
            except Exception as exc:
                failures.append((f"{folder}/{name}/{stype}/{layer['layer_id']}", str(exc)))
                print(f"  ! layer dump failed: {exc}")
    summary_path = output_dir / f"_summary_{timestamp}.json"
    summary_path.write_text(
        json.dumps(
            {
                "base": args.base,
                "folder": args.folder,
                "started_at": timestamp,
                "finished_at": datetime.now(UTC).isoformat(),
                "services_total": len(services),
                "layers_total": len(manifests),
                "failures": [{"target": t, "error": e} for t, e in failures],
                "manifests": manifests,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print("\n== Done ==")
    print(f"layers dumped : {len(manifests)}")
    print(f"failures      : {len(failures)}")
    print(f"summary       : {summary_path}")


if __name__ == "__main__":
    main()
