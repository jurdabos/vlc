"""Downloads any dataset from Valencia Open Data by dataset ID."""

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, UTC
from pathlib import Path


def fetch_all_dataset_ids(timeout=30):
    """
    Fetches all dataset IDs from the Valencia OpenDataSoft catalog.

    Args:
        timeout: Request timeout in seconds (default: 30).

    Returns:
        A set of dataset IDs.
    """
    base_url = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets"
    limit = 100
    offset = 0
    ids = set()
    
    print("Fetching all dataset IDs from catalog...")
    while True:
        url = f"{base_url}?limit={limit}&offset={offset}"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                data = json.loads(response.read().decode())
                results = data.get("results", [])
                if not results:
                    break
                ids |= {x["dataset_id"] for x in results}
                offset += limit
                print(f"  Found {len(ids)} dataset IDs so far...")
        except Exception as e:
            print(f"Error fetching catalog at offset {offset}: {e}")
            break
    
    print(f"Total dataset IDs found: {len(ids)}\n")
    return ids


def fetch_catalog_metadata(dataset_id, timeout=30):
    """
    Fetches the OpenDataSoft catalog dataset metadata and returns the raw JSON.

    Args:
        dataset_id: The dataset identifier to fetch metadata for.
        timeout: Request timeout in seconds (default: 30).

    Returns:
        A tuple (payload, meta) where:
        - payload is the parsed JSON dict or None on error
        - meta is a dict with url, fetched_at, status, and optional error
    """
    now = datetime.now(UTC)
    # URL-encoding dataset_id to prevent injection issues
    url = f"https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/{urllib.parse.quote(dataset_id)}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "vlc-data-ingestion/1.0",
            "Accept": "application/json"
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            if response.status != 200:
                error_msg = f"HTTP {response.status}"
                print(f"Warning: Catalog fetch failed for {dataset_id}: {error_msg}")
                return None, {"url": url, "fetched_at": now.isoformat(), "status": "error", "error": error_msg}
            raw = response.read()
            # Getting charset from response headers, defaulting to utf-8
            charset = response.headers.get_content_charset() or "utf-8"
            data = json.loads(raw.decode(charset, errors="replace"))
            return data, {"url": url, "fetched_at": now.isoformat(), "status": "ok"}
    except (urllib.error.HTTPError, urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        error_msg = str(e)
        print(f"Warning: Catalog fetch error for {dataset_id}: {error_msg}")
        return None, {"url": url, "fetched_at": now.isoformat(), "status": "error", "error": error_msg}


def download_dataset(dataset_id, output_base_dir):
    """
    Downloads all records from the specified dataset and embeds catalog metadata.

    Fetches the full dataset records and the corresponding OpenDataSoft catalog
    metadata, then saves both the data and enriched metadata to JSON files.
    """
    output_dir = Path(output_base_dir) / dataset_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    all_records = []
    limit = 100
    offset = 0
    total_count = None
    max_offset = 10000
    
    print(f"Starting download of {dataset_id} dataset...")
    print(f"Output directory: {output_dir}")
    print(f"Note: API has max offset of {max_offset}, will use export endpoint for full dataset\n")
    
    base_url = f"https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/{dataset_id}"
    export_url = f"{base_url}/exports/json"
    
    print(f"Downloading full dataset using export endpoint...")
    try:
        with urllib.request.urlopen(export_url) as response:
            all_records = json.loads(response.read().decode())
            total_count = len(all_records)
            print(f"Downloaded {total_count:,} records successfully")
    except Exception as e:
        print(f"Export endpoint failed: {e}")
        print(f"Falling back to pagination (limited to {max_offset} records)...\n")
        
        while offset < max_offset:
            url = f"{base_url}/records?limit={limit}&offset={offset}"
            
            try:
                with urllib.request.urlopen(url) as response:
                    data = json.loads(response.read().decode())
            except Exception as e:
                print(f"Error fetching data at offset {offset}: {e}")
                break
            
            if total_count is None:
                total_count = data.get("total_count", 0)
                print(f"Total records available: {total_count:,} (limited to {max_offset})")
            
            results = data.get("results", [])
            if not results:
                break
            
            all_records.extend(results)
            offset += limit
            
            progress = (len(all_records) / min(total_count, max_offset) * 100) if total_count else 0
            print(f"Downloaded {len(all_records):,} / {min(total_count, max_offset):,} records ({progress:.1f}%)")
            
            if len(all_records) >= min(total_count, max_offset):
                break
    
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    
    json_file = output_dir / f"{dataset_id}_full_{timestamp}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(all_records):,} records to: {json_file}")
    
    # Fetching catalog metadata for enriched documentation
    print(f"Fetching catalog metadata for {dataset_id}...")
    catalog_payload, catalog_meta = fetch_catalog_metadata(dataset_id)
    
    metadata_file = output_dir / f"{dataset_id}_metadata_{timestamp}.json"
    metadata = {
        "dataset_id": dataset_id,
        "download_timestamp": datetime.now(UTC).isoformat(),
        "total_records": len(all_records),
        "api_total_count": total_count,
        "source_url": f"https://valencia.opendatasoft.com/explore/dataset/{dataset_id}"
    }
    
    # Embedding catalog metadata
    if catalog_payload is not None:
        metadata["catalog"] = {
            "url": catalog_meta["url"],
            "fetched_at": catalog_meta["fetched_at"],
            "status": catalog_meta["status"],
            "data": catalog_payload
        }
        print(f"Catalog metadata included ({len(catalog_payload)} top-level keys)")
    else:
        metadata["catalog"] = {
            "url": catalog_meta["url"],
            "fetched_at": catalog_meta["fetched_at"],
            "status": catalog_meta["status"],
            "error": catalog_meta.get("error")
        }
        print(f"Proceeding without catalog data (fetch failed)")
    
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"Saved metadata to: {metadata_file}")
    
    return all_records, metadata


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Download datasets from Valencia Open Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run download_dataset.py --datasetid vias
  uv run download_dataset.py --datasetid arbratge-arbolado
  uv run download_dataset.py --datasetid estacions-atmosferiques
  uv run download_dataset.py --all --output D:\\dwh\\vlc
        """
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--datasetid",
        help="The dataset ID to download (e.g., 'vias', 'arbratge-arbolado')"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Download all available datasets from the catalog"
    )
    parser.add_argument(
        "--output",
        default=r"D:\tanul\iu\subjects\project_data_engineering\vlc\dataset",
        help="Base output directory (default: D:\\tanul\\iu\\subjects\\project_data_engineering\\vlc\\dataset)"
    )
    
    args = parser.parse_args()
    
    if args.all:
        dataset_ids = fetch_all_dataset_ids()
        print(f"Starting bulk download of {len(dataset_ids)} datasets...\n")
        
        success_count = 0
        failed = []
        
        for idx, dataset_id in enumerate(sorted(dataset_ids), 1):
            print(f"\n{'='*80}")
            print(f"[{idx}/{len(dataset_ids)}] Processing: {dataset_id}")
            print(f"{'='*80}")
            try:
                records, meta = download_dataset(dataset_id, args.output)
                success_count += 1
                print(f"✓ Successfully downloaded {len(records):,} records")
            except Exception as e:
                print(f"✗ Failed to download {dataset_id}: {e}")
                failed.append(dataset_id)
        
        print(f"\n{'='*80}")
        print(f"Bulk download complete!")
        print(f"Successful: {success_count}/{len(dataset_ids)}")
        if failed:
            print(f"Failed ({len(failed)}): {', '.join(failed)}")
    else:
        records, meta = download_dataset(args.datasetid, args.output)
        print(f"\nDownload complete!")
        print(f"Dataset ID: {args.datasetid}")
        print(f"Total records: {len(records):,}")


if __name__ == "__main__":
    main()
