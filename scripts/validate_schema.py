#!/usr/bin/env python3
"""
Validates JSON data against the air or weather JSON schemas.

Supports validating single records, arrays of records, or newline-delimited JSON (NDJSON).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import jsonschema
from jsonschema import Draft7Validator

# Schema locations relative to this script
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
SCHEMAS_DIR = PROJECT_ROOT / "producer" / "schemas"
SCHEMA_FILES = {
    "air": SCHEMAS_DIR / "air.json",
    "weather": SCHEMAS_DIR / "weather.json",
}


def load_schema(schema_type: str) -> Dict[str, Any]:
    """Loads the JSON schema for the specified type."""
    schema_path = SCHEMA_FILES.get(schema_type)
    if not schema_path or not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found for type: {schema_type}")
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_record(
    record: Dict[str, Any],
    validator: Draft7Validator,
    record_index: Optional[int] = None,
) -> List[str]:
    """
    Validates a single record against the schema.

    Returns a list of error messages (empty if valid).
    """
    errors = []
    prefix = f"Record {record_index}: " if record_index is not None else ""
    for error in validator.iter_errors(record):
        path = ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "(root)"
        errors.append(f"{prefix}{path}: {error.message}")
    return errors


def parse_input(data_str: str) -> List[Dict[str, Any]]:
    """
    Parses input data as JSON array, single object, or NDJSON.

    Returns a list of records.
    """
    data_str = data_str.strip()
    if not data_str:
        return []
    # Trying standard JSON first (array or single object)
    try:
        parsed = json.loads(data_str)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        raise ValueError("JSON must be an object or array of objects")
    except json.JSONDecodeError:
        pass
    # Trying NDJSON (newline-delimited JSON)
    records = []
    for i, line in enumerate(data_str.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            if not isinstance(rec, dict):
                raise ValueError(f"Line {i}: Expected JSON object, got {type(rec).__name__}")
            records.append(rec)
        except json.JSONDecodeError as e:
            raise ValueError(f"Line {i}: Invalid JSON: {e}") from e
    return records


def validate_data(
    records: List[Dict[str, Any]],
    schema_type: str,
    verbose: bool = False,
) -> tuple[int, int, List[str]]:
    """
    Validates a list of records against the specified schema.

    Returns (valid_count, invalid_count, all_errors).
    """
    schema = load_schema(schema_type)
    validator = Draft7Validator(schema)
    valid_count = 0
    invalid_count = 0
    all_errors: List[str] = []
    for i, record in enumerate(records):
        errors = validate_record(record, validator, record_index=i if len(records) > 1 else None)
        if errors:
            invalid_count += 1
            all_errors.extend(errors)
        else:
            valid_count += 1
            if verbose:
                fid = record.get("fiwareid", "N/A")
                ts = record.get("ts", "N/A")
                print(f"✓ Record {i}: fiwareid={fid}, ts={ts}", file=sys.stderr)
    return valid_count, invalid_count, all_errors


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    p = argparse.ArgumentParser(
        description="Validate JSON data against air or weather schemas.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validating a single JSON file
  python validate_schema.py -t air data.json

  # Validating from stdin
  echo '{"fiwareid": "test", "ts": "2024-01-01T00:00:00Z"}' | python validate_schema.py -t air

  # Validating NDJSON
  python validate_schema.py -t weather readings.ndjson
""",
    )
    p.add_argument(
        "-t",
        "--type",
        choices=["air", "weather"],
        required=True,
        help="Schema type to validate against",
    )
    p.add_argument(
        "input_file",
        nargs="?",
        type=argparse.FileType("r", encoding="utf-8"),
        default=sys.stdin,
        help="Input file (JSON or NDJSON). Reads from stdin if not provided.",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print details for each valid record",
    )
    p.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress output; exit code indicates validity",
    )
    p.add_argument(
        "--list-schemas",
        action="store_true",
        help="List available schema files and exit",
    )
    return p.parse_args()


def list_schemas() -> None:
    """Prints available schema files."""
    print("Available schemas:")
    for name, path in SCHEMA_FILES.items():
        status = "✓" if path.exists() else "✗ (missing)"
        print(f"  {name}: {path} {status}")


def main() -> int:
    """
    Main entry point.

    Returns exit code (0 = all valid, 1 = validation errors, 2 = other errors).
    """
    args = parse_args()
    if args.list_schemas:
        list_schemas()
        return 0
    try:
        data_str = args.input_file.read()
        records = parse_input(data_str)
    except ValueError as e:
        if not args.quiet:
            print(f"Error parsing input: {e}", file=sys.stderr)
        return 2
    if not records:
        if not args.quiet:
            print("No records to validate.", file=sys.stderr)
        return 0
    try:
        valid_count, invalid_count, errors = validate_data(records, args.type, verbose=args.verbose)
    except FileNotFoundError as e:
        if not args.quiet:
            print(f"Error: {e}", file=sys.stderr)
        return 2
    except jsonschema.SchemaError as e:
        if not args.quiet:
            print(f"Invalid schema: {e.message}", file=sys.stderr)
        return 2
    if not args.quiet:
        for err in errors:
            print(f"✗ {err}", file=sys.stderr)
        total = valid_count + invalid_count
        print(f"\nValidation complete: {valid_count}/{total} records valid ({args.type} schema)")
    return 1 if invalid_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
