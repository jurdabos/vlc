"""Guardrail against SQL-logic drift between Grafana and the dbt views.

The single-source-of-truth rule for this repo: derived query logic
(latest-per-station, staleness math, snapshot joins) lives exactly once,
in the dbt models under ``dbt/models/``, materialized as database views.
Grafana panels and alert rules may only:

* SELECT from those views/marts or the TimescaleDB continuous aggregates
  (``weather.daily``, ``air.weekly``, ...), or
* run plain projections/filters/counts against the raw hypertables
  (time-series panels).

This module flags every ``rawSql`` in ``grafana/dashboards/*.json`` and
``grafana/provisioning/alerting/*.yml`` that applies a derived-logic
marker to a raw hypertable, which indicates the logic was re-implemented
instead of read from a view. Exposed via ``vlc grafana check`` and run in
CI so drift fails the build.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import yaml

# Markers of derived logic that must live in dbt views, not in Grafana SQL.
DERIVED_MARKERS: tuple[str, ...] = (
    "distinct on",
    "max(ts)",
    "now() - ts",
    "now() - max(ts)",
)
# Raw hypertables that derived logic must never target directly.
RAW_RELATIONS: tuple[str, ...] = ("air.hyper", "weather.hyper")


@dataclass(frozen=True)
class Statement:
    """One SQL statement found in a Grafana asset, with its origin label."""

    origin: str
    sql: str


def _iter_raw_sql(node: object, origin: str) -> Iterator[Statement]:
    """Recursively yields every ``rawSql`` string below *node*."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "rawSql" and isinstance(value, str):
                yield Statement(origin, value)
            else:
                yield from _iter_raw_sql(value, origin)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_raw_sql(item, origin)


def iter_statements(root: Path) -> Iterator[Statement]:
    """Yields SQL statements from dashboards and alert provisioning under *root*."""
    dashboards_dir = root / "grafana" / "dashboards"
    if dashboards_dir.is_dir():
        for path in sorted(dashboards_dir.glob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            for panel in data.get("panels", []):
                origin = f"{path.name}: panel {panel.get('id')} ({panel.get('title', 'untitled')})"
                yield from _iter_raw_sql(panel, origin)
    alerting_dir = root / "grafana" / "provisioning" / "alerting"
    if alerting_dir.is_dir():
        for path in sorted(alerting_dir.glob("*.yml")):
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            yield from _iter_raw_sql(data, f"{path.name}: alert rule")


def find_violations(statements: Iterable[Statement]) -> list[str]:
    """Returns a human-readable violation per statement breaking the SSOT rule."""
    violations: list[str] = []
    for statement in statements:
        low = statement.sql.lower()
        if not any(relation in low for relation in RAW_RELATIONS):
            continue
        markers = [marker for marker in DERIVED_MARKERS if marker in low]
        if markers:
            snippet = " ".join(statement.sql.split())[:120]
            violations.append(
                f"{statement.origin}: derived logic ({', '.join(markers)}) targets a raw hypertable"
                f" — move it to a dbt view. SQL: {snippet}"
            )
    return violations


def check(root: Path) -> list[str]:
    """Runs the SSOT check for the repo at *root* and returns violations."""
    return find_violations(iter_statements(root))
