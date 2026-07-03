"""Database access layer for the vlc CLI.

Executes read-only SQL against the pipeline's TimescaleDB. Two transports:

* docker (default): shells out to ``docker compose exec -T timescaledb psql
  --csv``, matching the repo's no-published-ports posture where 5432 is
  reachable only inside the compose network.
* dsn: when ``VLC_DSN`` is set (e.g. ``postgresql://user:pass@localhost:5432/vlc``),
  connects directly via psycopg2 for setups that do expose the port.

All CLI data commands must send presentation-only SELECTs from the
dbt-managed views (see :mod:`vlc.queries`) so the CLI shares one source of
truth with the Grafana dashboards.
"""

from __future__ import annotations

import csv
import io
import os
import subprocess
from pathlib import Path

from acidbase.push import get_project_root

# Repo-relative compose file used by the docker transport.
COMPOSE_FILE = Path("compose") / "docker-compose.yml"


class DbError(RuntimeError):
    """Raised when a query cannot be executed against TimescaleDB."""


def parse_csv_rows(text: str) -> list[dict[str, str]]:
    """Parses ``psql --csv`` output into a list of row dicts."""
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def _docker_command(sql: str, root: Path) -> list[str]:
    """Builds the docker-compose psql invocation for *sql*."""
    compose_file = root / COMPOSE_FILE
    user = os.getenv("VLC_PGUSER", "vlc_dev")
    database = os.getenv("VLC_PGDATABASE", "vlc")
    return [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        "timescaledb",
        "psql",
        "-X",
        "-q",
        "-U",
        user,
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "--csv",
        "-c",
        sql,
    ]


def _run_via_docker(sql: str, root: Path) -> list[dict[str, str]]:
    """Runs *sql* through the compose-managed psql and parses CSV rows."""
    cmd = _docker_command(sql, root)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", check=False)
    except FileNotFoundError as exc:  # to fail helpfully when docker is absent
        raise DbError("docker executable not found on PATH; set VLC_DSN for a direct connection") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise DbError(f"query via docker compose failed: {detail or f'exit code {result.returncode}'}")
    return parse_csv_rows(result.stdout)


def _run_via_dsn(sql: str, dsn: str) -> list[dict[str, object]]:
    """Runs *sql* over a direct psycopg2 connection given by ``VLC_DSN``."""
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - psycopg2 is a project dep
        raise DbError("psycopg2 is required for the VLC_DSN transport") from exc
    try:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description or []]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise DbError(f"query via VLC_DSN failed: {exc}") from exc


def run_query(sql: str, root: Path | None = None) -> list[dict[str, object]]:
    """Executes *sql* and returns rows as dicts, choosing the transport.

    ``VLC_DSN`` selects the direct psycopg2 path; otherwise the query goes
    through ``docker compose exec timescaledb psql --csv`` relative to the
    project root (located via pyproject.toml discovery).
    """
    dsn = os.getenv("VLC_DSN")
    if dsn:
        return _run_via_dsn(sql, dsn)
    return _run_via_docker(sql, (root or get_project_root()).resolve())
