"""CLI entry point for the vlc project.

Provides the ``push`` subcommand (shared from :mod:`acidbase.push`) plus the
project data commands. Data commands read exclusively from the dbt-managed
views (see :mod:`vlc.queries`) so the CLI, the Grafana dashboards, and the
alert rules share one source of truth; ``vlc grafana check`` enforces that
contract for the Grafana side.

Usage examples::

    uv run vlc --help
    uv run vlc push -m "feat: describe the change"
    uv run vlc status
    uv run vlc latest --weather
    uv run vlc stations --csv
    uv run vlc grafana check
"""

from __future__ import annotations

import csv
import sys

import click
from acidbase.push import ensure_unicode_safe_streams, get_project_root, push_command

from vlc import db, queries

# Colors for the data_freshness status column (matches the dbt mart's classes).
_STATUS_STYLES = {"Fresh": "green", "Stale": "yellow", "Offline": "red"}


@click.group()
@click.version_option(package_name="vlc")
def cli() -> None:
    """CLI tools for the vlc data pipeline project."""
    ensure_unicode_safe_streams()


cli.add_command(push_command)


def _fetch(sql: str) -> list[dict[str, object]]:
    """Runs *sql* via :func:`vlc.db.run_query`, exiting with a friendly error."""
    try:
        return db.run_query(sql)
    except db.DbError as exc:
        click.echo(click.style(f"\u2717 {exc}", fg="red"), err=True)
        raise SystemExit(1) from exc


def _print_rows(rows: list[dict[str, object]], as_csv: bool) -> None:
    """Renders rows as CSV (for piping) or as a rich table (for humans)."""
    if not rows:
        click.echo("(no rows)")
        return
    if as_csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return
    from rich.console import Console
    from rich.table import Table

    table = Table(show_header=True, header_style="bold")
    for column in rows[0].keys():
        table.add_column(str(column))
    for row in rows:
        cells = []
        for column, value in row.items():
            text = "" if value is None else str(value)
            style = _STATUS_STYLES.get(text) if column == "status" else None
            cells.append(f"[{style}]{text}[/{style}]" if style else text)
        table.add_row(*cells)
    Console().print(table)


@cli.command("status")
@click.option("--csv", "as_csv", is_flag=True, help="Emit raw CSV instead of a table.")
def status_command(as_csv: bool) -> None:
    """Shows per-station data freshness (public.data_freshness mart)."""
    _print_rows(_fetch(queries.STATUS), as_csv)


@cli.command("latest")
@click.option("--weather", "domain", flag_value="weather", default=True, help="Latest weather readings (default).")
@click.option("--air", "domain", flag_value="air", help="Latest air quality readings.")
@click.option("--all", "domain", flag_value="all", help="Combined air+weather station snapshot.")
@click.option("--csv", "as_csv", is_flag=True, help="Emit raw CSV instead of a table.")
def latest_command(domain: str, as_csv: bool) -> None:
    """Shows the most recent reading per station (weather.latest / air.latest marts)."""
    sql = {"weather": queries.LATEST_WEATHER, "air": queries.LATEST_AIR, "all": queries.SNAPSHOT}[domain]
    _print_rows(_fetch(sql), as_csv)


@cli.command("stations")
@click.option("--csv", "as_csv", is_flag=True, help="Emit raw CSV instead of a table.")
def stations_command(as_csv: bool) -> None:
    """Shows the station inventory (public.stations mart)."""
    _print_rows(_fetch(queries.STATIONS), as_csv)


@cli.command("records")
@click.option("--csv", "as_csv", is_flag=True, help="Emit raw CSV instead of a table.")
def records_command(as_csv: bool) -> None:
    """Shows all-time meteorological extremes (public.weather_records mart)."""
    _print_rows(_fetch(queries.RECORDS), as_csv)


@cli.group("grafana")
def grafana_group() -> None:
    """Grafana-related utilities."""


@grafana_group.command("check")
def grafana_check_command() -> None:
    """Fails when dashboards/alerts re-derive logic owned by the dbt views."""
    from vlc.grafana_check import check

    violations = check(get_project_root())
    if violations:
        for violation in violations:
            click.echo(click.style(f"\u2717 {violation}", fg="red"), err=True)
        raise SystemExit(1)
    click.echo(click.style("\u2713 Grafana SQL respects the dbt single source of truth.", fg="green"))


def main() -> None:
    """Entry point for the ``vlc`` console script."""
    cli()


if __name__ == "__main__":  # pragma: no cover - module-as-script convenience
    main()
