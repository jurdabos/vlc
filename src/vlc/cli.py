"""CLI entry point for the vlc project.

Provides the ``push`` subcommand (shared from :mod:`acidbase.push`) as the
first command of the standard acidvuca interface. Project-specific data
commands (``status``, ``latest``, ``stations``) will attach to the same
group as they land; each must read from the dbt-managed views so the CLI,
the Grafana dashboards, and the alert rules share one source of truth.

Usage examples::

    uv run vlc --help
    uv run vlc push --dry-run
    uv run vlc push -m "feat: describe the change"
"""

from __future__ import annotations

import click
from acidbase.push import ensure_unicode_safe_streams, push_command


@click.group()
@click.version_option(package_name="vlc")
def cli() -> None:
    """CLI tools for the vlc data pipeline project."""
    ensure_unicode_safe_streams()


cli.add_command(push_command)


def main() -> None:
    """Entry point for the ``vlc`` console script."""
    cli()


if __name__ == "__main__":  # pragma: no cover - module-as-script convenience
    main()
