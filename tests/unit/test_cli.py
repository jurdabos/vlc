"""Smoke tests for the ``vlc`` CLI group and its ``push`` wiring.

The push workflow itself belongs to acidbase and is tested there; these
tests only assert that the vlc entry point exposes it correctly and that
a dry run in a pristine repository performs no mutations.
"""

import subprocess

import pytest
from click.testing import CliRunner

from vlc.cli import cli


@pytest.fixture
def runner():
    """Provides a Click test runner."""
    return CliRunner()


def test_cli_help_lists_commands(runner):
    """`vlc --help` exits cleanly and advertises all subcommands."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for command in ("push", "status", "latest", "stations", "records", "grafana"):
        assert command in result.output


def test_cli_version(runner):
    """`vlc --version` reports the installed package version."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "version" in result.output.lower()


def test_push_help(runner):
    """`vlc push --help` exposes the acidbase push options."""
    result = runner.invoke(cli, ["push", "--help"])
    assert result.exit_code == 0
    for flag in ("--message", "--dry-run", "--to"):
        assert flag in result.output


def test_push_dry_run_clean_repo(runner, tmp_path, monkeypatch):
    """A dry run in a fresh, clean git repo reports nothing to push and mutates nothing."""
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(cli, ["push", "--dry-run"])
    assert result.exit_code == 0
    assert "Nothing to push" in result.output
    # Verifying no commit was created
    log = subprocess.run(
        ["git", "log", "--oneline"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert log.stdout.strip() == ""
