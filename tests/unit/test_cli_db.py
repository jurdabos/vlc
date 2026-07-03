"""Unit tests for the vlc CLI database transport (vlc.db)."""

import subprocess

import pytest
from click.testing import CliRunner

from vlc import db
from vlc.cli import cli


def test_parse_csv_rows_basic():
    """psql --csv output becomes a list of dicts keyed by header."""
    text = "fiwareid,status\nW01,Fresh\nW05,Offline\n"
    rows = db.parse_csv_rows(text)
    assert rows == [
        {"fiwareid": "W01", "status": "Fresh"},
        {"fiwareid": "W05", "status": "Offline"},
    ]


def test_parse_csv_rows_empty():
    """Empty output yields no rows."""
    assert db.parse_csv_rows("") == []


def test_docker_command_shape(tmp_path):
    """The docker transport targets the repo compose file and emits CSV."""
    cmd = db._docker_command("SELECT 1;", tmp_path)
    assert cmd[:4] == ["docker", "compose", "-f", str(tmp_path / "compose" / "docker-compose.yml")]
    assert "--csv" in cmd
    assert cmd[-1] == "SELECT 1;"


def test_run_query_docker_success(monkeypatch, tmp_path):
    """Without VLC_DSN, run_query parses rows from the docker psql output."""
    monkeypatch.delenv("VLC_DSN", raising=False)

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, stdout="a,b\n1,2\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert db.run_query("SELECT 1;", root=tmp_path) == [{"a": "1", "b": "2"}]


def test_run_query_docker_failure(monkeypatch, tmp_path):
    """A failing psql invocation surfaces stderr in a DbError."""
    monkeypatch.delenv("VLC_DSN", raising=False)

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="boom")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(db.DbError, match="boom"):
        db.run_query("SELECT 1;", root=tmp_path)


def test_status_command_renders_rows(monkeypatch):
    """`vlc status --csv` renders whatever the transport returns."""
    rows = [
        {
            "schema_name": "weather",
            "fiwareid": "W05_VALENCIA_UPV_10m",
            "last_update": "2026-02-05 07:20:00+00",
            "staleness": "148 days",
            "status": "Offline",
        }
    ]
    monkeypatch.setattr("vlc.cli.db.run_query", lambda sql: rows)
    result = CliRunner().invoke(cli, ["status", "--csv"])
    assert result.exit_code == 0
    assert "W05_VALENCIA_UPV_10m" in result.output
    assert "Offline" in result.output


def test_status_command_db_error(monkeypatch):
    """Transport failures exit non-zero with a friendly message."""

    def boom(sql):
        raise db.DbError("cannot reach timescaledb")

    monkeypatch.setattr("vlc.cli.db.run_query", boom)
    result = CliRunner().invoke(cli, ["status"])
    assert result.exit_code == 1
    assert "cannot reach timescaledb" in result.stderr
