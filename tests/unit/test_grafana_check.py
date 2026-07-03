"""Tests for the Grafana SSOT guardrail (vlc.grafana_check)."""

import json
from pathlib import Path

from vlc.grafana_check import Statement, check, find_violations

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_repo_grafana_assets_pass():
    """The repo's own dashboards and alert rules respect the SSOT contract."""
    assert check(REPO_ROOT) == []


def test_derived_logic_against_hyper_flagged(tmp_path):
    """DISTINCT ON against a hypertable in a dashboard is a violation."""
    dash_dir = tmp_path / "grafana" / "dashboards"
    dash_dir.mkdir(parents=True)
    dashboard = {
        "panels": [
            {
                "id": 1,
                "title": "Bad panel",
                "targets": [
                    {"rawSql": "SELECT DISTINCT ON (fiwareid) * FROM weather.hyper ORDER BY fiwareid, ts DESC;"}
                ],
            }
        ]
    }
    (dash_dir / "bad.json").write_text(json.dumps(dashboard), encoding="utf-8")
    violations = check(tmp_path)
    assert len(violations) == 1
    assert "distinct on" in violations[0]
    assert "Bad panel" in violations[0]


def test_alert_yaml_staleness_math_flagged(tmp_path):
    """max(ts) staleness math against a hypertable in alert provisioning is a violation."""
    alert_dir = tmp_path / "grafana" / "provisioning" / "alerting"
    alert_dir.mkdir(parents=True)
    (alert_dir / "bad.yml").write_text(
        "groups:\n"
        "  - rules:\n"
        "      - data:\n"
        "          - model:\n"
        "              rawSql: SELECT max(ts) FROM weather.hyper GROUP BY fiwareid;\n",
        encoding="utf-8",
    )
    violations = check(tmp_path)
    assert len(violations) == 1
    assert "max(ts)" in violations[0]


def test_views_and_plain_projections_allowed():
    """Reads from dbt views and plain hypertable projections are fine."""
    allowed = [
        Statement("view read", "SELECT fiwareid, age_s FROM weather.latest;"),
        Statement("time series", "SELECT ts, no2 FROM air.hyper WHERE ts >= NOW() - INTERVAL '7 days';"),
        Statement("row count", "SELECT count(*) FROM weather.hyper;"),
        Statement("aggregate", "SELECT bucket, temp_avg FROM weather.daily;"),
    ]
    assert find_violations(allowed) == []
