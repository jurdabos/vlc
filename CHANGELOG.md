# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project loosely follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- CI lint job (`.github/workflows/test.yml`): pinned `ruff` to `0.14.6` (was unpinned `pip install ruff`). Unpinned installs were pulling `ruff 0.15.x`, whose formatter strips redundant outer parentheses in `lambda` bodies and produced `Would reformat: scripts/weather_snapshot_watcher.py` despite local checks passing on the lockfile-pinned 0.14.6.
- `scripts/weather_snapshot_watcher.py`: removed redundant outer parens in the `sorted(rows, key=lambda x: x.get("objectid", 0) or 0)` key so the file is also clean under `ruff 0.15.x` formatter (verified with both 0.14.6 and 0.15.12).

### Added
- `db/init/010-bootstrap.sql`: `weather.daily` and `weather.weekly` continuous aggregates now expose `sum(precip_mm) AS precip_total_mm` alongside the existing `max(precip_mm) AS precip_max_mm`. The `max` is the peak per-interval rainfall intensity in the bucket; `sum` is the bucket's total rainfall (since each `precip_mm` row is the rainfall accumulated within that station's reporting interval).
- `grafana/dashboards/weather.json`: new "Daily Precipitation Total - All Stations (Last 30 days)" panel sourcing `weather.daily.precip_total_mm` (full-width bar chart, `lengthmm` unit, legend `sum` reducer). The existing 7-day per-reading precipitation panel is kept for intensity views.
- `db/direct_sql_queries.txt`: "Records of records" block that surfaces the row holding each meteorological extreme (`max/min` of `temperature_c`, `humidity_pct`, `pressure_hpa`, `wind_speed_ms`; `max` of `precip_mm`) via per-metric `UNION ALL` subqueries.
- `db/direct_sql_queries.txt`: all weather hourly-aggregate variants (since-always, since-Monday, since-today, hour-of-day across all years) now also select `SUM(precip_mm) AS precip_total_mm` next to `MAX(precip_mm) AS precip_max_mm`.

### Changed
- `db/direct_sql_queries.txt`: the `SELECT … FROM weather.daily;` "daily trends" query now also projects `precip_total_mm`.
- Live TimescaleDB instance: applied the schema change to add `precip_total_mm` to `weather.daily` and `weather.weekly`. Because TimescaleDB continuous aggregates cannot be `ALTER`ed to add columns and cannot be (re)created inside an explicit transaction, the migration ran outside a transaction: dropped the dependent regular view `weather.daily_trends`, dropped both continuous aggregates, recreated them with the new column, reattached their existing refresh policies, and recreated `weather.daily_trends` (now also exposing `precip_total_mm`) with `SELECT` granted to `vlc_dev`.

### Notes / clarifications
- Empirically validated that `precip_mm` is per-reading rainfall accumulated within the station's reporting interval (≈10 min for `*_10m` stations, hourly for some others), not a cumulative running total: values rise and fall non-monotonically within a day, and per-station daily `sum(precip_mm)` is materially larger than `max(precip_mm)` in rainy periods.
- Continuous aggregates use `WITH (timescaledb.continuous)`; refresh policies are unchanged: `weather.daily` 60d/1d/1h, `weather.weekly` 180d/1d/4h.
