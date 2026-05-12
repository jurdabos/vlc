"""Unit tests for the rewritten air producer (geoportal.valencia.es ArcGIS REST)."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

sys.path.append(str(Path(__file__).parents[2] / "producer"))
import air_producer as ap  # noqa: E402


class DummyResilientProducer:
    """Captures key/value pairs handed to ``ResilientProducer.produce``."""

    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, key: bytes, value: bytes):
        self.calls.append({"key": key, "value": value})

    def flush(self, timeout: float = 30.0):
        return 0


def mock_serializer(data: Dict[str, Any], ctx=None) -> bytes:
    """Stand-in for the Avro serializer that just JSON-encodes the record."""
    return json.dumps(data).encode("utf-8")


# Epoch ms used as the canonical timestamp throughout the tests
TS_EPOCH_MS = 1760806800000  # 2025-10-18T17:00:00Z
TS_ISO = "2025-10-18T17:00:00Z"
LATER_TS_EPOCH_MS = 1760810400000  # 2025-10-18T18:00:00Z
LATER_TS_ISO = "2025-10-18T18:00:00Z"


# ---------------- helpers ----------------
def test_epoch_ms_to_iso_roundtrip():
    assert ap.epoch_ms_to_iso(TS_EPOCH_MS) == TS_ISO
    assert ap.epoch_ms_to_iso(0) == "1970-01-01T00:00:00Z"


def test_extract_lat_lon_arcgis_xy():
    lat, lon = ap.extract_lat_lon({"x": -0.3, "y": 39.1})
    assert (lat, lon) == (39.1, -0.3)


def test_extract_lat_lon_legacy_dict():
    lat, lon = ap.extract_lat_lon({"lat": 39.1, "lon": -0.3})
    assert (lat, lon) == (39.1, -0.3)


def test_extract_lat_lon_invalid_returns_none_pair():
    assert ap.extract_lat_lon(None) == (None, None)
    assert ap.extract_lat_lon("POINT(-0.3 39.1)") == (None, None)
    assert ap.extract_lat_lon({"foo": 1}) == (None, None)
    assert ap.extract_lat_lon({"lat": "nope", "lon": -0.3}) == (None, None)


# ---------------- map_record ----------------
def test_map_record_with_arcgis_row():
    row = {
        "fiwareid": "A10_OLIVERETA_60m",
        "fecha_carg": TS_EPOCH_MS,
        "so2": None,
        "no2": 24.0,
        "o3": None,
        "co": None,
        "pm10": 16.0,
        "pm25": 7.0,
        "calidad_am": "Buena",
        # arcgis_client.fetch_page injects these from feature.geometry
        "lat": 39.46924423509195,
        "lon": -0.40592344552906795,
    }
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["fiwareid"] == "A10_OLIVERETA_60m"
    assert out["ts"] == TS_EPOCH_MS
    assert out["_ts_iso"] == TS_ISO
    assert out["air_quality_summary"] == "Buena"
    assert out["lat"] == pytest.approx(39.469244, rel=1e-6)
    assert out["lon"] == pytest.approx(-0.405923, rel=1e-6)
    assert isinstance(out["_fp"], str) and len(out["_fp"]) == 40


def test_map_record_fallback_fiwareid():
    row = {"objectid": 42, "fecha_carg": TS_EPOCH_MS, "lat": 39.1, "lon": -0.3}
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["fiwareid"] == "obj42"


def test_map_record_missing_ts():
    row = {"fiwareid": "A01", "lat": 39.1, "lon": -0.3}
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["ts"] is None
    assert out["_ts_iso"] is None


def test_map_record_legacy_geo_point_2d_fallback():
    row = {
        "fiwareid": "A01",
        "fecha_carg": TS_EPOCH_MS,
        "no2": 10.0,
        "geo_point_2d": {"lat": 39.1, "lon": -0.3},
    }
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["lat"] == 39.1
    assert out["lon"] == -0.3


# ---------------- bootstrap helpers ----------------
def test_compute_select_includes_ts_when_missing():
    avail = ap.DESIRED_FIELDS  # ts not in this list by design
    sel = ap.compute_select(avail, ts_field="fecha_carg")
    assert "fecha_carg" in sel.split(",")
    assert "fiwareid" in sel.split(",")


def test_choose_ts_field_honors_env_when_present(monkeypatch):
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")
    assert ap.choose_ts_field(["objectid", "fecha_carg"], None) == "fecha_carg"


def test_choose_ts_field_returns_env_when_auto_disabled(monkeypatch):
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "custom_ts")
    monkeypatch.setattr(ap, "AUTO_TS_FIELD", False)
    assert ap.choose_ts_field(["id"], None) == "custom_ts"


def test_choose_ts_field_picks_candidate(monkeypatch):
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
    assert ap.choose_ts_field(["id", "update_jcd"], None) == "update_jcd"


def test_choose_ts_field_infers_epoch_ms_from_sample(monkeypatch):
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
    sample = {"id": 1, "weird_ts": TS_EPOCH_MS, "name": "abc"}
    assert ap.choose_ts_field(["id", "weird_ts", "name"], sample) == "weird_ts"


def test_bootstrap_schema_uses_layer_metadata(monkeypatch):
    meta = {"fields": [{"name": n} for n in ["objectid", "fiwareid", "so2", "no2", "fecha_carg"]]}
    monkeypatch.setattr(ap, "get_layer_metadata", lambda *a, **kw: meta)
    monkeypatch.setattr(ap, "fetch_one_feature", lambda *a, **kw: None)
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")
    out_fields, ts_field = ap.bootstrap_schema()
    assert ts_field == "fecha_carg"
    assert "fiwareid" in out_fields and "no2" in out_fields and "fecha_carg" in out_fields


def test_bootstrap_schema_falls_back_to_sample(monkeypatch):
    sample = {"objectid": 1, "fiwareid": "A01", "fecha_carg": TS_EPOCH_MS}
    monkeypatch.setattr(ap, "get_layer_metadata", lambda *a, **kw: None)
    monkeypatch.setattr(ap, "fetch_one_feature", lambda *a, **kw: sample)
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")
    out_fields, ts_field = ap.bootstrap_schema()
    assert ts_field == "fecha_carg"
    assert "fiwareid" in out_fields and "fecha_carg" in out_fields


# ---------------- produce_all ----------------
def test_produce_all_uses_key_format():
    dummy = DummyResilientProducer()
    events = [
        {"fiwareid": "A01", "ts": LATER_TS_EPOCH_MS, "_ts_iso": LATER_TS_ISO, "pm10": 10, "_fp": "abc"},
    ]
    ap.produce_all(dummy, events, mock_serializer)
    assert len(dummy.calls) == 1
    call = dummy.calls[0]
    assert call["key"].decode() == f"A01|{LATER_TS_ISO}"
    payload = json.loads(call["value"].decode())
    assert payload["pm10"] == 10
    # Internal fields must not leak to Kafka
    assert "_fp" not in payload and "_ts_iso" not in payload


def test_produce_all_skips_events_without_ts():
    dummy = DummyResilientProducer()
    events = [
        {"fiwareid": "A01", "ts": None, "_ts_iso": None, "pm10": 10, "_fp": "abc"},
        {"fiwareid": "A02", "ts": LATER_TS_EPOCH_MS, "_ts_iso": LATER_TS_ISO, "pm10": 20, "_fp": "def"},
    ]
    ap.produce_all(dummy, events, mock_serializer)
    assert len(dummy.calls) == 1
    assert dummy.calls[0]["key"].decode() == f"A02|{LATER_TS_ISO}"


# ---------------- fetch_since ----------------
def _arcgis_row(fiwareid: str, ts: int, **values) -> Dict[str, Any]:
    """Helper: build a row in the shape arcgis_client.fetch_page returns."""
    base = {
        "fiwareid": fiwareid,
        "fecha_carg": ts,
        "lat": 39.1,
        "lon": -0.3,
    }
    base.update(values)
    return base


def test_fetch_since_emits_new_and_advances_offset(monkeypatch, tmp_path):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path), raising=False)
    monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"), raising=False)
    monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"), raising=False)
    monkeypatch.setattr(ap, "LIMIT", 10, raising=False)

    page = [
        _arcgis_row("A01", LATER_TS_EPOCH_MS, no2=10.0, pm10=12.0, pm25=4.0),
        _arcgis_row("A02", LATER_TS_EPOCH_MS, no2=20.0, pm10=18.0, pm25=7.0),
    ]
    pages = iter([page, []])

    def fake_fetch_page(*args, **kwargs):
        return next(pages)

    monkeypatch.setattr(ap, "fetch_page", fake_fetch_page)

    station_offsets = {"A01": TS_ISO, "A02": TS_ISO}
    out, new_offsets, new_fps = ap.fetch_since(station_offsets, {}, "fiwareid,fecha_carg,no2,pm10,pm25", "fecha_carg")
    assert len(out) == 2
    assert new_offsets["A01"] == LATER_TS_ISO
    assert new_offsets["A02"] == LATER_TS_ISO
    assert "A01" in new_fps and "A02" in new_fps


def test_fetch_since_logs_and_returns_on_exception(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)

    def failing_fetch(*args, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap, "fetch_page", failing_fetch)
    station_offsets = {"A01": TS_ISO}
    out, new_offsets, _ = ap.fetch_since(station_offsets, {}, "fiwareid,fecha_carg", "fecha_carg")
    assert out == []
    assert new_offsets == station_offsets
    captured = capsys.readouterr().out
    # Must surface, must NOT be the legacy silent "no new records" path
    assert "[air] fetch failed" in captured
    assert "ConnectionError" in captured
    assert "Network error" in captured


def test_fetch_since_skips_records_without_ts(monkeypatch, tmp_path):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)
    pages = iter([[{"fiwareid": "A01", "lat": 39.1, "lon": -0.3}], []])  # no fecha_carg
    monkeypatch.setattr(ap, "fetch_page", lambda *a, **kw: next(pages))
    out, _, _ = ap.fetch_since({"A01": TS_ISO}, {}, "fiwareid,fecha_carg", "fecha_carg")
    assert out == []


def test_fetch_since_emits_for_changed_fingerprint_at_same_ts(monkeypatch, tmp_path):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)
    page = [_arcgis_row("A01", TS_EPOCH_MS, no2=99.0, pm10=10.0, pm25=5.0)]
    pages = iter([page, []])
    monkeypatch.setattr(ap, "fetch_page", lambda *a, **kw: next(pages))
    station_offsets = {"A01": TS_ISO}
    station_fps = {"A01": "stale-fingerprint"}
    out, new_offsets, new_fps = ap.fetch_since(
        station_offsets, station_fps, "fiwareid,fecha_carg,no2,pm10,pm25", "fecha_carg"
    )
    assert len(out) == 1
    assert new_offsets["A01"] == TS_ISO
    assert new_fps["A01"] != "stale-fingerprint"


def test_fetch_since_skips_for_same_fingerprint_at_same_ts(monkeypatch, tmp_path):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)
    row = _arcgis_row("A01", TS_EPOCH_MS, no2=24.0, pm10=16.0, pm25=7.0)
    fp = ap.value_fingerprint({"so2": None, "no2": 24.0, "o3": None, "co": None, "pm10": 16.0, "pm25": 7.0})
    pages = iter([[row], []])
    monkeypatch.setattr(ap, "fetch_page", lambda *a, **kw: next(pages))
    out, _, _ = ap.fetch_since({"A01": TS_ISO}, {"A01": fp}, "fiwareid,fecha_carg,no2,pm10,pm25", "fecha_carg")
    assert out == []


# ---------------- state persistence ----------------
def test_save_and_load_state(tmp_path, monkeypatch):
    state_json = tmp_path / "state.json"
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "STATE_JSON", str(state_json))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))

    ap.save_state({"A01": TS_ISO}, {"A01": "fp"})
    offsets, fps = ap.load_state()
    assert offsets == {"A01": TS_ISO}
    assert fps == {"A01": "fp"}


def test_load_state_empty_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    offsets, fps = ap.load_state()
    assert offsets == {} and fps == {}


def test_load_state_handles_corrupted_json(tmp_path, monkeypatch):
    state_json = tmp_path / "state.json"
    state_json.write_text("{invalid json", encoding="utf-8")
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "STATE_JSON", str(state_json))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    offsets, fps = ap.load_state()
    assert offsets == {} and fps == {}


def test_save_offset_writes_file(tmp_path, monkeypatch):
    offset_file = tmp_path / "offset.txt"
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(offset_file))
    ap.save_offset(TS_ISO)
    assert offset_file.read_text() == TS_ISO


def test_load_offset_reads_file(tmp_path, monkeypatch):
    offset_file = tmp_path / "offset.txt"
    offset_file.write_text(TS_ISO, encoding="utf-8")
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(offset_file))
    assert ap.load_offset() == TS_ISO


def test_load_offset_default_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    monkeypatch.setattr(ap, "PG_BOOTSTRAP", False)
    monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")
    assert ap.load_offset() == "1970-01-01T00:00:00Z"


# ---------------- signal handler ----------------
def test_stop_handler_sets_running_false(monkeypatch):
    monkeypatch.setattr(ap, "running", True)
    ap._stop()
    assert ap.running is False
    ap.running = True  # restore


def test_stop_handler_accepts_signal_args(monkeypatch):
    monkeypatch.setattr(ap, "running", True)
    ap._stop(2, None)
    assert ap.running is False
    ap.running = True
