"""Unit tests for the rewritten weather producer (geoportal.valencia.es ArcGIS REST)."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.append(str(Path(__file__).parents[2] / "producer"))
import weather_producer as wp  # noqa: E402


class DummyResilientProducer:
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, key: bytes, value: bytes):
        self.calls.append({"key": key, "value": value})

    def flush(self, timeout: float = 30.0):
        return 0


def mock_serializer(data: Dict[str, Any], ctx=None) -> bytes:
    return json.dumps(data).encode("utf-8")


TS_EPOCH_MS = 1760806800000  # 2025-10-18T17:00:00Z
TS_ISO = "2025-10-18T17:00:00Z"
LATER_TS_EPOCH_MS = 1760810400000  # 2025-10-18T18:00:00Z
LATER_TS_ISO = "2025-10-18T18:00:00Z"


# ---------------- map_record ----------------
def test_weather_map_record_field_renames():
    row = {
        "fiwareid": "W01",
        "fecha_carg": TS_EPOCH_MS,
        "direccion": "CENTRO",
        "viento_dir": 180,
        "viento_vel": 3.2,
        "temperatur": 22.5,
        "humedad_re": 55.0,
        "presion_ba": 1013.2,
        "precipitac": 0.4,
        "lat": 39.47,
        "lon": -0.38,
    }
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["ts"] == TS_EPOCH_MS
    assert out["_ts_iso"] == TS_ISO
    assert out["wind_dir_deg"] == 180
    assert out["wind_speed_ms"] == 3.2
    assert out["temperature_c"] == 22.5
    assert out["humidity_pct"] == 55.0
    assert out["pressure_hpa"] == 1013.2
    assert out["precip_mm"] == 0.4
    assert out["lat"] == 39.47
    assert out["lon"] == -0.38
    assert isinstance(out["_fp"], str) and len(out["_fp"]) == 40


def test_weather_map_record_fallback_fiwareid():
    row = {"objectid": 99, "fecha_carg": TS_EPOCH_MS, "lat": 39.47, "lon": -0.38}
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["fiwareid"] == "obj99"


def test_weather_map_record_missing_ts():
    row = {"fiwareid": "W01", "lat": 39.47, "lon": -0.38}
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["ts"] is None
    assert out["_ts_iso"] is None


def test_weather_map_record_legacy_geo_point_2d_fallback():
    row = {
        "fiwareid": "W01",
        "fecha_carg": TS_EPOCH_MS,
        "geo_point_2d": {"lat": 39.47, "lon": -0.38},
    }
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["lat"] == 39.47
    assert out["lon"] == -0.38


# ---------------- helpers ----------------
def test_weather_extract_lat_lon_arcgis_xy():
    lat, lon = wp.extract_lat_lon({"x": -0.38, "y": 39.47})
    assert (lat, lon) == (39.47, -0.38)


def test_weather_extract_lat_lon_legacy_dict():
    lat, lon = wp.extract_lat_lon({"lat": 39.47, "lon": -0.38})
    assert (lat, lon) == (39.47, -0.38)


def test_weather_extract_lat_lon_invalid():
    assert wp.extract_lat_lon(None) == (None, None)
    assert wp.extract_lat_lon({"foo": 1}) == (None, None)
    assert wp.extract_lat_lon({"lat": "nope", "lon": -0.3}) == (None, None)


def test_weather_value_fingerprint():
    rec1 = {
        "viento_dir": 180,
        "viento_vel": 3.2,
        "temperatur": 22.5,
        "humedad_re": 55.0,
        "presion_ba": 1013.2,
        "precipitac": 0.4,
    }
    rec2 = dict(rec1, temperatur=23.0)
    assert wp.value_fingerprint(rec1) != wp.value_fingerprint(rec2)
    assert len(wp.value_fingerprint(rec1)) == 40


def test_weather_epoch_ms_to_iso():
    assert wp.epoch_ms_to_iso(TS_EPOCH_MS) == TS_ISO


# ---------------- bootstrap helpers ----------------
def test_weather_compute_select():
    avail = ["objectid", "fiwareid", "temperatur", "viento_dir"]
    sel = wp.compute_select(avail, ts_field="fecha_carg")
    assert "fecha_carg" in sel.split(",")
    assert "temperatur" in sel.split(",")


def test_weather_choose_ts_field_honors_env(monkeypatch):
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")
    assert wp.choose_ts_field(["objectid", "fecha_carg"], None) == "fecha_carg"


def test_weather_choose_ts_field_picks_candidate(monkeypatch):
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(wp, "AUTO_TS_FIELD", True)
    assert wp.choose_ts_field(["id", "update_jcd"], None) == "update_jcd"


def test_weather_bootstrap_schema_uses_layer_metadata(monkeypatch):
    meta = {"fields": [{"name": n} for n in ["objectid", "fiwareid", "temperatur", "fecha_carg"]]}
    monkeypatch.setattr(wp, "get_layer_metadata", lambda *a, **kw: meta)
    monkeypatch.setattr(wp, "fetch_one_feature", lambda *a, **kw: None)
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")
    out_fields, ts_field = wp.bootstrap_schema()
    assert ts_field == "fecha_carg"
    assert "fiwareid" in out_fields and "temperatur" in out_fields and "fecha_carg" in out_fields


def test_weather_bootstrap_schema_falls_back_to_sample(monkeypatch):
    sample = {"objectid": 1, "fiwareid": "W01", "fecha_carg": TS_EPOCH_MS}
    monkeypatch.setattr(wp, "get_layer_metadata", lambda *a, **kw: None)
    monkeypatch.setattr(wp, "fetch_one_feature", lambda *a, **kw: sample)
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")
    out_fields, ts_field = wp.bootstrap_schema()
    assert ts_field == "fecha_carg"
    assert "fiwareid" in out_fields


# ---------------- produce_all ----------------
def test_weather_produce_all_uses_key_format():
    dummy = DummyResilientProducer()
    ev = {"fiwareid": "W01", "ts": TS_EPOCH_MS, "_ts_iso": TS_ISO, "temperature_c": 22.5, "_fp": "f"}
    wp.produce_all(dummy, [ev], mock_serializer)
    assert len(dummy.calls) == 1
    assert dummy.calls[0]["key"].decode() == f"W01|{TS_ISO}"
    payload = json.loads(dummy.calls[0]["value"].decode())
    assert payload["temperature_c"] == 22.5
    assert "_fp" not in payload and "_ts_iso" not in payload


def test_weather_produce_all_skips_no_ts():
    dummy = DummyResilientProducer()
    events = [
        {"fiwareid": "W01", "ts": None, "_ts_iso": None, "temperature_c": 22.5, "_fp": "a"},
        {"fiwareid": "W02", "ts": LATER_TS_EPOCH_MS, "_ts_iso": LATER_TS_ISO, "temperature_c": 23.0, "_fp": "b"},
    ]
    wp.produce_all(dummy, events, mock_serializer)
    assert len(dummy.calls) == 1
    assert dummy.calls[0]["key"].decode() == f"W02|{LATER_TS_ISO}"


# ---------------- fetch_since ----------------
def _arcgis_row(fiwareid: str, ts: int, **values) -> Dict[str, Any]:
    base = {"fiwareid": fiwareid, "fecha_carg": ts, "lat": 39.47, "lon": -0.38}
    base.update(values)
    return base


def test_weather_fetch_since_emits_new(monkeypatch, tmp_path):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)
    page = [_arcgis_row("W01", LATER_TS_EPOCH_MS, temperatur=22.5, humedad_re=55.0)]
    pages = iter([page, []])
    monkeypatch.setattr(wp, "fetch_page", lambda *a, **kw: next(pages))
    out, new_offsets, new_fps = wp.fetch_since(
        {"W01": TS_ISO}, {}, "fiwareid,fecha_carg,temperatur,humedad_re", "fecha_carg"
    )
    assert len(out) == 1
    assert new_offsets["W01"] == LATER_TS_ISO
    assert "W01" in new_fps


def test_weather_fetch_since_logs_and_returns_on_exception(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)

    def fail(*args, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(wp, "fetch_page", fail)
    out, new_offsets, _ = wp.fetch_since({"W01": TS_ISO}, {}, "fiwareid,fecha_carg", "fecha_carg")
    assert out == []
    assert new_offsets == {"W01": TS_ISO}
    captured = capsys.readouterr().out
    assert "[weather] fetch failed" in captured
    assert "ConnectionError" in captured


def test_weather_fetch_since_skips_no_ts(monkeypatch, tmp_path):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)
    pages = iter([[{"fiwareid": "W01", "lat": 39.47, "lon": -0.38}], []])
    monkeypatch.setattr(wp, "fetch_page", lambda *a, **kw: next(pages))
    out, _, _ = wp.fetch_since({"W01": TS_ISO}, {}, "fiwareid,fecha_carg", "fecha_carg")
    assert out == []


def test_weather_fetch_since_dedups_same_fingerprint(monkeypatch, tmp_path):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)
    row = _arcgis_row(
        "W01",
        TS_EPOCH_MS,
        viento_dir=180,
        viento_vel=3.2,
        temperatur=22.5,
        humedad_re=55.0,
        presion_ba=1013.2,
        precipitac=0.4,
    )
    fp = wp.value_fingerprint(
        {
            "viento_dir": 180,
            "viento_vel": 3.2,
            "temperatur": 22.5,
            "humedad_re": 55.0,
            "presion_ba": 1013.2,
            "precipitac": 0.4,
        }
    )
    pages = iter([[row], []])
    monkeypatch.setattr(wp, "fetch_page", lambda *a, **kw: next(pages))
    out, _, _ = wp.fetch_since(
        {"W01": TS_ISO},
        {"W01": fp},
        "fiwareid,fecha_carg,viento_dir,viento_vel,temperatur,humedad_re,presion_ba,precipitac",
        "fecha_carg",
    )
    assert out == []


def test_weather_fetch_since_emits_changed_fingerprint(monkeypatch, tmp_path):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)
    row = _arcgis_row("W01", TS_EPOCH_MS, viento_vel=5.0, temperatur=22.5)
    pages = iter([[row], []])
    monkeypatch.setattr(wp, "fetch_page", lambda *a, **kw: next(pages))
    out, _, _ = wp.fetch_since({"W01": TS_ISO}, {"W01": "stale"}, "fiwareid,fecha_carg,viento_vel", "fecha_carg")
    assert len(out) == 1


# ---------------- state persistence ----------------
def test_weather_save_and_load_state(tmp_path, monkeypatch):
    state_json = tmp_path / "state.json"
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "STATE_JSON", str(state_json))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    wp.save_state({"W01": TS_ISO}, {"W01": "fp123"})
    offsets, fps = wp.load_state()
    assert offsets == {"W01": TS_ISO}
    assert fps == {"W01": "fp123"}


def test_weather_load_state_default(tmp_path, monkeypatch):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "STATE_JSON", str(tmp_path / "state.json"))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    monkeypatch.setattr(wp, "START_OFFSET", "1970-01-01T00:00:00Z")
    offsets, fps = wp.load_state()
    assert offsets == {} and fps == {}


def test_weather_save_offset(tmp_path, monkeypatch):
    offset_file = tmp_path / "offset.txt"
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(offset_file))
    wp.save_offset(TS_ISO)
    assert offset_file.read_text() == TS_ISO


def test_weather_load_offset_from_file(tmp_path, monkeypatch):
    offset_file = tmp_path / "offset.txt"
    offset_file.write_text(TS_ISO, encoding="utf-8")
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(offset_file))
    assert wp.load_offset() == TS_ISO


def test_weather_load_offset_default(tmp_path, monkeypatch):
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    monkeypatch.setattr(wp, "PG_BOOTSTRAP", False)
    monkeypatch.setattr(wp, "START_OFFSET", "1970-01-01T00:00:00Z")
    assert wp.load_offset() == "1970-01-01T00:00:00Z"
