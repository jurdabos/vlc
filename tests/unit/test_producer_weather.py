import sys
from pathlib import Path
from typing import Any, Dict, List
import json
import pytest

# Make producer modules importable
sys.path.append(str(Path(__file__).parents[2] / "producer"))
import weather_producer as wp  # noqa: E402
import resilience  # noqa: E402


class DummyProducer:
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes):
        self.calls.append({"topic": topic, "key": key, "value": value})

    def flush(self):
        return 0


class DummyResilientProducer:
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, key: bytes, value: bytes):
        self.calls.append({"key": key, "value": value})

    def flush(self, timeout: float = 30.0):
        return 0


def test_weather_map_record_field_renames():
    row = {
        "fiwareid": "W01",
        "fecha_carg": "2025-10-18T17:00:00+00:00",
        "direccion": "CENTRO",
        "viento_dir": 180,
        "viento_vel": 3.2,
        "temperatur": 22.5,
        "humedad_re": 55.0,
        "presion_ba": 1013.2,
        "precipitac": 0.4,
        "geo_point_2d": {"lat": 39.47, "lon": -0.38},
    }
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["ts"] == "2025-10-18T17:00:00Z"
    assert out["wind_dir_deg"] == 180
    assert out["wind_speed_ms"] == 3.2
    assert out["temperature_c"] == 22.5
    assert out["humidity_pct"] == 55.0
    assert out["pressure_hpa"] == 1013.2
    assert out["precip_mm"] == 0.4
    assert "_fp" in out and isinstance(out["_fp"], str)


def test_weather_produce_all(monkeypatch):
    dummy = DummyResilientProducer()
    ev = {"fiwareid": "W01", "ts": "2025-10-18T17:00:00Z", "temperature_c": 22.5, "_fp": "f"}
    wp.produce_all(dummy, [ev])
    assert len(dummy.calls) == 1
    call = dummy.calls[0]
    assert call["key"].decode() == "W01|2025-10-18T17:00:00Z"
    payload = json.loads(call["value"].decode())
    assert payload["temperature_c"] == 22.5


class _FakeResp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.ok = status_code == 200

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("HTTP error")


def test_weather_normalize_ts_variants():
    """Verifies timestamp normalization in weather producer."""
    assert wp.normalize_ts("2025-10-18T17:00:00+00:00") == "2025-10-18T17:00:00Z"
    assert wp.normalize_ts("2025-10-18T17:00:00Z") == "2025-10-18T17:00:00Z"
    assert wp.normalize_ts("2025-10-18T17:00:00.345Z") == "2025-10-18T17:00:00Z"


def test_weather_extract_lat_lon():
    """Verifies geo extraction in weather producer."""
    lat, lon = wp.extract_lat_lon({"lat": 39.47, "lon": -0.38})
    assert lat == 39.47
    assert lon == -0.38

    lat2, lon2 = wp.extract_lat_lon("POINT(-0.38 39.47)")
    assert pytest.approx(lat2, rel=1e-6) == 39.47
    assert pytest.approx(lon2, rel=1e-6) == -0.38


def test_weather_value_fingerprint():
    """Verifies fingerprinting in weather producer."""
    rec1 = {
        "viento_dir": 180, "viento_vel": 3.2, "temperatur": 22.5,
        "humedad_re": 55.0, "presion_ba": 1013.2, "precipitac": 0.4
    }
    rec2 = {
        "viento_dir": 180, "viento_vel": 3.2, "temperatur": 23.0,  # Different temp
        "humedad_re": 55.0, "presion_ba": 1013.2, "precipitac": 0.4
    }
    fp1 = wp.value_fingerprint(rec1)
    fp2 = wp.value_fingerprint(rec2)
    assert fp1 != fp2
    assert len(fp1) == 40  # SHA1 hex


def test_weather_save_and_load_state(tmp_path, monkeypatch):
    """Verifies state persistence in weather producer."""
    state_json = tmp_path / "state.json"
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "STATE_JSON", str(state_json))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))

    wp.save_state("2025-10-18T18:00:00Z", {"W01": "fp123"})
    assert state_json.exists()

    offset, seen = wp.load_state()
    assert offset == "2025-10-18T18:00:00Z"
    assert seen == {"W01": "fp123"}


def test_weather_load_state_default(tmp_path, monkeypatch):
    """Verifies default state when no file exists."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "STATE_JSON", str(tmp_path / "state.json"))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    monkeypatch.setattr(wp, "START_OFFSET", "1970-01-01T00:00:00Z")

    offset, seen = wp.load_state()
    assert offset == "1970-01-01T00:00:00Z"
    assert seen == {}


def test_weather_save_offset(tmp_path, monkeypatch):
    """Verifies save_offset in weather producer."""
    offset_file = tmp_path / "offset.txt"
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(offset_file))

    wp.save_offset("2025-10-18T18:00:00Z")
    assert offset_file.exists()
    assert offset_file.read_text() == "2025-10-18T18:00:00Z"


def test_weather_get_meta_success(monkeypatch):
    """Verifies get_meta returns parsed JSON on success."""
    meta = {"dataset": {"fields": [{"name": "temperatur"}]}}

    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp(meta)

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    result = wp.get_meta("https://example.com/api")
    assert result == meta


def test_weather_get_meta_failure(monkeypatch):
    """Verifies get_meta returns None on failure."""
    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp({}, 500)

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    assert wp.get_meta("https://example.com/api") is None


def test_weather_get_meta_exception(monkeypatch):
    """Verifies get_meta returns None on exception."""
    def fake_http_request(session, method, url, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    assert wp.get_meta("https://example.com/api") is None


def test_weather_get_fields_from_meta():
    """Verifies field extraction from metadata."""
    meta = {
        "dataset": {
            "fields": [
                {"name": "temperatur", "type": "double"},
                {"name": "humedad_re", "type": "double"},
            ]
        }
    }
    fields = wp.get_fields_from_meta(meta)
    assert fields == ["temperatur", "humedad_re"]


def test_weather_get_fields_from_meta_empty():
    """Verifies empty list for invalid metadata."""
    assert wp.get_fields_from_meta({}) == []
    assert wp.get_fields_from_meta({"dataset": {}}) == []


def test_weather_fetch_one_record_success(monkeypatch):
    """Verifies fetch_one_record returns a record."""
    record = {"fiwareid": "W01", "fecha_carg": "2025-10-18T17:00:00Z"}

    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp({"results": [record]})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    result = wp.fetch_one_record("https://example.com/api")
    assert result == record


def test_weather_fetch_one_record_empty(monkeypatch):
    """Verifies fetch_one_record returns None when empty."""
    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp({"results": []})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    assert wp.fetch_one_record("https://example.com/api") is None


def test_weather_fetch_one_record_exception(monkeypatch):
    """Verifies fetch_one_record returns None on exception."""
    def fake_http_request(session, method, url, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    assert wp.fetch_one_record("https://example.com/api") is None


def test_weather_choose_ts_field(monkeypatch):
    """Verifies timestamp field selection."""
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")
    result = wp.choose_ts_field(["id", "fecha_carg", "name"], None)
    assert result == "fecha_carg"


def test_weather_choose_ts_field_fallback(monkeypatch):
    """Verifies fallback to candidate fields."""
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(wp, "AUTO_TS_FIELD", True)
    result = wp.choose_ts_field(["id", "update_jcd", "name"], None)
    assert result == "update_jcd"


def test_weather_choose_ts_field_from_sample(monkeypatch):
    """Verifies inference from sample."""
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(wp, "AUTO_TS_FIELD", True)
    sample = {"id": 1, "custom_ts": "2025-10-18T17:00:00Z"}
    result = wp.choose_ts_field(["id", "custom_ts"], sample)
    assert result == "custom_ts"


def test_weather_compute_select():
    """Verifies compute_select includes ts field."""
    avail = ["objectid", "fiwareid", "temperatur", "geo_point_2d"]
    sel = wp.compute_select(avail, ts_field="fecha_carg")
    assert "fecha_carg" in sel.split(",")
    assert "temperatur" in sel.split(",")


def test_weather_fetch_since(monkeypatch, tmp_path):
    """Verifies fetch_since returns records and advances offset."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)

    page = {
        "results": [
            {
                "fiwareid": "W01",
                "fecha_carg": "2025-10-18T18:00:00+00:00",
                "temperatur": 22.5,
                "humedad_re": 55.0,
                "geo_point_2d": {"lat": 39.47, "lon": -0.38},
            }
        ]
    }

    call_count = [0]

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return _FakeResp(page)
        return _FakeResp({"results": []})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)

    out, new_offset, seen_map = wp.fetch_since(
        "2025-10-18T17:00:00Z", {}, wp.BASES,
        "fiwareid,fecha_carg,temperatur,humedad_re,geo_point_2d", "fecha_carg"
    )
    assert len(out) == 1
    assert new_offset == "2025-10-18T18:00:00Z"
    assert "W01" in seen_map


def test_weather_fetch_since_exception(monkeypatch, tmp_path):
    """Verifies fetch_since handles exceptions."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)

    def fake_http_request(session, method, url, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)

    out, new_offset, seen_map = wp.fetch_since(
        "2025-10-18T17:00:00Z", {}, wp.BASES, "fiwareid,fecha_carg", "fecha_carg"
    )
    assert out == []
    assert new_offset == "2025-10-18T17:00:00Z"


def test_weather_bootstrap_schema(monkeypatch):
    """Verifies bootstrap_schema returns select and ts_field."""
    meta = {
        "dataset": {
            "fields": [
                {"name": "fiwareid"},
                {"name": "temperatur"},
                {"name": "fecha_carg"},
            ]
        }
    }

    def fake_http_request(session, method, url, **kwargs):
        if "/records" not in url:
            return _FakeResp(meta)
        return _FakeResp({"results": []})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")

    select, ts_field = wp.bootstrap_schema()
    assert ts_field == "fecha_carg"
    assert "temperatur" in select or "fiwareid" in select


def test_weather_bootstrap_schema_fallback_to_sample(monkeypatch):
    """Verifies bootstrap_schema uses sample when meta fails."""
    sample = {
        "fiwareid": "W01",
        "temperatur": 22.5,
        "fecha_carg": "2025-10-18T17:00:00Z",
    }

    call_count = [0]

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if "/records" in url:
            return _FakeResp({"results": [sample]})
        return _FakeResp({}, 404)

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")

    select, ts_field = wp.bootstrap_schema()
    assert ts_field == "fecha_carg"


def test_weather_produce_all_skips_no_ts():
    """Verifies produce_all skips records without ts."""
    dummy = DummyResilientProducer()
    events = [
        {"fiwareid": "W01", "ts": None, "temperature_c": 22.5, "_fp": "a"},
        {"fiwareid": "W02", "ts": "2025-10-18T18:00:00Z", "temperature_c": 23.0, "_fp": "b"},
    ]
    wp.produce_all(dummy, events)
    assert len(dummy.calls) == 1
    assert dummy.calls[0]["key"].decode() == "W02|2025-10-18T18:00:00Z"


def test_weather_extract_lat_lon_invalid():
    """Verifies extract_lat_lon handles invalid input."""
    lat, lon = wp.extract_lat_lon({"lat": "invalid", "lon": -0.3})
    assert lat is None
    assert lon is None

    lat2, lon2 = wp.extract_lat_lon(None)
    assert lat2 is None
    assert lon2 is None


def test_weather_map_record_fallback_fiwareid():
    """Verifies map_record uses objectid fallback."""
    row = {
        "objectid": 99,
        "fecha_carg": "2025-10-18T17:00:00Z",
        "geo_point_2d": {"lat": 39.47, "lon": -0.38},
    }
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["fiwareid"] == "obj99"


def test_weather_map_record_missing_ts():
    """Verifies map_record handles missing timestamp."""
    row = {
        "fiwareid": "W01",
        "geo_point_2d": {"lat": 39.47, "lon": -0.38},
    }
    out = wp.map_record(row, ts_field="fecha_carg")
    assert out["ts"] is None


def test_weather_load_state_exception(tmp_path, monkeypatch):
    """Verifies load_state handles corrupted JSON gracefully."""
    state_json = tmp_path / "state.json"
    state_json.write_text("invalid json{{", encoding="utf-8")

    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "STATE_JSON", str(state_json))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    monkeypatch.setattr(wp, "START_OFFSET", "1970-01-01T00:00:00Z")

    offset, seen = wp.load_state()
    assert offset == "1970-01-01T00:00:00Z"
    assert seen == {}


def test_weather_normalize_ts_fallback():
    """Verifies normalize_ts handles edge case timestamps."""
    # Timestamp with timezone offset
    result = wp.normalize_ts("2025-10-18T19:00:00+02:00")
    assert result == "2025-10-18T17:00:00Z"


def test_weather_choose_ts_field_returns_env_when_auto_disabled(monkeypatch):
    """Verifies choose_ts_field returns env value when AUTO_TS_FIELD is false."""
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "custom_ts")
    monkeypatch.setattr(wp, "AUTO_TS_FIELD", False)
    result = wp.choose_ts_field(["id", "name"], None)  # custom_ts not in fields
    assert result == "custom_ts"


def test_weather_choose_ts_field_returns_none(monkeypatch):
    """Verifies choose_ts_field returns None when no match found."""
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(wp, "AUTO_TS_FIELD", True)
    result = wp.choose_ts_field(["id", "name", "value"], None)
    assert result is None


def test_weather_fetch_since_deduplication(monkeypatch, tmp_path):
    """Verifies fetch_since deduplication with same timestamp."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)

    # Create fingerprint for the expected record
    rec_values = {
        "viento_dir": 180, "viento_vel": 3.2, "temperatur": 22.5,
        "humedad_re": 55.0, "presion_ba": 1013.2, "precipitac": 0.4
    }
    expected_fp = wp.value_fingerprint(rec_values)

    page = {
        "results": [
            {
                "fiwareid": "W01",
                "fecha_carg": "2025-10-18T17:00:00+00:00",  # Same as offset
                "viento_dir": 180,
                "viento_vel": 3.2,
                "temperatur": 22.5,
                "humedad_re": 55.0,
                "presion_ba": 1013.2,
                "precipitac": 0.4,
                "geo_point_2d": {"lat": 39.47, "lon": -0.38},
            }
        ]
    }

    call_count = [0]

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return _FakeResp(page)
        return _FakeResp({"results": []})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)

    # Already seen this station with same fingerprint
    seen_for_offset = {"W01": expected_fp}

    out, new_offset, seen_map = wp.fetch_since(
        "2025-10-18T17:00:00Z", seen_for_offset, wp.BASES,
        "fiwareid,fecha_carg,viento_dir,viento_vel,temperatur,humedad_re,presion_ba,precipitac,geo_point_2d",
        "fecha_carg"
    )
    # Should skip because fingerprint matches
    assert len(out) == 0


def test_weather_fetch_since_emits_changed_value(monkeypatch, tmp_path):
    """Verifies fetch_since emits when values change at same timestamp."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)

    page = {
        "results": [
            {
                "fiwareid": "W01",
                "fecha_carg": "2025-10-18T17:00:00+00:00",  # Same as offset
                "viento_dir": 180,
                "viento_vel": 5.0,  # Changed value
                "temperatur": 22.5,
                "humedad_re": 55.0,
                "presion_ba": 1013.2,
                "precipitac": 0.4,
                "geo_point_2d": {"lat": 39.47, "lon": -0.38},
            }
        ]
    }

    call_count = [0]

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return _FakeResp(page)
        return _FakeResp({"results": []})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)

    # Old fingerprint doesn't match
    seen_for_offset = {"W01": "old_fingerprint_123"}

    out, new_offset, seen_map = wp.fetch_since(
        "2025-10-18T17:00:00Z", seen_for_offset, wp.BASES,
        "fiwareid,fecha_carg,viento_dir,viento_vel,temperatur,humedad_re,presion_ba,precipitac,geo_point_2d",
        "fecha_carg"
    )
    # Should emit because fingerprint changed
    assert len(out) == 1


def test_weather_bootstrap_schema_no_ts_field(monkeypatch):
    """Verifies bootstrap_schema falls back to TIMESTAMP_FIELD when none found."""
    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp({}, 404)  # All requests fail

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)
    monkeypatch.setattr(wp, "TIMESTAMP_FIELD", "fecha_carg")

    select, ts_field = wp.bootstrap_schema()
    assert ts_field == "fecha_carg"


def test_weather_load_offset_from_file(tmp_path, monkeypatch):
    """Verifies load_offset reads from offset.txt file."""
    offset_file = tmp_path / "offset.txt"
    offset_file.write_text("2025-10-18T17:00:00Z", encoding="utf-8")

    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(offset_file))

    result = wp.load_offset()
    assert result == "2025-10-18T17:00:00Z"


def test_weather_load_offset_default(tmp_path, monkeypatch):
    """Verifies load_offset returns default when no file exists."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "OFFSET_FILE", str(tmp_path / "offset.txt"))
    monkeypatch.setattr(wp, "START_OFFSET", "1970-01-01T00:00:00Z")
    monkeypatch.setattr(wp, "PG_BOOTSTRAP", False)

    result = wp.load_offset()
    assert result == "1970-01-01T00:00:00Z"


def test_weather_fetch_since_skips_records_without_ts(monkeypatch, tmp_path):
    """Verifies fetch_since skips records without timestamp."""
    monkeypatch.setattr(wp, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(wp, "LIMIT", 10)

    page = {
        "results": [
            {"fiwareid": "W01"},  # Missing fecha_carg
        ]
    }

    call_count = [0]

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return _FakeResp(page)
        return _FakeResp({"results": []})

    monkeypatch.setattr(wp, "http_request_with_retry", fake_http_request)

    out, new_offset, seen_map = wp.fetch_since(
        "2025-10-18T17:00:00Z", {}, wp.BASES, "fiwareid,fecha_carg", "fecha_carg"
    )
    assert len(out) == 0


def test_weather_get_fields_from_meta_exception():
    """Verifies get_fields_from_meta handles exceptions."""
    # Missing "fields" key
    meta = {"dataset": {"other": []}}
    result = wp.get_fields_from_meta(meta)
    assert result == []
