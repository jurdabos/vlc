import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import json
import pytest

# Make producer modules importable
sys.path.append(str(Path(__file__).parents[2] / "producer"))
import air_producer as ap  # noqa: E402


class DummyProducer:
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes):
        self.calls.append({
            "topic": topic,
            "key": key,
            "value": value,
        })

    def flush(self):
        return 0


def test_normalize_ts_variants():
    assert ap.normalize_ts("2025-10-18T17:00:00+00:00") == "2025-10-18T17:00:00Z"
    assert ap.normalize_ts("2025-10-18T17:00:00Z") == "2025-10-18T17:00:00Z"
    assert ap.normalize_ts("2025-10-18T17:00:00.345Z") == "2025-10-18T17:00:00Z"


def test_extract_lat_lon_dict_and_point():
    lat, lon = ap.extract_lat_lon({"lat": 39.1, "lon": -0.3})
    assert (lat, lon) == (39.1, -0.3)
    # POINT (lon lat)
    lat2, lon2 = ap.extract_lat_lon("POINT (-0.4059 39.4692)")
    assert pytest.approx(lat2, rel=1e-6) == 39.4692
    assert pytest.approx(lon2, rel=1e-6) == -0.4059


def test_map_record_includes_expected_fields():
    row = {
        "fiwareid": "A10_OLIVERETA_60m",
        "fecha_carg": "2025-10-18T17:00:00+00:00",
        "so2": None,
        "no2": 24.0,
        "o3": None,
        "co": None,
        "pm10": 16.0,
        "pm25": 7.0,
        "calidad_am": "Buena",
        "geo_point_2d": {"lat": 39.46924423509195, "lon": -0.40592344552906795},
    }
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["fiwareid"] == "A10_OLIVERETA_60m"
    assert out["ts"] == "2025-10-18T17:00:00Z"
    assert out["air_quality_summary"] == "Buena"
    assert "_fp" in out and isinstance(out["_fp"], str) and len(out["_fp"]) == 40


def test_compute_select_includes_ts_when_missing():
    avail = [
        "objectid", "nombre", "direccion", "so2", "no2", "o3", "co", "pm10", "pm25",
        "calidad_am", "fiwareid", "geo_point_2d",
    ]
    sel = ap.compute_select(avail, ts_field="fecha_carg")
    assert "fecha_carg" in sel.split(",")


def test_produce_all_uses_topic_and_key(monkeypatch):
    dummy = DummyProducer()
    events = [{"fiwareid": "A01", "ts": "2025-10-18T18:00:00Z", "pm10": 10, "_fp": "abc"}]
    ap.produce_all(dummy, events)
    assert len(dummy.calls) == 1
    call = dummy.calls[0]
    assert call["topic"] == ap.TOPIC  # default is "vlc.air"
    assert call["key"].decode() == "A01|2025-10-18T18:00:00Z"
    payload = json.loads(call["value"].decode())
    assert payload["pm10"] == 10


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


def test_fetch_since_emits_new_and_advances_offset(monkeypatch, tmp_path):
    # Arrange state dir
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path), raising=False)
    monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"), raising=False)
    monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"), raising=False)
    monkeypatch.setattr(ap, "LIMIT", 2, raising=False)

    offset = "2025-10-18T17:00:00Z"

    page0 = {
        "total_count": 2,
        "results": [
            {
                "fiwareid": "A01",
                "fecha_carg": "2025-10-18T18:00:00+00:00",
                "no2": 10.0,
                "pm10": 12.0,
                "pm25": 4.0,
                "geo_point_2d": {"lat": 39.1, "lon": -0.3},
            },
            {
                "fiwareid": "A02",
                "fecha_carg": "2025-10-18T18:00:00+00:00",
                "no2": 20.0,
                "pm10": 18.0,
                "pm25": 7.0,
                "geo_point_2d": {"lat": 39.2, "lon": -0.31},
            },
        ],
    }

    def fake_get(url, params=None):
        # Only the records endpoint is used in this test
        if url.endswith("/records"):
            # first page returns two rows, next page returns empty
            if params and params.get("offset") == "0":
                return _FakeResp(page0)
            return _FakeResp({"total_count": 2, "results": []})
        return _FakeResp({}, 404)

    monkeypatch.setattr(ap.session, "get", fake_get)

    select = "fiwareid,geo_point_2d,fecha_carg,no2,pm10,pm25"
    out, new_offset, seen_map = ap.fetch_since(offset, {}, ap.BASES, select, ts_field="fecha_carg")

    assert len(out) == 2
    assert new_offset == "2025-10-18T18:00:00Z"
    # Seen map should track fingerprints for the max timestamp
    assert set(seen_map.keys()) == {"A01", "A02"}

    # And produce_all should emit two messages
    dummy = DummyProducer()
    ap.produce_all(dummy, out)
    assert len(dummy.calls) == 2


def test_produce_all_skips_events_without_ts():
    """Verifies that produce_all skips records without timestamp."""
    dummy = DummyProducer()
    events = [
        {"fiwareid": "A01", "ts": None, "pm10": 10, "_fp": "abc"},  # No ts - should skip
        {"fiwareid": "A02", "ts": "2025-10-18T18:00:00Z", "pm10": 20, "_fp": "def"},  # Valid
    ]
    ap.produce_all(dummy, events)
    assert len(dummy.calls) == 1
    assert dummy.calls[0]["key"].decode() == "A02|2025-10-18T18:00:00Z"


def test_get_meta_success(monkeypatch):
    """Verifies get_meta returns parsed JSON on success."""
    meta_response = {"dataset": {"fields": [{"name": "so2"}, {"name": "no2"}]}}

    def fake_get(url):
        return _FakeResp(meta_response)

    monkeypatch.setattr(ap.session, "get", fake_get)
    result = ap.get_meta("https://example.com/api")
    assert result == meta_response


def test_get_meta_returns_none_on_failure(monkeypatch):
    """Verifies get_meta returns None on HTTP error."""
    def fake_get(url):
        return _FakeResp({}, 500)

    monkeypatch.setattr(ap.session, "get", fake_get)
    result = ap.get_meta("https://example.com/api")
    assert result is None


def test_get_meta_returns_none_on_exception(monkeypatch):
    """Verifies get_meta returns None on network exception."""
    def fake_get(url):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap.session, "get", fake_get)
    result = ap.get_meta("https://example.com/api")
    assert result is None


def test_fetch_one_record_success(monkeypatch):
    """Verifies fetch_one_record returns a single record."""
    record = {"fiwareid": "A01", "fecha_carg": "2025-10-18T17:00:00Z"}

    def fake_get(url, params=None):
        resp = _FakeResp({"results": [record]})
        return resp

    monkeypatch.setattr(ap.session, "get", fake_get)
    result = ap.fetch_one_record("https://example.com/api")
    assert result == record


def test_fetch_one_record_returns_none_on_empty(monkeypatch):
    """Verifies fetch_one_record returns None when no records."""
    def fake_get(url, params=None):
        return _FakeResp({"results": []})

    monkeypatch.setattr(ap.session, "get", fake_get)
    result = ap.fetch_one_record("https://example.com/api")
    assert result is None


def test_fetch_one_record_returns_none_on_exception(monkeypatch):
    """Verifies fetch_one_record returns None on exception."""
    def fake_get(url, params=None):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap.session, "get", fake_get)
    result = ap.fetch_one_record("https://example.com/api")
    assert result is None


def test_save_offset(tmp_path, monkeypatch):
    """Verifies save_offset writes to file."""
    offset_file = tmp_path / "offset.txt"
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "OFFSET_FILE", str(offset_file))

    ap.save_offset("2025-10-18T18:00:00Z")
    assert offset_file.exists()
    assert offset_file.read_text() == "2025-10-18T18:00:00Z"


def test_extract_lat_lon_invalid_dict():
    """Verifies extract_lat_lon handles invalid dict values."""
    lat, lon = ap.extract_lat_lon({"lat": "invalid", "lon": -0.3})
    assert lat is None
    assert lon is None


def test_map_record_fallback_fiwareid():
    """Verifies map_record uses objectid fallback for fiwareid."""
    row = {
        "objectid": 42,
        "fecha_carg": "2025-10-18T17:00:00Z",
        "geo_point_2d": {"lat": 39.1, "lon": -0.3},
    }
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["fiwareid"] == "obj42"


def test_map_record_missing_ts():
    """Verifies map_record handles missing timestamp."""
    row = {
        "fiwareid": "A01",
        "geo_point_2d": {"lat": 39.1, "lon": -0.3},
    }
    out = ap.map_record(row, ts_field="fecha_carg")
    assert out["ts"] is None


def test_choose_ts_field_returns_env_when_auto_disabled(monkeypatch):
    """Verifies choose_ts_field returns env value when AUTO_TS_FIELD is false."""
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "custom_ts")
    monkeypatch.setattr(ap, "AUTO_TS_FIELD", False)
    result = ap.choose_ts_field(["id", "name"], None)  # custom_ts not in fields
    assert result == "custom_ts"  # Returns env value even if not present


def test_choose_ts_field_returns_none_when_no_match(monkeypatch):
    """Verifies choose_ts_field returns None when no timestamp field found."""
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
    monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
    result = ap.choose_ts_field(["id", "name", "value"], None)
    assert result is None


def test_bootstrap_schema_with_meta(monkeypatch):
    """Verifies bootstrap_schema uses metadata fields."""
    meta = {
        "dataset": {
            "fields": [
                {"name": "objectid"},
                {"name": "fiwareid"},
                {"name": "so2"},
                {"name": "no2"},
                {"name": "fecha_carg"},
                {"name": "geo_point_2d"},
            ]
        }
    }

    def fake_get(url, params=None):
        if "/records" not in url:
            return _FakeResp(meta)
        return _FakeResp({"results": []})

    monkeypatch.setattr(ap.session, "get", fake_get)
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")

    select, ts_field = ap.bootstrap_schema()
    assert ts_field == "fecha_carg"
    assert "fiwareid" in select
    assert "so2" in select


def test_bootstrap_schema_falls_back_to_sample(monkeypatch):
    """Verifies bootstrap_schema uses sample record when meta fails."""
    sample_record = {
        "fiwareid": "A01",
        "so2": 1.0,
        "fecha_carg": "2025-10-18T17:00:00Z",
    }

    call_count = [0]

    def fake_get(url, params=None):
        call_count[0] += 1
        if "/records" in url:
            return _FakeResp({"results": [sample_record]})
        return _FakeResp({}, 404)  # Meta fails

    monkeypatch.setattr(ap.session, "get", fake_get)
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")

    select, ts_field = ap.bootstrap_schema()
    assert ts_field == "fecha_carg"


def test_fetch_since_handles_api_exception(monkeypatch, tmp_path):
    """Verifies fetch_since handles API exceptions gracefully."""
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)

    def fake_get(url, params=None):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap.session, "get", fake_get)

    out, new_offset, seen_map = ap.fetch_since(
        "2025-10-18T17:00:00Z", {}, ap.BASES, "fiwareid,fecha_carg", "fecha_carg"
    )
    # Should return empty on exception
    assert out == []
    assert new_offset == "2025-10-18T17:00:00Z"


def test_fetch_since_skips_records_without_ts(monkeypatch, tmp_path):
    """Verifies fetch_since skips records without timestamp."""
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)

    page = {
        "results": [
            {"fiwareid": "A01"},  # Missing fecha_carg - should be skipped
        ]
    }

    call_count = [0]

    def fake_get(url, params=None):
        call_count[0] += 1
        if call_count[0] == 1:
            return _FakeResp(page)
        return _FakeResp({"results": []})

    monkeypatch.setattr(ap.session, "get", fake_get)

    out, new_offset, seen_map = ap.fetch_since(
        "2025-10-18T17:00:00Z", {}, ap.BASES, "fiwareid,fecha_carg,so2,no2,o3,co,pm10,pm25,geo_point_2d", "fecha_carg"
    )
    # Should skip records without timestamp
    assert len(out) == 0
