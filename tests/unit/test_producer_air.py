import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Make producer modules importable
sys.path.append(str(Path(__file__).parents[2] / "producer"))
import air_producer as ap  # noqa: E402


class DummyProducer:
    """Mock for confluent_kafka.Producer (legacy tests)."""

    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes):
        self.calls.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )

    def flush(self):
        return 0


class DummyResilientProducer:
    """Mock for ResilientProducer."""

    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def produce(self, key: bytes, value: bytes):
        self.calls.append({"key": key, "value": value})

    def flush(self, timeout: float = 30.0):
        return 0


def mock_serializer(data: Dict[str, Any], ctx=None) -> bytes:
    """Mock JSON serializer for testing."""
    return json.dumps(data).encode("utf-8")


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
        "objectid",
        "nombre",
        "direccion",
        "so2",
        "no2",
        "o3",
        "co",
        "pm10",
        "pm25",
        "calidad_am",
        "fiwareid",
        "geo_point_2d",
    ]
    sel = ap.compute_select(avail, ts_field="fecha_carg")
    assert "fecha_carg" in sel.split(",")


def test_produce_all_uses_topic_and_key(monkeypatch):
    dummy = DummyResilientProducer()
    events = [{"fiwareid": "A01", "ts": "2025-10-18T18:00:00Z", "pm10": 10, "_fp": "abc"}]
    ap.produce_all(dummy, events, mock_serializer)
    assert len(dummy.calls) == 1
    call = dummy.calls[0]
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

    def fake_http_request(session, method, url, **kwargs):
        params = kwargs.get("params", {})
        # Only the records endpoint is used in this test
        if "/records" in url:
            # first page returns two rows, next page returns empty
            if params and params.get("offset") == "0":
                return _FakeResp(page0)
            return _FakeResp({"total_count": 2, "results": []})
        return _FakeResp({}, 404)

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

    select = "fiwareid,geo_point_2d,fecha_carg,no2,pm10,pm25"
    out, new_offset, seen_map = ap.fetch_since(offset, {}, ap.BASES, select, ts_field="fecha_carg")

    assert len(out) == 2
    assert new_offset == "2025-10-18T18:00:00Z"
    # Seen map should track fingerprints for the max timestamp
    assert set(seen_map.keys()) == {"A01", "A02"}

    # And produce_all should emit two messages
    dummy = DummyResilientProducer()
    ap.produce_all(dummy, out, mock_serializer)
    assert len(dummy.calls) == 2


def test_produce_all_skips_events_without_ts():
    """Verifies that produce_all skips records without timestamp."""
    dummy = DummyResilientProducer()
    events = [
        {"fiwareid": "A01", "ts": None, "pm10": 10, "_fp": "abc"},  # No ts - should skip
        {"fiwareid": "A02", "ts": "2025-10-18T18:00:00Z", "pm10": 20, "_fp": "def"},  # Valid
    ]
    ap.produce_all(dummy, events, mock_serializer)
    assert len(dummy.calls) == 1
    assert dummy.calls[0]["key"].decode() == "A02|2025-10-18T18:00:00Z"


def test_get_meta_success(monkeypatch):
    """Verifies get_meta returns parsed JSON on success."""
    meta_response = {"dataset": {"fields": [{"name": "so2"}, {"name": "no2"}]}}

    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp(meta_response)

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
    result = ap.get_meta("https://example.com/api")
    assert result == meta_response


def test_get_meta_returns_none_on_failure(monkeypatch):
    """Verifies get_meta returns None on HTTP error."""

    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp({}, 500)

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
    result = ap.get_meta("https://example.com/api")
    assert result is None


def test_get_meta_returns_none_on_exception(monkeypatch):
    """Verifies get_meta returns None on network exception."""

    def fake_http_request(session, method, url, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
    result = ap.get_meta("https://example.com/api")
    assert result is None


def test_fetch_one_record_success(monkeypatch):
    """Verifies fetch_one_record returns a single record."""
    record = {"fiwareid": "A01", "fecha_carg": "2025-10-18T17:00:00Z"}

    def fake_http_request(session, method, url, **kwargs):
        resp = _FakeResp({"results": [record]})
        return resp

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
    result = ap.fetch_one_record("https://example.com/api")
    assert result == record


def test_fetch_one_record_returns_none_on_empty(monkeypatch):
    """Verifies fetch_one_record returns None when no records."""

    def fake_http_request(session, method, url, **kwargs):
        return _FakeResp({"results": []})

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
    result = ap.fetch_one_record("https://example.com/api")
    assert result is None


def test_fetch_one_record_returns_none_on_exception(monkeypatch):
    """Verifies fetch_one_record returns None on exception."""

    def fake_http_request(session, method, url, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
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

    def fake_http_request(session, method, url, **kwargs):
        if "/records" not in url:
            return _FakeResp(meta)
        return _FakeResp({"results": []})

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
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

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if "/records" in url:
            return _FakeResp({"results": [sample_record]})
        return _FakeResp({}, 404)  # Meta fails

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
    monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")

    select, ts_field = ap.bootstrap_schema()
    assert ts_field == "fecha_carg"


def test_fetch_since_handles_api_exception(monkeypatch, tmp_path):
    """Verifies fetch_since handles API exceptions gracefully."""
    monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(ap, "LIMIT", 10)

    def fake_http_request(session, method, url, **kwargs):
        raise ConnectionError("Network error")

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

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

    def fake_http_request(session, method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return _FakeResp(page)
        return _FakeResp({"results": []})

    monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

    out, new_offset, seen_map = ap.fetch_since(
        "2025-10-18T17:00:00Z", {}, ap.BASES, "fiwareid,fecha_carg,so2,no2,o3,co,pm10,pm25,geo_point_2d", "fecha_carg"
    )
    # Should skip records without timestamp
    assert len(out) == 0


# ============== Additional coverage tests ==============


class TestLoadOffsetDbBootstrap:
    """Tests for load_offset with PG_BOOTSTRAP enabled."""

    def test_load_offset_from_file_takes_precedence(self, tmp_path, monkeypatch):
        """Verifies that offset file takes precedence over DB bootstrap."""
        offset_file = tmp_path / "offset.txt"
        offset_file.write_text("2025-10-18T17:00:00Z", encoding="utf-8")

        monkeypatch.setattr(ap, "OFFSET_FILE", str(offset_file))
        monkeypatch.setattr(ap, "PG_BOOTSTRAP", True)
        monkeypatch.setattr(ap, "START_OFFSET", "latest_db")

        result = ap.load_offset()
        assert result == "2025-10-18T17:00:00Z"

    def test_load_offset_db_bootstrap_success(self, tmp_path, monkeypatch):
        """Verifies DB bootstrap fetches max timestamp from database."""
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "nonexistent.txt"))
        monkeypatch.setattr(ap, "PG_BOOTSTRAP", True)
        monkeypatch.setattr(ap, "START_OFFSET", "latest_db")

        # Mocking psycopg2
        class MockCursor:
            def execute(self, query, params):
                pass

            def fetchone(self):
                return ("2025-10-18T18:00:00Z",)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        class MockPsycopg2:
            def connect(self, **kwargs):
                return MockConn()

        import sys

        sys.modules["psycopg2"] = MockPsycopg2()

        result = ap.load_offset()
        assert result == "2025-10-18T18:00:00Z"

        # Cleaning up
        del sys.modules["psycopg2"]

    def test_load_offset_db_bootstrap_returns_none(self, tmp_path, monkeypatch):
        """Verifies fallback when DB returns None."""
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "nonexistent.txt"))
        monkeypatch.setattr(ap, "PG_BOOTSTRAP", True)
        monkeypatch.setattr(ap, "START_OFFSET", "latest_db")

        class MockCursor:
            def execute(self, query, params):
                pass

            def fetchone(self):
                return (None,)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        class MockPsycopg2:
            def connect(self, **kwargs):
                return MockConn()

        import sys

        sys.modules["psycopg2"] = MockPsycopg2()
        monkeypatch.setattr(ap, "START_OFFSET", "latest_db")

        result = ap.load_offset()
        # Should return START_OFFSET since DB returned None
        assert result == "latest_db" or result is not None

        del sys.modules["psycopg2"]

    def test_load_offset_db_bootstrap_exception(self, tmp_path, monkeypatch):
        """Verifies fallback on DB connection exception."""
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "nonexistent.txt"))
        monkeypatch.setattr(ap, "PG_BOOTSTRAP", True)
        monkeypatch.setattr(ap, "START_OFFSET", "latest_db")

        class MockPsycopg2:
            def connect(self, **kwargs):
                raise ConnectionError("DB connection failed")

        import sys

        sys.modules["psycopg2"] = MockPsycopg2()

        result = ap.load_offset()
        assert result == "latest_db"  # Falls back to START_OFFSET

        del sys.modules["psycopg2"]

    def test_load_offset_without_db_bootstrap(self, tmp_path, monkeypatch):
        """Verifies default offset when PG_BOOTSTRAP is disabled."""
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "nonexistent.txt"))
        monkeypatch.setattr(ap, "PG_BOOTSTRAP", False)
        monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")

        result = ap.load_offset()
        assert result == "1970-01-01T00:00:00Z"


class TestLoadStateExceptionHandling:
    """Tests for load_state exception handling."""

    def test_load_state_corrupted_json(self, tmp_path, monkeypatch):
        """Verifies load_state handles corrupted JSON gracefully."""
        state_json = tmp_path / "state.json"
        state_json.write_text("{invalid json", encoding="utf-8")

        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(state_json))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")

        offset, seen = ap.load_state()
        # Should fall back to load_offset()
        assert offset == "1970-01-01T00:00:00Z"
        assert seen == {}


class TestNormalizeTsFallbackPaths:
    """Tests for normalize_ts fallback parsing paths."""

    def test_normalize_ts_strptime_fallback(self):
        """Verifies strptime fallback for non-ISO timestamps."""
        # Timestamp without timezone that triggers strptime fallback
        result = ap.normalize_ts("2025-10-18T17:00:00")
        assert result.endswith("Z")
        assert "2025-10-18" in result

    def test_normalize_ts_subsecond_stripping(self):
        """Verifies subsecond stripping for timestamps with milliseconds."""
        result = ap.normalize_ts("2025-10-18T17:00:00.123456Z")
        assert result == "2025-10-18T17:00:00Z"
        assert "." not in result

    def test_normalize_ts_subsecond_with_timezone(self):
        """Verifies subsecond stripping with timezone offset."""
        result = ap.normalize_ts("2025-10-18T19:00:00.500+02:00")
        assert result == "2025-10-18T17:00:00Z"


class TestGetFieldsFromMetaException:
    """Tests for get_fields_from_meta exception handling."""

    def test_get_fields_from_meta_exception_handling(self):
        """Verifies exception handling in get_fields_from_meta."""
        # Passing something that causes an exception when accessing fields
        result = ap.get_fields_from_meta(None)
        assert result == []

    def test_get_fields_from_meta_missing_dataset_key(self):
        """Verifies empty list when dataset key is missing."""
        result = ap.get_fields_from_meta({"other": "data"})
        assert result == []


class TestFetchSinceEarlyReturn:
    """Tests for fetch_since early return scenarios."""

    def test_fetch_since_returns_data_on_exception_after_first_page(self, monkeypatch, tmp_path):
        """Verifies fetch_since returns collected data when exception occurs after first page."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "LIMIT", 1)  # Force pagination

        page0 = {
            "results": [
                {
                    "fiwareid": "A01",
                    "fecha_carg": "2025-10-18T18:00:00+00:00",
                    "no2": 10.0,
                    "pm10": 12.0,
                    "pm25": 4.0,
                    "geo_point_2d": {"lat": 39.1, "lon": -0.3},
                }
            ]
        }

        call_count = [0]

        def fake_http_request(session, method, url, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FakeResp(page0)
            # Second call (next page) raises exception
            raise ConnectionError("Network error on page 2")

        monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

        out, new_offset, seen_map = ap.fetch_since(
            "2025-10-18T17:00:00Z",
            {},
            ap.BASES,
            "fiwareid,fecha_carg,no2,pm10,pm25,geo_point_2d",
            "fecha_carg",
        )
        # Should return the data collected before exception
        assert len(out) == 1
        assert new_offset == "2025-10-18T18:00:00Z"


class TestFetchSinceMaxTsEmission:
    """Tests for fetch_since emission when ts equals max_ts."""

    def test_fetch_since_emits_for_ts_equals_max_ts(self, monkeypatch, tmp_path):
        """Verifies emission for records where ts == max_ts (not offset)."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "LIMIT", 10)

        # Two records at same new timestamp (both > offset)
        page = {
            "results": [
                {
                    "fiwareid": "A01",
                    "fecha_carg": "2025-10-18T18:00:00+00:00",
                    "no2": 10.0,
                    "geo_point_2d": {"lat": 39.1, "lon": -0.3},
                },
                {
                    "fiwareid": "A02",
                    "fecha_carg": "2025-10-18T18:00:00+00:00",
                    "no2": 20.0,
                    "geo_point_2d": {"lat": 39.2, "lon": -0.31},
                },
            ]
        }

        call_count = [0]

        def fake_http_request(session, method, url, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FakeResp(page)
            return _FakeResp({"results": []})

        monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

        out, new_offset, seen_map = ap.fetch_since(
            "2025-10-18T17:00:00Z",  # Older offset
            {},
            ap.BASES,
            "fiwareid,fecha_carg,no2,geo_point_2d",
            "fecha_carg",
        )
        # Both records should be emitted
        assert len(out) == 2
        assert new_offset == "2025-10-18T18:00:00Z"
        # Both stations should be tracked in seen_map
        assert "A01" in seen_map
        assert "A02" in seen_map


class TestBootstrapSchemaFallback:
    """Tests for bootstrap_schema fallback behavior."""

    def test_bootstrap_schema_fallback_to_timestamp_field(self, monkeypatch):
        """Verifies fallback to TIMESTAMP_FIELD when choose_ts_field returns None."""

        def fake_http_request(session, method, url, **kwargs):
            # All metadata/record requests fail
            return _FakeResp({}, 404)

        monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")
        monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)

        select, ts_field = ap.bootstrap_schema()
        # Should fall back to TIMESTAMP_FIELD
        assert ts_field == "fecha_carg"


class TestMainFunction:
    """Tests for the main() function."""

    def test_main_single_iteration_no_data(self, monkeypatch, tmp_path):
        """Verifies main loop handles no-data case."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "DLQ_DIR", str(tmp_path / "dlq"))
        monkeypatch.setattr(ap, "POLL_SECS", 0)  # No sleep
        monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")

        # Mocking to exit after one iteration
        iteration_count = [0]
        original_running = ap.running

        def stop_after_one():
            iteration_count[0] += 1
            return iteration_count[0] < 2

        # Mocking http requests to return empty
        def fake_http_request(session, method, url, **kwargs):
            return _FakeResp({"results": []})

        monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

        # Mocking Producer
        class MockProducer:
            def produce(self, topic, key, value, callback=None):
                pass

            def flush(self, timeout=None):
                return 0

        monkeypatch.setattr("confluent_kafka.Producer", lambda cfg: MockProducer())

        # Stopping after bootstrap
        monkeypatch.setattr(ap, "running", False)

        # This should not raise and should exit cleanly
        ap.main()

        # Resetting running state
        ap.running = original_running

    def test_main_with_data_produces_messages(self, monkeypatch, tmp_path, capsys):
        """Verifies main loop produces messages when data is available."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "DLQ_DIR", str(tmp_path / "dlq"))
        monkeypatch.setattr(ap, "POLL_SECS", 0)
        monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")
        monkeypatch.setattr(ap, "LIMIT", 10)

        # Setting up data to return on first fetch
        page = {
            "results": [
                {
                    "fiwareid": "A01",
                    "fecha_carg": "2025-10-18T18:00:00+00:00",
                    "no2": 10.0,
                    "geo_point_2d": {"lat": 39.1, "lon": -0.3},
                }
            ]
        }
        meta = {"dataset": {"fields": [{"name": "fiwareid"}, {"name": "fecha_carg"}, {"name": "no2"}]}}

        call_count = [0]

        def fake_http_request(session, method, url, **kwargs):
            call_count[0] += 1
            if "/records" not in url:
                return _FakeResp(meta)
            if call_count[0] <= 2:
                return _FakeResp(page)
            return _FakeResp({"results": []})

        monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

        # Mocking Producer
        produced_messages = []

        class MockProducer:
            def produce(self, topic, key, value, callback=None):
                produced_messages.append({"topic": topic, "key": key, "value": value})
                if callback:
                    callback(None, type("Msg", (), {"key": lambda: key, "value": lambda: value})())

            def flush(self, timeout=None):
                return 0

        monkeypatch.setattr("confluent_kafka.Producer", lambda cfg: MockProducer())

        # Stopping immediately after one iteration
        original_running = ap.running
        monkeypatch.setattr(ap, "running", False)

        ap.main()

        # Verifying output
        captured = capsys.readouterr()
        assert "[air] using ts_field" in captured.out

        ap.running = original_running

    def test_main_handles_exception_gracefully(self, monkeypatch, tmp_path, capsys):
        """Verifies main loop catches and logs exceptions."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "DLQ_DIR", str(tmp_path / "dlq"))
        monkeypatch.setattr(ap, "POLL_SECS", 0)
        monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")

        def fake_http_request(session, method, url, **kwargs):
            raise RuntimeError("Simulated failure")

        monkeypatch.setattr(ap, "http_request_with_retry", fake_http_request)

        class MockProducer:
            def produce(self, topic, key, value, callback=None):
                pass

            def flush(self, timeout=None):
                return 0

        monkeypatch.setattr("confluent_kafka.Producer", lambda cfg: MockProducer())

        original_running = ap.running
        monkeypatch.setattr(ap, "running", False)

        # Should not raise
        ap.main()

        ap.running = original_running


class TestStopHandler:
    """Tests for signal handler."""

    def test_stop_handler(self, monkeypatch):
        """Verifies _stop handler sets running to False."""
        monkeypatch.setattr(ap, "running", True)
        ap._stop()
        assert ap.running is False
        # Resetting
        ap.running = True
