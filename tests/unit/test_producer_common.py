"""Common producer tests: state persistence, fingerprinting, deduplication."""
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Make producer modules importable
sys.path.append(str(Path(__file__).parents[2] / "producer"))
import air_producer as ap  # noqa: E402


class TestStatePersistence:
    """Tests for state file loading and saving."""

    def test_save_and_load_state(self, tmp_path, monkeypatch):
        """Verifies that state can be saved and loaded correctly."""
        state_json = tmp_path / "state.json"
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(state_json))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))

        offset_in = "2025-10-18T18:00:00Z"
        seen_in = {"A01": "fp123", "A02": "fp456"}

        ap.save_state(offset_in, seen_in)
        assert state_json.exists()

        offset_out, seen_out = ap.load_state()
        assert offset_out == offset_in
        assert seen_out == seen_in

    def test_load_state_returns_default_when_missing(self, tmp_path, monkeypatch):
        """Verifies that load_state returns default offset when no state file exists."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "START_OFFSET", "1970-01-01T00:00:00Z")

        offset, seen = ap.load_state()
        assert offset == "1970-01-01T00:00:00Z"
        assert seen == {}

    def test_load_state_migrates_from_offset_txt(self, tmp_path, monkeypatch):
        """Verifies that load_state can migrate from legacy offset.txt file."""
        offset_file = tmp_path / "offset.txt"
        offset_file.write_text("2025-10-18T17:00:00Z", encoding="utf-8")

        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(offset_file))

        offset, seen = ap.load_state()
        assert offset == "2025-10-18T17:00:00Z"
        assert seen == {}  # no fingerprints from legacy format


class TestValueFingerprint:
    """Tests for value fingerprinting used in deduplication."""

    def test_fingerprint_deterministic(self):
        """Verifies that fingerprints are deterministic for same input."""
        rec = {"so2": 1.0, "no2": 24.0, "o3": None, "co": None, "pm10": 16.0, "pm25": 7.0}
        fp1 = ap.value_fingerprint(rec)
        fp2 = ap.value_fingerprint(rec)
        assert fp1 == fp2
        assert isinstance(fp1, str) and len(fp1) == 40  # SHA1 hex

    def test_fingerprint_changes_with_values(self):
        """Verifies that fingerprints change when values change."""
        rec1 = {"so2": 1.0, "no2": 24.0, "o3": None, "co": None, "pm10": 16.0, "pm25": 7.0}
        rec2 = {"so2": 1.0, "no2": 25.0, "o3": None, "co": None, "pm10": 16.0, "pm25": 7.0}  # no2 changed
        fp1 = ap.value_fingerprint(rec1)
        fp2 = ap.value_fingerprint(rec2)
        assert fp1 != fp2


class TestDeduplication:
    """Tests for deduplication logic in fetch_since."""

    def test_deduplication_skips_same_fingerprint(self, tmp_path, monkeypatch):
        """Verifies that records with same timestamp and fingerprint are skipped."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "LIMIT", 10)

        # First, generate the fingerprint we expect
        sample_rec = {"so2": None, "no2": 24.0, "o3": None, "co": None, "pm10": 16.0, "pm25": 7.0}
        expected_fp = ap.value_fingerprint(sample_rec)

        offset = "2025-10-18T17:00:00Z"
        seen_for_offset = {"A01": expected_fp}  # Station already seen with this fingerprint

        row_data = {
            "fiwareid": "A01",
            "fecha_carg": "2025-10-18T17:00:00+00:00",  # same timestamp
            "so2": None,
            "no2": 24.0,
            "o3": None,
            "co": None,
            "pm10": 16.0,
            "pm25": 7.0,
            "geo_point_2d": {"lat": 39.1, "lon": -0.3},
        }

        class FakeResp:
            ok = True
            status_code = 200
            def json(self):
                return {"total_count": 1, "results": [row_data]}
            def raise_for_status(self):
                pass

        call_count = [0]

        def fake_get(url, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return FakeResp()
            # Subsequent calls return empty
            return type("Empty", (), {"ok": True, "status_code": 200, "json": lambda: {"results": []}, "raise_for_status": lambda: None})()

        monkeypatch.setattr(ap.session, "get", fake_get)

        out, new_offset, new_seen = ap.fetch_since(
            offset, seen_for_offset, ap.BASES, "fiwareid,fecha_carg,no2,pm10,pm25,geo_point_2d", "fecha_carg"
        )

        # Should skip because fingerprint matches
        assert len(out) == 0
        assert new_offset == offset  # offset should not advance

    def test_deduplication_emits_changed_fingerprint(self, tmp_path, monkeypatch):
        """Verifies that records with changed fingerprint are emitted."""
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        monkeypatch.setattr(ap, "LIMIT", 10)

        offset = "2025-10-18T17:00:00Z"
        seen_for_offset = {"A01": "old_fingerprint_123"}  # Old fingerprint

        row_data = {
            "fiwareid": "A01",
            "fecha_carg": "2025-10-18T17:00:00+00:00",
            "so2": None,
            "no2": 30.0,  # Changed value -> different fingerprint
            "o3": None,
            "co": None,
            "pm10": 16.0,
            "pm25": 7.0,
            "geo_point_2d": {"lat": 39.1, "lon": -0.3},
        }

        class FakeResp:
            ok = True
            status_code = 200
            def json(self):
                return {"total_count": 1, "results": [row_data]}
            def raise_for_status(self):
                pass

        call_count = [0]

        def fake_get(url, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return FakeResp()
            return type("Empty", (), {"ok": True, "status_code": 200, "json": lambda: {"results": []}, "raise_for_status": lambda: None})()

        monkeypatch.setattr(ap.session, "get", fake_get)

        out, new_offset, new_seen = ap.fetch_since(
            offset, seen_for_offset, ap.BASES, "fiwareid,fecha_carg,so2,no2,o3,co,pm10,pm25,geo_point_2d", "fecha_carg"
        )

        # Should emit because fingerprint changed
        assert len(out) == 1
        assert out[0]["fiwareid"] == "A01"


class TestTimestampNormalization:
    """Tests for timestamp normalization."""

    def test_normalize_ts_with_offset(self):
        """Verifies normalization of timestamp with timezone offset."""
        assert ap.normalize_ts("2025-10-18T19:00:00+02:00") == "2025-10-18T17:00:00Z"

    def test_normalize_ts_strips_milliseconds(self):
        """Verifies that milliseconds are stripped."""
        assert ap.normalize_ts("2025-10-18T17:00:00.123456Z") == "2025-10-18T17:00:00Z"

    def test_normalize_ts_without_tz(self):
        """Verifies handling of timestamp without timezone.

        Note: timestamps without timezone are parsed as local time by datetime.strptime,
        then assigned UTC. The result depends on the local timezone of the test runner.
        We test only that the format is correct (ends with Z) and the date is preserved.
        """
        result = ap.normalize_ts("2025-10-18T17:00:00")
        assert result.endswith("Z")
        assert result.startswith("2025-10-18T")


class TestChooseTsField:
    """Tests for timestamp field detection."""

    def test_honors_env_if_present(self, monkeypatch):
        """Verifies that TIMESTAMP_FIELD env is used if field exists."""
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")
        result = ap.choose_ts_field(["id", "fecha_carg", "name"], None)
        assert result == "fecha_carg"

    def test_falls_back_to_candidates(self, monkeypatch):
        """Verifies fallback to candidate timestamp fields."""
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
        monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
        result = ap.choose_ts_field(["id", "update_jcd", "name"], None)
        assert result == "update_jcd"

    def test_infers_from_sample(self, monkeypatch):
        """Verifies inference from sample record."""
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
        monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
        sample = {"id": 1, "custom_date": "2025-10-18T17:00:00Z", "name": "test"}
        result = ap.choose_ts_field(["id", "custom_date", "name"], sample)
        assert result == "custom_date"


class TestExtractLatLon:
    """Tests for geo_point_2d extraction."""

    def test_extract_from_dict(self):
        """Verifies extraction from dictionary format."""
        lat, lon = ap.extract_lat_lon({"lat": 39.47, "lon": -0.38})
        assert lat == 39.47
        assert lon == -0.38

    def test_extract_from_wkt_point(self):
        """Verifies extraction from WKT POINT format."""
        lat, lon = ap.extract_lat_lon("POINT(-0.38 39.47)")
        assert pytest.approx(lat, rel=1e-6) == 39.47
        assert pytest.approx(lon, rel=1e-6) == -0.38

    def test_returns_none_for_invalid(self):
        """Verifies that None is returned for invalid formats."""
        lat, lon = ap.extract_lat_lon("invalid")
        assert lat is None
        assert lon is None

        lat2, lon2 = ap.extract_lat_lon(None)
        assert lat2 is None
        assert lon2 is None


class TestGracefulShutdown:
    """Tests for graceful shutdown via signal handlers."""

    def test_stop_handler_sets_running_false(self, monkeypatch):
        """Verifies that _stop handler sets running flag to False."""
        # Reset running to True
        monkeypatch.setattr(ap, "running", True)
        assert ap.running is True
        # Invoking the _stop handler
        ap._stop()
        assert ap.running is False

    def test_signal_handler_registered_for_sigint(self):
        """Verifies that SIGINT is handled by a _stop function.

        Note: the handler may be from either air_producer or weather_producer
        depending on import order, but both have the same behavior.
        """
        import signal
        handler = signal.getsignal(signal.SIGINT)
        # Verifying handler is a callable named _stop
        assert callable(handler)
        assert handler.__name__ == "_stop"

    def test_signal_handler_registered_for_sigterm(self):
        """Verifies that SIGTERM is handled by a _stop function.

        Note: the handler may be from either air_producer or weather_producer
        depending on import order, but both have the same behavior.
        """
        import signal
        handler = signal.getsignal(signal.SIGTERM)
        # Verifying handler is a callable named _stop
        assert callable(handler)
        assert handler.__name__ == "_stop"

    def test_stop_handler_accepts_signal_args(self, monkeypatch):
        """Verifies that _stop handler accepts signal and frame args."""
        monkeypatch.setattr(ap, "running", True)
        # Simulating signal handler call with signum and frame
        ap._stop(2, None)  # SIGINT = 2
        assert ap.running is False


class TestGetFieldsFromMeta:
    """Tests for metadata field extraction."""

    def test_extracts_field_names(self):
        """Verifies extraction of field names from metadata."""
        meta = {
            "dataset": {
                "fields": [
                    {"name": "objectid", "type": "int"},
                    {"name": "nombre", "type": "text"},
                    {"name": "so2", "type": "double"},
                ]
            }
        }
        fields = ap.get_fields_from_meta(meta)
        assert fields == ["objectid", "nombre", "so2"]

    def test_returns_empty_for_invalid_meta(self):
        """Verifies empty list for invalid metadata."""
        assert ap.get_fields_from_meta({}) == []
        assert ap.get_fields_from_meta({"dataset": {}}) == []
