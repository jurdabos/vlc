"""Common producer tests + arcgis_client tests."""

import json
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parents[2] / "producer"))
import air_producer as ap  # noqa: E402
import arcgis_client as ac  # noqa: E402


# ---------------- arcgis_client URL builders ----------------
def test_layer_url_strips_trailing_slash():
    assert ac.layer_url("https://example/server/MapServer/", 156) == "https://example/server/MapServer/156"


def test_query_url_format():
    assert ac.query_url("https://example/server/MapServer", 157) == "https://example/server/MapServer/157/query"


# ---------------- arcgis_client metadata helpers ----------------
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


def test_get_layer_metadata_success(monkeypatch):
    expected = {"name": "Layer", "fields": [{"name": "fiwareid"}]}
    monkeypatch.setattr(ac, "http_request_with_retry", lambda *a, **kw: _FakeResp(expected))
    assert ac.get_layer_metadata(MagicMock(), "https://x", 156) == expected


def test_get_layer_metadata_returns_none_on_error_envelope(monkeypatch):
    payload = {"error": {"code": 500, "message": "Boom"}}
    monkeypatch.setattr(ac, "http_request_with_retry", lambda *a, **kw: _FakeResp(payload))
    assert ac.get_layer_metadata(MagicMock(), "https://x", 156) is None


def test_get_layer_metadata_returns_none_on_exception(monkeypatch):
    def boom(*a, **kw):
        raise ConnectionError("net")

    monkeypatch.setattr(ac, "http_request_with_retry", boom)
    assert ac.get_layer_metadata(MagicMock(), "https://x", 156) is None


def test_get_field_names():
    meta = {"fields": [{"name": "objectid"}, {"name": "fiwareid"}]}
    assert ac.get_field_names(meta) == ["objectid", "fiwareid"]
    assert ac.get_field_names({}) == []
    assert ac.get_field_names({"fields": [{"no_name": True}]}) == []


def test_fetch_one_feature_success(monkeypatch):
    payload = {"features": [{"attributes": {"fiwareid": "A01", "fecha_carg": 0}}]}
    monkeypatch.setattr(ac, "http_request_with_retry", lambda *a, **kw: _FakeResp(payload))
    rec = ac.fetch_one_feature(MagicMock(), "https://x", 156)
    assert rec == {"fiwareid": "A01", "fecha_carg": 0}


def test_fetch_one_feature_none_on_empty(monkeypatch):
    monkeypatch.setattr(ac, "http_request_with_retry", lambda *a, **kw: _FakeResp({"features": []}))
    assert ac.fetch_one_feature(MagicMock(), "https://x", 156) is None


def test_fetch_one_feature_none_on_exception(monkeypatch):
    def boom(*a, **kw):
        raise ConnectionError("net")

    monkeypatch.setattr(ac, "http_request_with_retry", boom)
    assert ac.fetch_one_feature(MagicMock(), "https://x", 156) is None


# ---------------- arcgis_client.fetch_page ----------------
def test_fetch_page_merges_geometry_into_attributes(monkeypatch):
    payload = {
        "features": [
            {
                "attributes": {"fiwareid": "A01", "fecha_carg": 1234567890000, "no2": 12.0},
                "geometry": {"x": -0.34, "y": 39.46},
            }
        ]
    }
    captured: Dict[str, Any] = {}

    def fake_http(session, method, url, config=None, params=None):
        captured["url"] = url
        captured["params"] = params
        return _FakeResp(payload)

    monkeypatch.setattr(ac, "http_request_with_retry", fake_http)
    rows = ac.fetch_page(
        MagicMock(),
        "https://example/server/MapServer",
        156,
        out_fields="fiwareid,fecha_carg,no2",
        ts_field="fecha_carg",
        limit=2000,
        offset=0,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["fiwareid"] == "A01"
    assert r["no2"] == 12.0
    assert r["lat"] == 39.46
    assert r["lon"] == -0.34
    # Confirm we ask the server for the right shape
    assert captured["url"].endswith("/156/query")
    assert captured["params"]["where"] == "1=1"
    assert captured["params"]["outFields"] == "fiwareid,fecha_carg,no2"
    assert captured["params"]["returnGeometry"] == "true"
    assert captured["params"]["outSR"] == "4326"
    assert captured["params"]["orderByFields"] == "fecha_carg DESC"
    assert captured["params"]["resultRecordCount"] == "2000"
    assert captured["params"]["resultOffset"] == "0"
    assert captured["params"]["f"] == "json"


def test_fetch_page_raises_on_error_envelope(monkeypatch):
    payload = {"error": {"code": 500, "message": "Boom"}}
    monkeypatch.setattr(ac, "http_request_with_retry", lambda *a, **kw: _FakeResp(payload))
    import pytest

    with pytest.raises(RuntimeError):
        ac.fetch_page(
            MagicMock(),
            "https://x",
            156,
            out_fields="*",
            ts_field="fecha_carg",
            limit=10,
            offset=0,
        )


def test_fetch_page_handles_missing_geometry(monkeypatch):
    payload = {"features": [{"attributes": {"fiwareid": "A01", "fecha_carg": 0}}]}
    monkeypatch.setattr(ac, "http_request_with_retry", lambda *a, **kw: _FakeResp(payload))
    rows = ac.fetch_page(MagicMock(), "https://x", 156, out_fields="*", ts_field="fecha_carg", limit=10, offset=0)
    assert rows == [{"fiwareid": "A01", "fecha_carg": 0}]


# ---------------- shared producer helpers via ap module ----------------
class TestStatePersistence:
    """Using ap (air_producer) as the canonical implementation; weather mirrors it."""

    def test_save_and_load_state(self, tmp_path, monkeypatch):
        state_json = tmp_path / "state.json"
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(state_json))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(tmp_path / "offset.txt"))
        ap.save_state({"A01": "2025-10-18T18:00:00Z"}, {"A01": "fp"})
        offsets, fps = ap.load_state()
        assert offsets == {"A01": "2025-10-18T18:00:00Z"}
        assert fps == {"A01": "fp"}

    def test_load_state_legacy_offset_returns_empty(self, tmp_path, monkeypatch):
        # Pre-2.0 producers wrote /state/offset.txt only; the new format requires
        # /state/state.json so a stand-alone offset.txt yields empty per-station dicts.
        offset_file = tmp_path / "offset.txt"
        offset_file.write_text("2025-10-18T17:00:00Z", encoding="utf-8")
        monkeypatch.setattr(ap, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(ap, "STATE_JSON", str(tmp_path / "state.json"))
        monkeypatch.setattr(ap, "OFFSET_FILE", str(offset_file))
        offsets, fps = ap.load_state()
        assert offsets == {}
        assert fps == {}


class TestValueFingerprint:
    def test_deterministic(self):
        rec = {"so2": 1.0, "no2": 24.0, "o3": None, "co": None, "pm10": 16.0, "pm25": 7.0}
        assert ap.value_fingerprint(rec) == ap.value_fingerprint(rec)
        assert len(ap.value_fingerprint(rec)) == 40

    def test_changes_with_values(self):
        rec1 = {"so2": 1.0, "no2": 24.0, "pm10": 16.0, "pm25": 7.0, "o3": None, "co": None}
        rec2 = dict(rec1, no2=25.0)
        assert ap.value_fingerprint(rec1) != ap.value_fingerprint(rec2)


class TestGracefulShutdown:
    def test_signal_handler_registered_for_sigint(self):
        import signal

        handler = signal.getsignal(signal.SIGINT)
        assert callable(handler) and handler.__name__ == "_stop"

    def test_signal_handler_registered_for_sigterm(self):
        import signal

        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler) and handler.__name__ == "_stop"


# ---------------- avro round-trip on the local serializer ----------------
def test_local_avro_serializer_emits_confluent_wire_format():
    rec = {
        "fiwareid": "A01",
        "ts": 1760806800000,
        "so2": None,
        "no2": 24.0,
        "o3": None,
        "co": None,
        "pm10": 16.0,
        "pm25": 7.0,
        "air_quality_summary": "Buena",
        "lat": 39.1,
        "lon": -0.3,
    }
    blob = ap.local_avro_serializer(rec, ap.AIR_SCHEMA)
    # Confluent wire format: magic byte 0x00 + 4-byte schema id (0 for local)
    assert blob[:5] == b"\x00\x00\x00\x00\x00"
    assert len(blob) > 5  # something was actually appended


def test_record_round_trip_via_local_serializer():
    """Smoke: a mapped record passed through produce_all + local serializer makes
    a valid Avro payload when consumed back via fastavro."""
    import io as _io

    import fastavro

    rec = ap.map_record(
        {
            "fiwareid": "A10_OLIVERETA_60m",
            "fecha_carg": 1760806800000,
            "no2": 24.0,
            "pm10": 16.0,
            "pm25": 7.0,
            "calidad_am": "Buena",
            "lat": 39.469244,
            "lon": -0.405923,
        },
        ts_field="fecha_carg",
    )
    # Stripping internal helper fields the way produce_all does
    kafka_ev = {k: v for k, v in rec.items() if k not in ("_fp", "_ts_iso")}
    blob = ap.local_avro_serializer(kafka_ev, ap.AIR_SCHEMA)
    # Drop Confluent header (5 bytes), then read with fastavro
    decoded = fastavro.schemaless_reader(_io.BytesIO(blob[5:]), ap.AIR_SCHEMA)
    # ts comes back as a tz-aware datetime via the timestamp-millis logical type
    from datetime import datetime as _dt
    from datetime import timezone as _tz

    assert decoded["fiwareid"] == "A10_OLIVERETA_60m"
    assert decoded["ts"] == _dt(2025, 10, 18, 17, 0, tzinfo=_tz.utc)
    assert decoded["air_quality_summary"] == "Buena"
    assert decoded["no2"] == 24.0
    # Round-trip the JSON-stripped version too to confirm there's nothing internal lurking
    assert "_fp" not in decoded and "_ts_iso" not in decoded


# ---------------- choose_ts_field shared semantics ----------------
class TestChooseTsField:
    def test_honors_env_if_present(self, monkeypatch):
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "fecha_carg")
        assert ap.choose_ts_field(["id", "fecha_carg", "name"], None) == "fecha_carg"

    def test_falls_back_to_candidates(self, monkeypatch):
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
        monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
        assert ap.choose_ts_field(["id", "update_jcd", "name"], None) == "update_jcd"

    def test_infers_from_iso_string_sample(self, monkeypatch):
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
        monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
        sample = {"id": 1, "custom_date": "2025-10-18T17:00:00Z", "name": "test"}
        assert ap.choose_ts_field(["id", "custom_date", "name"], sample) == "custom_date"

    def test_infers_from_epoch_ms_sample(self, monkeypatch):
        monkeypatch.setattr(ap, "TIMESTAMP_FIELD", "nonexistent")
        monkeypatch.setattr(ap, "AUTO_TS_FIELD", True)
        sample = {"id": 1, "weird_ts": 1760806800000}
        assert ap.choose_ts_field(["id", "weird_ts"], sample) == "weird_ts"


# Smoke test for json import used in this module (and silencing unused-import linters).
def test_json_module_available():
    assert json.loads("{}") == {}
