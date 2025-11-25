"""Unit tests for Kafka Connect configuration validation."""
import json
from pathlib import Path

import pytest

try:
    import jsonschema
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False

# Paths
CONFIG_DIR = Path(__file__).parents[2] / "connect" / "config"
SCHEMA_FILE = CONFIG_DIR / "jdbc-sink.schema.json"


@pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestConnectConfigSchema:
    """Tests for Kafka Connect JDBC sink configuration validation."""

    @pytest.fixture
    def schema(self):
        """Loads the JSON schema for JDBC sink connectors."""
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_schema_file_exists(self):
        """Verifies the JSON schema file exists."""
        assert SCHEMA_FILE.exists(), f"Schema file not found: {SCHEMA_FILE}"

    def test_air_config_valid(self, schema):
        """Verifies air connector config is valid against schema."""
        config_file = CONFIG_DIR / "jdbc-sink.timescale.air.json"
        assert config_file.exists(), f"Config file not found: {config_file}"
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
        jsonschema.validate(instance=config, schema=schema)

    def test_weather_config_valid(self, schema):
        """Verifies weather connector config is valid against schema."""
        config_file = CONFIG_DIR / "jdbc-sink.timescale.weather.json"
        assert config_file.exists(), f"Config file not found: {config_file}"
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
        jsonschema.validate(instance=config, schema=schema)

    def test_all_configs_have_required_fields(self, schema):
        """Verifies all connector configs have required fields."""
        config_files = list(CONFIG_DIR.glob("jdbc-sink.timescale.*.json"))
        assert len(config_files) >= 2, "Expected at least 2 connector configs"
        for config_file in config_files:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            # Checking required top-level fields
            assert "name" in config, f"{config_file.name}: missing 'name'"
            assert "config" in config, f"{config_file.name}: missing 'config'"
            # Checking required config fields
            cfg = config["config"]
            assert cfg.get("insert.mode") == "upsert", \
                f"{config_file.name}: insert.mode should be 'upsert' for idempotent writes"
            assert cfg.get("pk.fields") == "fiwareid,ts", \
                f"{config_file.name}: pk.fields should be 'fiwareid,ts'"
            assert cfg.get("auto.create") == "false", \
                f"{config_file.name}: auto.create should be 'false' for hypertables"

    def test_configs_use_secret_references(self, schema):
        """Verifies configs use secret file references, not hardcoded credentials."""
        config_files = list(CONFIG_DIR.glob("jdbc-sink.timescale.*.json"))
        for config_file in config_files:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            cfg = config["config"]
            # Credentials should use ${file:...} syntax
            assert "${file:" in cfg.get("connection.url", ""), \
                f"{config_file.name}: connection.url should use secret reference"
            assert "${file:" in cfg.get("connection.user", ""), \
                f"{config_file.name}: connection.user should use secret reference"
            assert "${file:" in cfg.get("connection.password", ""), \
                f"{config_file.name}: connection.password should use secret reference"

    def test_topic_matches_table(self, schema):
        """Verifies topic name matches target table schema."""
        config_files = list(CONFIG_DIR.glob("jdbc-sink.timescale.*.json"))
        for config_file in config_files:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            cfg = config["config"]
            topic = cfg.get("topics", "")
            table = cfg.get("table.name.format", "")
            # vlc.air -> air.hyper, vlc.weather -> weather.hyper
            if topic == "vlc.air":
                assert table == "air.hyper", \
                    f"{config_file.name}: topic vlc.air should map to air.hyper"
            elif topic == "vlc.weather":
                assert table == "weather.hyper", \
                    f"{config_file.name}: topic vlc.weather should map to weather.hyper"


class TestConnectConfigJson:
    """Basic JSON validity tests (no jsonschema dependency)."""

    def test_all_configs_are_valid_json(self):
        """Verifies all config files are valid JSON."""
        config_files = list(CONFIG_DIR.glob("*.json"))
        for config_file in config_files:
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    json.load(f)
            except json.JSONDecodeError as e:
                pytest.fail(f"{config_file.name}: Invalid JSON - {e}")

    def test_config_files_exist(self):
        """Verifies expected config files exist."""
        expected = ["jdbc-sink.timescale.air.json", "jdbc-sink.timescale.weather.json"]
        for name in expected:
            assert (CONFIG_DIR / name).exists(), f"Missing config: {name}"
