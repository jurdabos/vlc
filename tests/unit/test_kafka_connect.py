"""Unit tests for Kafka Connect service and connector registration."""

import json
import re
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest


class TestKafkaConnectService:
    """Tests for Kafka Connect service configuration and reachability."""

    def test_docker_compose_defines_connect_service(self, compose_file: Path):
        """Verifies that docker-compose.yml defines the Kafka Connect service."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        assert "connect" in compose_config["services"]

    def test_connect_service_uses_correct_image(self, compose_file: Path):
        """Verifies that the Connect service uses the custom-built Docker image."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]
        assert connect_service["image"] == "vlc/connect:7.6.1-jdbc-pg"

    def test_connect_service_builds_from_local_dockerfile(self, compose_file: Path):
        """Verifies that the Connect service builds from the local Dockerfile."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]
        assert connect_service["build"] == "./connect"

    def test_connect_service_depends_on_kafka(self, compose_file: Path):
        """Verifies that the Connect service depends on Kafka and waits for health check."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]
        assert "kafka" in connect_service["depends_on"]
        assert connect_service["depends_on"]["kafka"]["condition"] == "service_healthy"
        # Also depends on schema-registry
        assert "schema-registry" in connect_service["depends_on"]
        assert connect_service["depends_on"]["schema-registry"]["condition"] == "service_started"

    def test_connect_service_environment_variables(self, compose_file: Path, kafka_connect_env: Dict):
        """Verifies that the Connect service has all required environment variables."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_env = compose_config["services"]["connect"]["environment"]

        for key, expected_value in kafka_connect_env.items():
            assert key in connect_env, f"Missing environment variable: {key}"
            # Converting both to strings for comparison to handle integer values in YAML
            assert str(connect_env[key]) == str(expected_value), f"Incorrect value for {key}"

    def test_connect_service_has_rest_api_port_configured(self, compose_file: Path):
        """Verifies that the Connect service has REST API port configured in environment."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]
        # Port is configured via CONNECT_REST_PORT environment variable
        assert connect_service["environment"]["CONNECT_REST_PORT"] == 8083

    def test_bootstrap_script_waits_for_connect_service(self, bootstrap_script_path: Path):
        """Verifies that the bootstrap script waits for Kafka Connect to be reachable."""
        content = bootstrap_script_path.read_text()

        # Checking for wait_for_connect function
        assert "wait_for_connect()" in content

        # Checking for REST API health check
        assert "http_ok" in content
        assert "/connectors" in content

    def test_bootstrap_script_uses_correct_connect_url(self, bootstrap_script_path: Path):
        """Verifies that the bootstrap script uses the correct Connect URL."""
        content = bootstrap_script_path.read_text()

        # Checking for Connect URL - script uses localhost for running from host
        # The docker exec commands will run curl inside the connect container
        assert 'CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"' in content

    def test_bootstrap_script_handles_connect_unavailability(self, bootstrap_script_path: Path):
        """Verifies that the bootstrap script handles Connect service unavailability gracefully."""
        content = bootstrap_script_path.read_text()

        # Checking for timeout handling
        assert "for _ in $(seq 1 60)" in content

        # Checking for warning message when Connect is not reachable
        assert "WARN: Connect not reachable" in content

    def test_connect_service_has_file_config_provider(self, compose_file: Path):
        """Verifies that the Connect service has file-based config provider configured."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_env = compose_config["services"]["connect"]["environment"]

        assert connect_env["CONNECT_CONFIG_PROVIDERS"] == "file"
        assert connect_env["CONNECT_CONFIG_PROVIDERS_FILE_CLASS"] == (
            "org.apache.kafka.common.config.provider.FileConfigProvider"
        )
        assert connect_env["CONNECT_CONFIG_PROVIDERS_FILE_PARAM_PATH"] == "/opt/kafka/connect/secrets"

    def test_connect_service_mounts_secrets_volume(self, compose_file: Path):
        """Verifies that the Connect service mounts the secrets directory."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]
        volumes = connect_service["volumes"]

        # Checking for secrets volume mount
        assert any("./connect/secrets:/opt/kafka/connect/secrets" in vol for vol in volumes)


class TestJdbcSinkConnectorConfiguration:
    """Tests for JDBC Sink connector configurations."""

    def test_air_connector_config_exists(self, air_connector_config_path: Path):
        """Verifies that the air quality connector configuration file exists."""
        assert air_connector_config_path.exists()

    def test_weather_connector_config_exists(self, weather_connector_config_path: Path):
        """Verifies that the weather connector configuration file exists."""
        assert weather_connector_config_path.exists()

    def test_air_connector_has_correct_name(self, air_connector_config_path: Path):
        """Verifies that the air connector has the correct name."""
        with open(air_connector_config_path) as f:
            config = json.load(f)

        assert config["name"] == "jdbc-sink-timescale-air"

    def test_weather_connector_has_correct_name(self, weather_connector_config_path: Path):
        """Verifies that the weather connector has the correct name."""
        with open(weather_connector_config_path) as f:
            config = json.load(f)

        assert config["name"] == "jdbc-sink-timescale-weather"

    def test_air_connector_uses_jdbc_sink_class(self, air_connector_config_path: Path):
        """Verifies that the air connector uses the JDBC Sink connector class."""
        with open(air_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["connector.class"] == "io.confluent.connect.jdbc.JdbcSinkConnector"

    def test_weather_connector_uses_jdbc_sink_class(self, weather_connector_config_path: Path):
        """Verifies that the weather connector uses the JDBC Sink connector class."""
        with open(weather_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["connector.class"] == "io.confluent.connect.jdbc.JdbcSinkConnector"

    def test_air_connector_subscribes_to_correct_topic(self, air_connector_config_path: Path):
        """Verifies that the air connector subscribes to the valencia.air topic."""
        with open(air_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["topics"] == "vlc.air"

    def test_weather_connector_subscribes_to_correct_topic(self, weather_connector_config_path: Path):
        """Verifies that the weather connector subscribes to the valencia.weather topic."""
        with open(weather_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["topics"] == "vlc.weather"

    def test_air_connector_uses_file_config_for_connection(self, air_connector_config_path: Path):
        """Verifies that the air connector uses file-based config provider for database connection."""
        with open(air_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["connection.url"] == "${file:secrets.properties:TS_JDBC_URL}"
        assert config["config"]["connection.user"] == "${file:secrets.properties:TS_USERNAME}"
        assert config["config"]["connection.password"] == "${file:secrets.properties:TS_PASSWORD}"

    def test_weather_connector_uses_file_config_for_connection(self, weather_connector_config_path: Path):
        """Verifies that the weather connector uses file-based config provider for database connection."""
        with open(weather_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["connection.url"] == "${file:secrets.properties:TS_JDBC_URL}"
        assert config["config"]["connection.user"] == "${file:secrets.properties:TS_USERNAME}"
        assert config["config"]["connection.password"] == "${file:secrets.properties:TS_PASSWORD}"

    def test_air_connector_uses_upsert_mode(self, air_connector_config_path: Path):
        """Verifies that the air connector uses upsert insert mode."""
        with open(air_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["insert.mode"] == "upsert"
        assert config["config"]["pk.mode"] == "record_value"
        assert config["config"]["pk.fields"] == "fiwareid,ts"

    def test_weather_connector_uses_upsert_mode(self, weather_connector_config_path: Path):
        """Verifies that the weather connector uses upsert insert mode."""
        with open(weather_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["insert.mode"] == "upsert"
        assert config["config"]["pk.mode"] == "record_value"
        assert config["config"]["pk.fields"] == "fiwareid,ts"

    def test_air_connector_targets_correct_table(self, air_connector_config_path: Path):
        """Verifies that the air connector writes to the correct table."""
        with open(air_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["table.name.format"] == "air.hyper"

    def test_weather_connector_targets_correct_table(self, weather_connector_config_path: Path):
        """Verifies that the weather connector writes to the correct table."""
        with open(weather_connector_config_path) as f:
            config = json.load(f)

        assert config["config"]["table.name.format"] == "weather.hyper"

    def test_connectors_use_json_converter(self, air_connector_config_path: Path, weather_connector_config_path: Path):
        """Verifies that both connectors use JSON converter without schemas."""
        for config_path in [air_connector_config_path, weather_connector_config_path]:
            with open(config_path) as f:
                config = json.load(f)

            assert config["config"]["value.converter"] == "org.apache.kafka.connect.json.JsonConverter"
            assert config["config"]["value.converter.schemas.enable"] == "false"

    def test_connectors_have_timestamp_transformation(
        self,
        air_connector_config_path: Path,
        weather_connector_config_path: Path
    ):
        """Verifies that both connectors have timestamp transformation configured."""
        for config_path in [air_connector_config_path, weather_connector_config_path]:
            with open(config_path) as f:
                config = json.load(f)

            assert "transforms" in config["config"]
            assert config["config"]["transforms"] == "tsToTimestamp"
            assert config["config"]["transforms.tsToTimestamp.type"] == (
                "org.apache.kafka.connect.transforms.TimestampConverter$Value"
            )
            assert config["config"]["transforms.tsToTimestamp.field"] == "ts"
            assert config["config"]["transforms.tsToTimestamp.target.type"] == "Timestamp"

    def test_bootstrap_script_registers_air_connector(self, bootstrap_script_path: Path):
        """Verifies that the bootstrap script registers the air connector."""
        content = bootstrap_script_path.read_text()

        assert 'upsert_connector "connect/config/jdbc-sink.timescale.air.json"' in content

    def test_bootstrap_script_registers_weather_connector(self, bootstrap_script_path: Path):
        """Verifies that the bootstrap script registers the weather connector."""
        content = bootstrap_script_path.read_text()

        assert 'upsert_connector "connect/config/jdbc-sink.timescale.weather.json"' in content

    def test_bootstrap_script_upsert_function_handles_updates(self, bootstrap_script_path: Path):
        """Verifies that the upsert_connector function handles both POST and PUT."""
        content = bootstrap_script_path.read_text()

        # Checking for PUT request (update existing connector)
        assert "-X PUT" in content
        assert "/connectors/${name}/config" in content

        # Checking for fallback to POST if 404
        assert 'if [ "${code}" = "404" ]' in content
        assert "-X POST" in content
        assert "/connectors" in content
