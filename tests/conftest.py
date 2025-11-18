"""Shared fixtures for Kafka infrastructure tests."""

from pathlib import Path
from typing import Dict

import pytest


@pytest.fixture
def project_root() -> Path:
    """Returns the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def compose_file(project_root: Path) -> Path:
    """Returns path to docker-compose.yml."""
    return project_root / "compose" / "docker-compose.yml"


@pytest.fixture
def dockerfile_path(project_root: Path) -> Path:
    """Returns path to Kafka Connect Dockerfile."""
    return project_root / "connect" / "Dockerfile"


@pytest.fixture
def air_connector_config_path(project_root: Path) -> Path:
    """Returns path to air quality JDBC sink connector configuration."""
    return project_root / "connect" / "config" / "jdbc-sink.timescale.air.json"


@pytest.fixture
def weather_connector_config_path(project_root: Path) -> Path:
    """Returns path to weather JDBC sink connector configuration."""
    return project_root / "connect" / "config" / "jdbc-sink.timescale.weather.json"


@pytest.fixture
def bootstrap_script_path(project_root: Path) -> Path:
    """Returns path to Kafka bootstrap script."""
    return project_root / "scripts" / "bootstrap_kafka.sh"


@pytest.fixture
def kafka_data_topic_config() -> Dict[str, any]:
    """Returns expected configuration for data topics."""
    return {
        "topics": ["valencia.air", "valencia.weather"],
        "partitions": 3,
        "replication_factor": 1,
        "retention_ms": 2592000000,  # 30 days
        "cleanup_policy": "delete"
    }


@pytest.fixture
def kafka_connect_internal_topic_config() -> Dict[str, any]:
    """Returns expected configuration for Kafka Connect internal topics."""
    return {
        "topics": ["_connect-configs", "_connect-offsets", "_connect-status"],
        "partitions": 1,
        "replication_factor": 1,
        "cleanup_policy": "compact"
    }


@pytest.fixture
def kafka_connect_env() -> Dict[str, str]:
    """Returns expected Kafka Connect environment variables."""
    return {
        "CONNECT_BOOTSTRAP_SERVERS": "kafka:9092",
        "CONNECT_REST_ADVERTISED_HOST_NAME": "connect",
        "CONNECT_REST_PORT": "8083",
        "CONNECT_GROUP_ID": "vlc-connect",
        "CONNECT_CONFIG_STORAGE_TOPIC": "_connect-configs",
        "CONNECT_OFFSET_STORAGE_TOPIC": "_connect-offsets",
        "CONNECT_STATUS_STORAGE_TOPIC": "_connect-status",
        "CONNECT_KEY_CONVERTER": "org.apache.kafka.connect.storage.StringConverter",
        "CONNECT_VALUE_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
        "CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE": "false",
        "CONNECT_PLUGIN_PATH": "/usr/share/confluent-hub-components,/usr/share/java"
    }
