"""Unit tests for Kafka topic configurations."""

import re
from pathlib import Path
from typing import Dict


class TestKafkaDataTopics:
    """Tests for Kafka data topics (valencia.air and valencia.weather)."""

    def test_bootstrap_script_creates_air_topic(self, bootstrap_script_path: Path, kafka_data_topic_config: Dict):
        """Verifies that the bootstrap script creates the valencia.air topic with correct configuration."""
        content = bootstrap_script_path.read_text()

        # Checking for topic name in environment variables
        assert 'DATA_TOPIC="${DATA_TOPIC:-vlc.air}"' in content

        # Checking topic creation call with correct parameters
        assert 'create_topic "${DATA_TOPIC}"' in content

        # Verifying partition count
        pattern = rf'DATA_PARTITIONS="\${{DATA_PARTITIONS:-{kafka_data_topic_config["partitions"]}}}"'
        assert re.search(pattern, content), "Expected partitions configuration not found"

        # Verifying replication factor
        pattern = rf'DATA_RF="\${{DATA_RF:-{kafka_data_topic_config["replication_factor"]}}}"'
        assert re.search(pattern, content), "Expected replication factor not found"

        # Verifying retention policy
        pattern = rf'DATA_RETENTION_MS="\${{DATA_RETENTION_MS:-{kafka_data_topic_config["retention_ms"]}}}"'
        assert re.search(pattern, content), "Expected retention configuration not found"

    def test_bootstrap_script_creates_weather_topic(self, bootstrap_script_path: Path, kafka_data_topic_config: Dict):
        """Verifies that the bootstrap script creates the valencia.weather topic with correct configuration."""
        content = bootstrap_script_path.read_text()

        # Checking for topic name in environment variables
        assert 'DATA_TOPIC_2="${DATA_TOPIC_2:-vlc.weather}"' in content

        # Checking topic creation call
        assert 'create_topic "${DATA_TOPIC_2}"' in content

    def test_data_topics_have_delete_cleanup_policy(self, bootstrap_script_path: Path):
        """Verifies that data topics use 'delete' cleanup policy."""
        content = bootstrap_script_path.read_text()

        # Checking for cleanup.policy=delete in topic creation
        assert "--config cleanup.policy=delete" in content
        assert "--config retention.ms=${DATA_RETENTION_MS}" in content

    def test_data_topics_use_same_configuration(self, bootstrap_script_path: Path):
        """Verifies that both data topics use the same partition and replication configuration."""
        content = bootstrap_script_path.read_text()

        # Both topics should use same variables
        data_topic_1_pattern = r'create_topic "\$\{DATA_TOPIC\}"\s+"\$\{DATA_PARTITIONS\}"\s+"\$\{DATA_RF\}"'
        data_topic_2_pattern = r'create_topic "\$\{DATA_TOPIC_2\}"\s+"\$\{DATA_PARTITIONS\}"\s+"\$\{DATA_RF\}"'

        assert re.search(data_topic_1_pattern, content), "valencia.air topic not configured correctly"
        assert re.search(data_topic_2_pattern, content), "valencia.weather topic not configured correctly"

    def test_bootstrap_script_verifies_topic_creation(self, bootstrap_script_path: Path):
        """Verifies that the bootstrap script confirms topic creation."""
        content = bootstrap_script_path.read_text()

        # Checking for kafka-topics --describe command to verify creation
        assert "kafka-topics --bootstrap-server ${BOOTSTRAP} --describe" in content
        # Script describes each topic separately
        assert "--topic ${DATA_TOPIC}" in content
        assert "--topic ${DATA_TOPIC_2}" in content


class TestKafkaConnectInternalTopics:
    """Tests for Kafka Connect internal topics (_connect_configs, _connect_offsets, _connect_status)."""

    def test_bootstrap_script_creates_connect_config_topic(
        self, bootstrap_script_path: Path, kafka_connect_internal_topic_config: Dict
    ):
        """Verifies that _connect-configs topic is created with correct configuration."""
        content = bootstrap_script_path.read_text()

        # Checking for topic name (underscore prefix for internal topics)
        assert 'CFG_TOPIC="${CFG_TOPIC:-_connect-configs}"' in content

        # Checking topic creation with single partition and compact policy
        pattern = r'create_topic "\$\{CFG_TOPIC\}" 1 "\$\{DATA_RF\}" "--config cleanup\.policy=compact"'
        assert re.search(pattern, content), "_connect-configs topic not configured correctly"

    def test_bootstrap_script_creates_connect_offset_topic(
        self, bootstrap_script_path: Path, kafka_connect_internal_topic_config: Dict
    ):
        """Verifies that _connect-offsets topic is created with correct configuration."""
        content = bootstrap_script_path.read_text()

        # Checking for topic name (underscore prefix for internal topics)
        assert 'OFF_TOPIC="${OFF_TOPIC:-_connect-offsets}"' in content

        # Checking topic creation with single partition and compact policy
        pattern = r'create_topic "\$\{OFF_TOPIC\}" 1 "\$\{DATA_RF\}" "--config cleanup\.policy=compact"'
        assert re.search(pattern, content), "_connect-offsets topic not configured correctly"

    def test_bootstrap_script_creates_connect_status_topic(
        self, bootstrap_script_path: Path, kafka_connect_internal_topic_config: Dict
    ):
        """Verifies that _connect-status topic is created with correct configuration."""
        content = bootstrap_script_path.read_text()

        # Checking for topic name (underscore prefix for internal topics)
        assert 'STS_TOPIC="${STS_TOPIC:-_connect-status}"' in content

        # Checking topic creation with single partition and compact policy
        pattern = r'create_topic "\$\{STS_TOPIC\}" 1 "\$\{DATA_RF\}" "--config cleanup\.policy=compact"'
        assert re.search(pattern, content), "_connect-status topic not configured correctly"

    def test_internal_topics_use_single_partition(self, bootstrap_script_path: Path):
        """Verifies that all Connect internal topics use a single partition."""
        content = bootstrap_script_path.read_text()

        # All internal topics should be created with partition count of 1
        internal_topic_patterns = [
            r'create_topic "\$\{CFG_TOPIC\}" 1 ',
            r'create_topic "\$\{OFF_TOPIC\}" 1 ',
            r'create_topic "\$\{STS_TOPIC\}" 1 ',
        ]

        for pattern in internal_topic_patterns:
            assert re.search(pattern, content), f"Pattern {pattern} not found"

    def test_internal_topics_use_compact_cleanup_policy(self, bootstrap_script_path: Path):
        """Verifies that all Connect internal topics use compact cleanup policy."""
        content = bootstrap_script_path.read_text()

        # Counting occurrences of compact policy for internal topics
        compact_policy_count = content.count("--config cleanup.policy=compact")

        # Should have exactly 3 (one for each internal topic)
        assert compact_policy_count == 3, f"Expected 3 compact policies, found {compact_policy_count}"

    def test_docker_compose_references_internal_topics(self, compose_file: Path):
        """Verifies that docker-compose.yml references the internal topics correctly."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_env = compose_config["services"]["connect"]["environment"]

        assert connect_env["CONNECT_CONFIG_STORAGE_TOPIC"] == "_connect-configs"
        assert connect_env["CONNECT_OFFSET_STORAGE_TOPIC"] == "_connect-offsets"
        assert connect_env["CONNECT_STATUS_STORAGE_TOPIC"] == "_connect-status"
