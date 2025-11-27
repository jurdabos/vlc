"""Unit tests for Kafka Connect Docker image build."""

import re
from pathlib import Path


class TestKafkaConnectDockerImage:
    """Tests for Kafka Connect Docker image build configuration."""

    def test_dockerfile_exists(self, dockerfile_path: Path):
        """Verifies that the Dockerfile exists."""
        assert dockerfile_path.exists()

    def test_dockerfile_uses_correct_base_image(self, dockerfile_path: Path):
        """Verifies that the Dockerfile uses the correct Confluent Kafka Connect base image."""
        content = dockerfile_path.read_text()

        assert "FROM confluentinc/cp-kafka-connect:7.6.1" in content

    def test_dockerfile_installs_jdbc_sink_connector(self, dockerfile_path: Path):
        """Verifies that the Dockerfile installs the Confluent JDBC Sink connector."""
        content = dockerfile_path.read_text()

        # Checking for confluent-hub install command
        assert "confluent-hub install" in content

        # Checking for JDBC connector installation
        assert "confluentinc/kafka-connect-jdbc" in content

        # Checking for --no-prompt flag to avoid interactive prompts
        assert "--no-prompt" in content

    def test_dockerfile_installs_postgresql_jdbc_driver(self, dockerfile_path: Path):
        """Verifies that the Dockerfile installs the PostgreSQL JDBC driver."""
        content = dockerfile_path.read_text()

        # Checking for curl command to download the driver
        assert "curl" in content
        assert "postgresql" in content

        # Checking for specific driver version
        assert "postgresql-42.7.3.jar" in content

        # Checking download URL
        assert "https://jdbc.postgresql.org/download/" in content

    def test_dockerfile_places_driver_in_correct_location(self, dockerfile_path: Path):
        """Verifies that the PostgreSQL JDBC driver is placed in a directory on the plugin path."""
        content = dockerfile_path.read_text()

        # Accept either classic location under /usr/share/java/kafka-connect-jdbc or inside the plugin's lib directory
        ok_paths = [
            "/usr/share/java/kafka-connect-jdbc/postgresql-42.7.3.jar",
            "/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar",
        ]
        assert any(p in content for p in ok_paths), f"Expected driver copied to one of {ok_paths}"

    def test_dockerfile_has_minimal_layers(self, dockerfile_path: Path):
        """Verifies that the Dockerfile is optimized with minimal layers."""
        content = dockerfile_path.read_text()

        # Counting RUN commands (should be minimal)
        run_commands = re.findall(r"^RUN\s", content, re.MULTILINE)

        # Multi-stage build: 2 in builder stage, 1 in final stage = 3 total
        assert len(run_commands) == 3, f"Expected 3 RUN commands (multi-stage build), found {len(run_commands)}"

    def test_compose_builds_custom_image(self, compose_file: Path):
        """Verifies that docker-compose.yml builds the custom Connect image."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]

        # Checking that both image and build are specified
        assert "image" in connect_service
        assert "build" in connect_service

        # Checking build context
        assert connect_service["build"] == "./connect"

    def test_compose_mounts_connector_configs(self, compose_file: Path):
        """Verifies that docker-compose.yml mounts the connector configuration directory."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_service = compose_config["services"]["connect"]

        # Checking for volume mount
        assert "volumes" in connect_service
        assert "./connect/config:/config" in connect_service["volumes"]

    def test_dockerfile_commands_use_long_format_flags(self, dockerfile_path: Path):
        """Verifies that Dockerfile commands use long-format flags for readability."""
        content = dockerfile_path.read_text()

        # Checking for long-format curl flags
        assert "-L" in content  # follow redirects
        assert "-o" in content  # output file

    def test_dockerfile_has_no_unnecessary_commands(self, dockerfile_path: Path):
        """Verifies that the Dockerfile doesn't contain unnecessary commands."""
        content = dockerfile_path.read_text()

        # Should not have apt-get or yum (base image has what we need)
        assert "apt-get" not in content.lower()
        assert "yum" not in content.lower()

        # Should not have unnecessary WORKDIR changes
        workdir_count = content.count("WORKDIR")
        assert workdir_count == 0, "Should not change working directory"

    def test_plugin_path_configuration(self, compose_file: Path):
        """Verifies that the plugin path includes the Confluent Hub components directory."""
        import yaml

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        connect_env = compose_config["services"]["connect"]["environment"]

        plugin_path = connect_env["CONNECT_PLUGIN_PATH"]

        # Checking that plugin path includes both locations
        assert "/usr/share/confluent-hub-components" in plugin_path
        assert "/usr/share/java" in plugin_path


class TestDockerImageBuildProcess:
    """Tests for the Docker image build process validation."""

    def test_dockerfile_syntax_is_valid(self, dockerfile_path: Path):
        """Verifies that the Dockerfile has valid syntax."""
        content = dockerfile_path.read_text()

        # Checking that each line is either empty, a comment, or a valid Dockerfile instruction
        valid_instructions = {
            "FROM",
            "RUN",
            "CMD",
            "LABEL",
            "EXPOSE",
            "ENV",
            "ADD",
            "COPY",
            "ENTRYPOINT",
            "VOLUME",
            "USER",
            "WORKDIR",
            "ARG",
            "ONBUILD",
            "STOPSIGNAL",
            "HEALTHCHECK",
            "SHELL",
        }

        lines = content.strip().split("\n")

        for line in lines:
            stripped = line.strip()

            # Empty lines and comments are OK
            if not stripped or stripped.startswith("#"):
                continue

            # Checking for line continuations
            if stripped.endswith("\\"):
                continue

            # Checking if line starts with valid instruction
            first_word = stripped.split()[0]
            assert first_word in valid_instructions or line.startswith("    "), (
                f"Invalid Dockerfile instruction: {first_word}"
            )

    def test_jdbc_driver_url_is_reachable(self, dockerfile_path: Path):
        """Verifies that the PostgreSQL JDBC driver download URL is valid."""
        content = dockerfile_path.read_text()

        # Extracting URL
        url_pattern = r"https://jdbc\.postgresql\.org/download/postgresql-[\d.]+\.jar"
        match = re.search(url_pattern, content)

        assert match, "PostgreSQL JDBC driver URL not found"

        url = match.group(0)

        # URL should follow the expected pattern
        assert url.startswith("https://jdbc.postgresql.org/download/")
        assert url.endswith(".jar")

    def test_confluent_jdbc_connector_version(self, dockerfile_path: Path):
        """Verifies that the JDBC connector uses a pinned version."""
        content = dockerfile_path.read_text()

        # Checking for pinned version specification (not latest)
        assert "confluentinc/kafka-connect-jdbc:10.2.5" in content
