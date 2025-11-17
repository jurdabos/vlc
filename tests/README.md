# Kafka Infrastructure Tests

This directory contains unit tests for the Valencia Kafka infrastructure components.

## Test Organization

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── unit/
│   ├── test_kafka_topics.py     # Tests for Kafka topic configurations
│   ├── test_kafka_connect.py    # Tests for Kafka Connect service and connectors
│   └── test_docker_image.py     # Tests for Docker image build
└── README.md
```

## Test Coverage

### 1. Kafka Data Topics (`test_kafka_topics.py`)
- Verifies `valencia.air` topic configuration (partitions, replication, retention)
- Verifies `valencia.weather` topic configuration
- Validates delete cleanup policy and retention settings
- Confirms topics are created via bootstrap script

### 2. Kafka Connect Internal Topics (`test_kafka_topics.py`)
- Verifies `_connect_configs` topic configuration
- Verifies `_connect_offsets` topic configuration
- Verifies `_connect_status` topic configuration
- Validates single partition and compact cleanup policy
- Confirms proper references in docker-compose.yml

### 3. Kafka Connect Service (`test_kafka_connect.py`)
- Verifies Kafka Connect service is defined in docker-compose.yml
- Validates correct Docker image and build configuration
- Checks service dependencies and health checks
- Validates environment variables
- Verifies REST API port exposure (8083)
- Tests bootstrap script waits for service reachability

### 4. JDBC Sink Connectors (`test_kafka_connect.py`)
- Verifies air quality connector configuration exists
- Verifies weather connector configuration exists
- Validates connector names and classes
- Checks topic subscriptions (vlc.air, vlc.weather)
- Validates database connection settings (env vars)
- Confirms upsert mode with correct primary keys
- Verifies table targeting (air.hyper, weather.hyper)
- Validates JSON converter configuration
- Checks timestamp transformation settings
- Confirms bootstrap script registers both connectors

### 5. Docker Image Build (`test_docker_image.py`)
- Verifies Dockerfile exists and uses correct base image
- Validates Confluent JDBC Sink connector installation
- Confirms PostgreSQL JDBC driver installation
- Checks driver placement in correct directory
- Validates Dockerfile optimization (minimal layers)
- Confirms plugin path configuration
- Validates Dockerfile syntax

## Running Tests

### Install Dependencies

Using `uv`:
```powershell
uv pip install -e ".[dev]"
```

Or using pip:
```powershell
pip install -e ".[dev]"
```

### Run All Tests

```powershell
pytest
```

### Run Specific Test Files

```powershell
# Topic tests only
pytest tests/unit/test_kafka_topics.py

# Kafka Connect tests only
pytest tests/unit/test_kafka_connect.py

# Docker image tests only
pytest tests/unit/test_docker_image.py
```

### Run Tests with Coverage

```powershell
pytest --cov
```

### Run Tests Verbosely

```powershell
pytest -v
```

### Run Specific Test Cases

```powershell
# Run a specific test class
pytest tests/unit/test_kafka_topics.py::TestKafkaDataTopics

# Run a specific test method
pytest tests/unit/test_kafka_topics.py::TestKafkaDataTopics::test_bootstrap_script_creates_air_topic
```

## Test Requirements

- Python 3.10+
- pytest 8.0.0+
- PyYAML 6.0.0+
- All test dependencies listed in `pyproject.toml`

## Test Philosophy

These are **unit tests** that verify the **configuration and structure** of the Kafka infrastructure components:

- They do NOT require running Docker containers
- They do NOT require actual Kafka or Kafka Connect instances
- They validate configuration files, scripts, and Docker definitions
- They ensure consistency across multiple configuration sources

For integration tests that verify actual runtime behavior, see the `integration/` directory (if available).

## Continuous Integration

These tests should be run in CI/CD pipelines before deploying infrastructure changes.

Example GitHub Actions workflow:
```yaml
- name: Install dependencies
  run: uv pip install -e ".[dev]"

- name: Run unit tests
  run: pytest tests/unit/
```

## Troubleshooting

### Import Errors
If you see import errors, ensure you've installed the package in development mode:
```powershell
uv pip install -e ".[dev]"
```

### YAML Parser Errors
Ensure PyYAML is installed:
```powershell
uv add pyyaml --dev
```

### Path Errors on Windows
The tests use `pathlib.Path` which handles Windows paths correctly. If you see path-related errors, ensure you're running from the project root directory.
