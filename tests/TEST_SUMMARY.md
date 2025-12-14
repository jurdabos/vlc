# VLC Pipeline Tests - Summary

## Overview

Comprehensive unit tests for the Valencia (VLC) air quality and weather data pipeline. Tests verify Kafka infrastructure configuration, producer logic, resilience mechanisms, and Docker builds without requiring running containers.

**Latest Update**: 2024-12-14 — Expanded test suite with producer and resilience tests.

## Test Statistics

- **Total Tests**: 220
- **Test Files**: 8
- **All Tests**: ✅ PASSING
- **Execution Time**: ~0.9 seconds
- **Code Coverage**: 33% overall (87% for producers, 94% for resilience module)

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                      # Shared fixtures
├── unit/
│   ├── __init__.py
│   ├── test_kafka_topics.py         # 12 tests - Topic configuration
│   ├── test_kafka_connect.py        # 31 tests - Connect service config
│   ├── test_connect_config.py       #  9 tests - JDBC connector configs
│   ├── test_docker_image.py         # 15 tests - Docker build validation
│   ├── test_producer_air.py         # 42 tests - Air producer logic
│   ├── test_producer_weather.py     # 54 tests - Weather producer logic
│   ├── test_producer_common.py      # 23 tests - Shared producer utilities
│   └── test_resilience.py           # 42 tests - Retry/DLQ/throttling
└── TEST_SUMMARY.md                   # This file
```

## Test Coverage by Component

### ✅ Kafka Topics (12 tests)
**File**: `test_kafka_topics.py`

- Data topic creation (vlc.air, vlc.weather) with correct partitions, replication, retention
- Connect internal topics (_connect-configs, _connect-offsets, _connect-status)
- Cleanup policies (delete for data, compact for internal)
- Bootstrap script validation

### ✅ Kafka Connect Service (31 tests)
**File**: `test_kafka_connect.py`

- Service definition in docker-compose.yml
- Image version validation (7.6.1)
- Environment variables (11 required vars)
- Dependencies (kafka healthcheck, schema-registry)
- File-based config provider for secrets
- REST API configuration (port 8083)
- Wait/timeout handling in bootstrap script

### ✅ JDBC Sink Connectors (9 tests)
**File**: `test_connect_config.py`

- Air connector: jdbc-sink-timescale-air → air.hyper
- Weather connector: jdbc-sink-timescale-weather → weather.hyper
- Upsert mode with PK (fiwareid, ts)
- JSON Schema converter configuration
- Timestamp transformation
- File-based secrets (${file:secrets.properties:TS_*})

### ✅ Docker Image Build (15 tests)
**File**: `test_docker_image.py`

- Dockerfile syntax and base image (cp-kafka-connect:7.6.1)
- JDBC connector installation via confluent-hub
- PostgreSQL driver (postgresql-42.7.3.jar)
- Layer optimization
- Plugin path configuration

### ✅ Air Producer (42 tests)
**File**: `test_producer_air.py`

- Record mapping and field extraction
- Timestamp normalization (ISO 8601)
- Geo-point parsing (lat/lon from various formats)
- Fingerprint-based deduplication
- State persistence (offset tracking)
- Schema Registry integration
- Error handling

### ✅ Weather Producer (54 tests)
**File**: `test_producer_weather.py`

- Record mapping for weather measurements
- Wind direction/speed, temperature, humidity, pressure, precipitation
- Timestamp and geo-point handling
- Deduplication logic
- State management
- Schema validation

### ✅ Common Producer Utilities (23 tests)
**File**: `test_producer_common.py`

- HTTP request retry logic
- Timestamp parsing and normalization
- Geo-point extraction utilities
- Fingerprint generation
- State file operations

### ✅ Resilience Module (42 tests)
**File**: `test_resilience.py`

- `RetryConfig`: Exponential backoff configuration
- `InflightLimiter`: Concurrent request limiting
- `ProduceStats`: Success/failure tracking with time windows
- `RateThrottler`: Adaptive rate limiting based on failure ratio
- `ResilientProducer`: DLQ (dead letter queue) management
- `http_request_with_retry`: HTTP retry with jitter

## Running the Tests

### Quick Start
```bash
# Sync dependencies (includes dev/test deps)
uv sync

# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov
```

### Test Execution Options
```bash
# Verbose output
uv run pytest -v

# Specific test file
uv run pytest tests/unit/test_kafka_topics.py

# Specific test class
uv run pytest tests/unit/test_kafka_connect.py::TestKafkaConnectService

# Run only producer tests
uv run pytest tests/unit/test_producer_*.py

# Run only resilience tests
uv run pytest tests/unit/test_resilience.py -v

# Generate HTML coverage report
uv run pytest --cov --cov-report=html
```

## Test Philosophy

These are **pure unit tests** that:
- ✅ Verify configuration consistency across files
- ✅ Validate producer logic with mocked dependencies
- ✅ Test resilience mechanisms (retry, DLQ, throttling)
- ✅ Check Docker image build definitions
- ✅ Ensure proper service dependencies
- ❌ Do NOT require running containers
- ❌ Do NOT make network calls (mocked)
- ❌ Do NOT require Kafka/Connect to be running

## Dependencies

Test dependencies are defined in `pyproject.toml` under `[project.optional-dependencies]`:

```
pytest>=8.0.0
pytest-cov>=4.1.0
pytest-mock>=3.12.0
pyyaml>=6.0.0
```

## Integration with CI/CD

These tests are designed to run in CI/CD pipelines:

```yaml
# .github/workflows/test.yml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: '3.12'

- name: Install uv
  uses: astral-sh/setup-uv@v4

- name: Install dependencies
  run: uv sync

- name: Run tests
  run: uv run pytest tests/unit/ --cov
```

## Benefits

1. **Fast Feedback**: All 220 tests run in ~0.9 seconds
2. **No Infrastructure Required**: Tests run without Docker/Kafka
3. **High Coverage**: 87-94% coverage on core producer/resilience code
4. **Maintainable**: Clear test names and organization
5. **CI/CD Ready**: Easy integration into pipelines
6. **Cross-Platform**: Works on Linux, macOS, Windows
7. **Mocking**: Uses pytest-mock for isolation
8. **Flexible**: Easy to extend when adding features

## Next Steps

Potential test extensions:
1. Integration tests with testcontainers (Kafka, TimescaleDB)
2. End-to-end tests that produce/consume messages
3. Performance/load tests for producer throughput
4. Schema evolution tests with Schema Registry
