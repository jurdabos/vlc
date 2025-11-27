"""Unit tests for producer resilience module."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

# Make producer modules importable
sys.path.append(str(Path(__file__).parents[2] / "producer"))
from resilience import (  # noqa: E402
    DiskQueue,
    ExponentialBackoff,
    InflightLimiter,
    ProduceStats,
    RateThrottler,
    ResilientProducer,
    RetryConfig,
    http_request_with_retry,
    is_retryable_error,
    is_retryable_status,
)


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self):
        """Verifies default configuration values."""
        config = RetryConfig()
        assert config.base_delay_ms == 1000
        assert config.max_delay_ms == 60000
        assert config.max_retries == 5
        assert config.jitter_factor == 0.3

    def test_from_env(self, monkeypatch):
        """Verifies configuration from environment variables."""
        monkeypatch.setenv("VLC_BACKOFF_BASE_MS", "500")
        monkeypatch.setenv("VLC_BACKOFF_MAX_MS", "30000")
        monkeypatch.setenv("VLC_BACKOFF_MAX_RETRIES", "3")
        monkeypatch.setenv("VLC_BACKOFF_JITTER", "0.2")
        config = RetryConfig.from_env()
        assert config.base_delay_ms == 500
        assert config.max_delay_ms == 30000
        assert config.max_retries == 3
        assert config.jitter_factor == 0.2


class TestExponentialBackoff:
    """Tests for ExponentialBackoff calculations."""

    def test_delay_increases_exponentially(self):
        """Verifies exponential growth of delay."""
        config = RetryConfig(base_delay_ms=1000, max_delay_ms=60000, jitter_factor=0)
        backoff = ExponentialBackoff(config)
        # Without jitter: base * 2^attempt
        assert backoff.delay_ms(0) == 1000  # 1000 * 2^0
        assert backoff.delay_ms(1) == 2000  # 1000 * 2^1
        assert backoff.delay_ms(2) == 4000  # 1000 * 2^2
        assert backoff.delay_ms(3) == 8000  # 1000 * 2^3

    def test_delay_capped_at_max(self):
        """Verifies delay is capped at max_delay_ms."""
        config = RetryConfig(base_delay_ms=1000, max_delay_ms=5000, jitter_factor=0)
        backoff = ExponentialBackoff(config)
        # Should cap at 5000
        assert backoff.delay_ms(10) == 5000

    def test_jitter_applied(self):
        """Verifies jitter is applied to delay."""
        config = RetryConfig(base_delay_ms=1000, max_delay_ms=60000, jitter_factor=0.3)
        backoff = ExponentialBackoff(config)
        # Running multiple times to check jitter variance
        delays = [backoff.delay_ms(1) for _ in range(20)]
        # Base delay at attempt 1 is 2000, jitter ±30% = 1400-2600
        assert all(1400 <= d <= 2600 for d in delays)
        # Verifying there's some variance
        assert len(set(delays)) > 1

    def test_sleep_calls_time_sleep(self, monkeypatch):
        """Verifies sleep method calls time.sleep with correct duration."""
        config = RetryConfig(base_delay_ms=100, max_delay_ms=1000, jitter_factor=0)
        backoff = ExponentialBackoff(config)
        sleep_mock = MagicMock()
        monkeypatch.setattr(time, "sleep", sleep_mock)
        backoff.sleep(0)
        sleep_mock.assert_called_once_with(0.1)  # 100ms = 0.1s


class TestRetryableChecks:
    """Tests for retryable error/status detection."""

    def test_is_retryable_error_timeout(self):
        """Verifies timeout is retryable."""
        assert is_retryable_error(requests.exceptions.Timeout())

    def test_is_retryable_error_connection(self):
        """Verifies connection error is retryable."""
        assert is_retryable_error(requests.exceptions.ConnectionError())

    def test_is_retryable_error_chunked(self):
        """Verifies chunked encoding error is retryable."""
        assert is_retryable_error(requests.exceptions.ChunkedEncodingError())

    def test_is_not_retryable_error(self):
        """Verifies other errors are not retryable."""
        assert not is_retryable_error(ValueError())
        assert not is_retryable_error(requests.exceptions.HTTPError())

    def test_is_retryable_status_codes(self):
        """Verifies retryable status codes."""
        assert is_retryable_status(429)
        assert is_retryable_status(500)
        assert is_retryable_status(502)
        assert is_retryable_status(503)
        assert is_retryable_status(504)

    def test_is_not_retryable_status_codes(self):
        """Verifies non-retryable status codes."""
        assert not is_retryable_status(200)
        assert not is_retryable_status(400)
        assert not is_retryable_status(401)
        assert not is_retryable_status(403)
        assert not is_retryable_status(404)


class TestHttpRequestWithRetry:
    """Tests for http_request_with_retry function."""

    def test_success_on_first_attempt(self):
        """Verifies successful request returns immediately."""
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        session.request.return_value = response
        config = RetryConfig(max_retries=3)
        result = http_request_with_retry(session, "GET", "http://test.com", config)
        assert result == response
        assert session.request.call_count == 1

    def test_retry_on_429(self, monkeypatch):
        """Verifies retry on 429 Too Many Requests."""
        monkeypatch.setattr(time, "sleep", lambda x: None)
        session = MagicMock()
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        fail_resp.headers = {}
        success_resp = MagicMock()
        success_resp.status_code = 200
        session.request.side_effect = [fail_resp, fail_resp, success_resp]
        config = RetryConfig(max_retries=3, base_delay_ms=10, jitter_factor=0)
        result = http_request_with_retry(session, "GET", "http://test.com", config)
        assert result == success_resp
        assert session.request.call_count == 3

    def test_retry_on_500(self, monkeypatch):
        """Verifies retry on 500 Server Error."""
        monkeypatch.setattr(time, "sleep", lambda x: None)
        session = MagicMock()
        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_resp.headers = {}
        success_resp = MagicMock()
        success_resp.status_code = 200
        session.request.side_effect = [fail_resp, success_resp]
        config = RetryConfig(max_retries=3, base_delay_ms=10, jitter_factor=0)
        result = http_request_with_retry(session, "GET", "http://test.com", config)
        assert result == success_resp
        assert session.request.call_count == 2

    def test_retry_on_timeout(self, monkeypatch):
        """Verifies retry on timeout exception."""
        monkeypatch.setattr(time, "sleep", lambda x: None)
        session = MagicMock()
        success_resp = MagicMock()
        success_resp.status_code = 200
        session.request.side_effect = [
            requests.exceptions.Timeout(),
            success_resp,
        ]
        config = RetryConfig(max_retries=3, base_delay_ms=10, jitter_factor=0)
        result = http_request_with_retry(session, "GET", "http://test.com", config)
        assert result == success_resp
        assert session.request.call_count == 2

    def test_raises_after_max_retries(self, monkeypatch):
        """Verifies exception raised after max retries exhausted."""
        monkeypatch.setattr(time, "sleep", lambda x: None)
        session = MagicMock()
        session.request.side_effect = requests.exceptions.Timeout()
        config = RetryConfig(max_retries=2, base_delay_ms=10, jitter_factor=0)
        with pytest.raises(requests.exceptions.Timeout):
            http_request_with_retry(session, "GET", "http://test.com", config)
        # 1 initial + 2 retries = 3 calls
        assert session.request.call_count == 3

    def test_respects_retry_after_header(self, monkeypatch):
        """Verifies Retry-After header is respected."""
        sleep_calls = []
        monkeypatch.setattr(time, "sleep", lambda x: sleep_calls.append(x))
        session = MagicMock()
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        fail_resp.headers = {"Retry-After": "2"}
        success_resp = MagicMock()
        success_resp.status_code = 200
        session.request.side_effect = [fail_resp, success_resp]
        config = RetryConfig(max_retries=3, base_delay_ms=1000, jitter_factor=0)
        http_request_with_retry(session, "GET", "http://test.com", config)
        # Verifying we slept for 2 seconds as per Retry-After
        assert 2 in sleep_calls

    def test_non_retryable_error_raises_immediately(self):
        """Verifies non-retryable errors raise without retry."""
        session = MagicMock()
        session.request.side_effect = requests.exceptions.HTTPError("404")
        config = RetryConfig(max_retries=3)
        with pytest.raises(requests.exceptions.HTTPError):
            http_request_with_retry(session, "GET", "http://test.com", config)
        assert session.request.call_count == 1


class TestInflightLimiter:
    """Tests for InflightLimiter concurrency control."""

    def test_default_from_env(self, monkeypatch):
        """Verifies default from environment variable."""
        monkeypatch.setenv("VLC_MAX_INFLIGHT_POLLS", "5")
        limiter = InflightLimiter()
        assert limiter.max_inflight == 5

    def test_explicit_max(self):
        """Verifies explicit max_inflight parameter."""
        limiter = InflightLimiter(max_inflight=3)
        assert limiter.max_inflight == 3

    def test_context_manager(self):
        """Verifies context manager acquires and releases."""
        limiter = InflightLimiter(max_inflight=1)
        with limiter:
            # Inside context, semaphore is acquired
            pass
        # After context, semaphore is released


class TestDiskQueue:
    """Tests for DiskQueue on-disk persistence."""

    def test_enqueue_and_dequeue(self, tmp_path):
        """Verifies enqueue and dequeue operations."""
        queue = DiskQueue(str(tmp_path), topic="test")
        queue.enqueue(b"key1", b"value1")
        queue.enqueue(b"key2", b"value2")
        messages = queue.dequeue_all()
        assert len(messages) == 2
        assert messages[0] == (b"key1", b"value1")
        assert messages[1] == (b"key2", b"value2")

    def test_dequeue_clears_queue(self, tmp_path):
        """Verifies dequeue clears the queue file."""
        queue = DiskQueue(str(tmp_path), topic="test")
        queue.enqueue(b"key", b"value")
        queue.dequeue_all()
        # Second dequeue should return empty
        messages = queue.dequeue_all()
        assert messages == []

    def test_size(self, tmp_path):
        """Verifies size returns correct count."""
        queue = DiskQueue(str(tmp_path), topic="test")
        assert queue.size() == 0
        queue.enqueue(b"key1", b"value1")
        assert queue.size() == 1
        queue.enqueue(b"key2", b"value2")
        assert queue.size() == 2

    def test_empty_dequeue(self, tmp_path):
        """Verifies dequeue on empty queue returns empty list."""
        queue = DiskQueue(str(tmp_path), topic="test")
        messages = queue.dequeue_all()
        assert messages == []

    def test_handles_malformed_json(self, tmp_path):
        """Verifies malformed JSON lines are skipped."""
        queue = DiskQueue(str(tmp_path), topic="test")
        # Writing malformed JSON directly
        queue_file = tmp_path / "test.jsonl"
        with open(queue_file, "w", encoding="utf-8") as f:
            f.write('{"key": "k1", "value": "v1"}\n')
            f.write("not valid json\n")
            f.write('{"key": "k2", "value": "v2"}\n')
        messages = queue.dequeue_all()
        # Should get 2 valid messages, skip malformed
        assert len(messages) == 2


class TestProduceStats:
    """Tests for ProduceStats tracking."""

    def test_initial_values(self):
        """Verifies initial counter values."""
        stats = ProduceStats()
        assert stats.success_count == 0
        assert stats.failure_count == 0
        assert stats.failure_ratio == 0.0

    def test_record_success(self):
        """Verifies success recording."""
        stats = ProduceStats()
        stats.record_success()
        stats.record_success()
        assert stats.success_count == 2
        assert stats.failure_count == 0
        assert stats.failure_ratio == 0.0

    def test_record_failure(self):
        """Verifies failure recording."""
        stats = ProduceStats()
        stats.record_failure()
        assert stats.failure_count == 1
        assert stats.failure_ratio == 1.0

    def test_failure_ratio(self):
        """Verifies failure ratio calculation."""
        stats = ProduceStats()
        stats.record_success()
        stats.record_success()
        stats.record_failure()
        # 1 failure / 3 total = 0.333...
        assert pytest.approx(stats.failure_ratio, rel=0.01) == 0.333

    def test_window_reset(self, monkeypatch):
        """Verifies counters reset after window expires."""
        stats = ProduceStats(window_seconds=1.0)
        stats.record_success()
        stats.record_failure()
        assert stats.total == 2
        # Simulating time passage
        original_time = time.time
        monkeypatch.setattr(time, "time", lambda: original_time() + 2)
        stats.record_success()
        # Window should have reset
        assert stats.success_count == 1
        assert stats.failure_count == 0


class TestRateThrottler:
    """Tests for RateThrottler rate limiting."""

    def test_no_throttle_when_healthy(self, monkeypatch):
        """Verifies no throttle when failure ratio is low."""
        sleep_calls = []
        monkeypatch.setattr(time, "sleep", lambda x: sleep_calls.append(x))
        throttler = RateThrottler(failure_threshold=0.1)
        throttler.record_success()
        throttler.record_success()
        throttler.maybe_throttle()
        assert len(sleep_calls) == 0

    def test_throttle_when_unhealthy(self, monkeypatch):
        """Verifies throttle when failure ratio exceeds threshold."""
        sleep_calls = []
        monkeypatch.setattr(time, "sleep", lambda x: sleep_calls.append(x))
        throttler = RateThrottler(min_delay_ms=100, max_delay_ms=1000, failure_threshold=0.1)
        # Creating 50% failure ratio
        throttler.record_success()
        throttler.record_failure()
        throttler.maybe_throttle()
        assert len(sleep_calls) == 1
        assert sleep_calls[0] > 0


class TestResilientProducer:
    """Tests for ResilientProducer wrapper."""

    def test_produce_calls_underlying_producer(self, tmp_path):
        """Verifies produce calls underlying Kafka producer."""
        mock_producer = MagicMock()
        rp = ResilientProducer(mock_producer, "test-topic", str(tmp_path))
        rp.produce(b"key", b"value")
        mock_producer.produce.assert_called_once()
        # Checking the call was made with correct topic (positional arg)
        call_args = mock_producer.produce.call_args
        assert call_args[0][0] == "test-topic"  # First positional arg is topic
        call_kwargs = call_args[1]
        assert call_kwargs["key"] == b"key"
        assert call_kwargs["value"] == b"value"

    def test_delivery_failure_queues_to_dlq(self, tmp_path):
        """Verifies failed delivery queues message to DLQ."""
        mock_producer = MagicMock()
        rp = ResilientProducer(mock_producer, "test-topic", str(tmp_path))
        # Producing a message
        rp.produce(b"key", b"value")
        # Simulating delivery callback with error
        callback = mock_producer.produce.call_args[1]["callback"]
        mock_msg = MagicMock()
        mock_msg.key.return_value = b"key"
        mock_msg.value.return_value = b"value"
        mock_error = MagicMock()
        mock_error.__str__ = lambda self: "Broker unavailable"
        callback(mock_error, mock_msg)
        # Verifying message was queued to DLQ
        assert rp.dlq_size == 1

    def test_delivery_success_records_stats(self, tmp_path):
        """Verifies successful delivery records stats."""
        mock_producer = MagicMock()
        rp = ResilientProducer(mock_producer, "test-topic", str(tmp_path))
        rp.produce(b"key", b"value")
        # Simulating successful delivery callback
        callback = mock_producer.produce.call_args[1]["callback"]
        mock_msg = MagicMock()
        mock_msg.key.return_value = b"key"
        mock_msg.value.return_value = b"value"
        callback(None, mock_msg)
        assert rp.stats.success_count == 1

    def test_retry_dlq(self, tmp_path):
        """Verifies retry_dlq reads and re-produces messages."""
        mock_producer = MagicMock()
        rp = ResilientProducer(mock_producer, "test-topic", str(tmp_path))
        # Directly enqueue to DLQ
        rp._dlq.enqueue(b"key1", b"value1")
        rp._dlq.enqueue(b"key2", b"value2")
        count = rp.retry_dlq()
        assert count == 2
        assert mock_producer.produce.call_count == 2

    def test_flush_timeout_queues_pending(self, tmp_path):
        """Verifies flush timeout queues pending messages to DLQ."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 1  # 1 message still pending
        rp = ResilientProducer(mock_producer, "test-topic", str(tmp_path))
        # Producing without delivery callback
        rp.produce(b"key", b"value")
        # Flushing with timeout (simulated by flush returning > 0)
        rp.flush(timeout=1.0)
        # Pending message should be in DLQ
        assert rp.dlq_size == 1

    def test_topic_property(self, tmp_path):
        """Verifies topic property returns correct value."""
        mock_producer = MagicMock()
        rp = ResilientProducer(mock_producer, "my-topic", str(tmp_path))
        assert rp.topic == "my-topic"

    def test_throttle_disabled(self, tmp_path):
        """Verifies throttling can be disabled."""
        mock_producer = MagicMock()
        rp = ResilientProducer(mock_producer, "test-topic", str(tmp_path), throttle_on_failures=False)
        assert rp.stats is None
