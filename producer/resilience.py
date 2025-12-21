"""Resilience utilities for producers: backoff, retry, DLQ, rate limiting.

Implements the backoff & shed contract:
- Exponential backoff with jitter on HTTP 429/5xx and timeouts
- Cap concurrent polls via VLC_MAX_INFLIGHT_POLLS
- Detect Kafka pressure → reduce produce rate
- Never drop data silently; on-disk queue for retries when Kafka is down
- Auto-reconnect when Kafka broker restarts
"""

import json
import os
import random
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from confluent_kafka import KafkaError, KafkaException, Producer


# ------------- Configuration -------------
@dataclass
class RetryConfig:
    """Configuration for exponential backoff retry logic."""

    base_delay_ms: int = 1000
    max_delay_ms: int = 60000
    max_retries: int = 5
    jitter_factor: float = 0.3  # ±30% jitter

    @classmethod
    def from_env(cls) -> "RetryConfig":
        """Creates RetryConfig from environment variables."""
        return cls(
            base_delay_ms=int(os.getenv("VLC_BACKOFF_BASE_MS", "1000")),
            max_delay_ms=int(os.getenv("VLC_BACKOFF_MAX_MS", "60000")),
            max_retries=int(os.getenv("VLC_BACKOFF_MAX_RETRIES", "5")),
            jitter_factor=float(os.getenv("VLC_BACKOFF_JITTER", "0.3")),
        )


# ------------- Exponential Backoff -------------
class ExponentialBackoff:
    """Calculates exponential backoff delays with jitter."""

    def __init__(self, config: RetryConfig):
        self.config = config

    def delay_ms(self, attempt: int) -> int:
        """Calculates delay for given attempt (0-indexed).

        Uses exponential backoff: base * 2^attempt, capped at max_delay.
        Applies jitter: ±jitter_factor of calculated delay.
        """
        # Exponential delay
        delay = self.config.base_delay_ms * (2**attempt)
        delay = min(delay, self.config.max_delay_ms)
        # Applying jitter
        jitter_range = delay * self.config.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        return max(0, int(delay + jitter))

    def sleep(self, attempt: int) -> None:
        """Sleeps for the calculated backoff delay."""
        delay_ms = self.delay_ms(attempt)
        time.sleep(delay_ms / 1000.0)


# ------------- HTTP Retry Logic -------------
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def is_retryable_error(exc: Exception) -> bool:
    """Checks if exception is retryable (timeout or connection error)."""
    return isinstance(
        exc,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ),
    )


def is_retryable_status(status_code: int) -> bool:
    """Checks if HTTP status code is retryable."""
    return status_code in RETRYABLE_STATUS_CODES


def http_request_with_retry(
    session: requests.Session, method: str, url: str, config: Optional[RetryConfig] = None, **kwargs
) -> requests.Response:
    """Makes HTTP request with exponential backoff retry on transient failures.

    Retries on:
    - HTTP 429 (Too Many Requests)
    - HTTP 5xx (Server errors)
    - Timeouts and connection errors

    Args:
        session: requests.Session to use
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        config: Retry configuration (defaults to env-based config)
        **kwargs: Additional arguments passed to session.request()

    Returns:
        Response object on success

    Raises:
        requests.HTTPError: After max retries exhausted
        requests.RequestException: On non-retryable errors
    """
    if config is None:
        config = RetryConfig.from_env()
    backoff = ExponentialBackoff(config)
    last_exc: Optional[Exception] = None
    for attempt in range(config.max_retries + 1):
        try:
            resp = session.request(method, url, **kwargs)
            # Checking for retryable status codes
            if is_retryable_status(resp.status_code):
                if attempt < config.max_retries:
                    # Respecting Retry-After header if present
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_secs = int(retry_after)
                            time.sleep(wait_secs)
                            continue
                        except ValueError:
                            pass
                    backoff.sleep(attempt)
                    continue
                # Max retries exhausted
                resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            if is_retryable_error(e) and attempt < config.max_retries:
                backoff.sleep(attempt)
                continue
            raise
    # Should not reach here, but just in case
    if last_exc:
        raise last_exc
    raise RuntimeError("Unexpected state in http_request_with_retry")


# ------------- Inflight Limiter -------------
class InflightLimiter:
    """Limits concurrent operations using a semaphore."""

    def __init__(self, max_inflight: Optional[int] = None):
        if max_inflight is None:
            max_inflight = int(os.getenv("VLC_MAX_INFLIGHT_POLLS", "1"))
        self._semaphore = threading.Semaphore(max_inflight)
        self._max = max_inflight

    def __enter__(self):
        self._semaphore.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()
        return False

    @property
    def max_inflight(self) -> int:
        """Returns the maximum number of concurrent operations."""
        return self._max


# ------------- On-Disk Queue (DLQ) -------------
class DiskQueue:
    """Persists failed messages to disk for retry.

    Uses JSON-lines format for simplicity and append-only writes.
    """

    def __init__(self, queue_dir: Optional[str] = None, topic: str = "default"):
        if queue_dir is None:
            queue_dir = os.getenv("VLC_DLQ_DIR", "/state/dlq")
        self._dir = Path(queue_dir)
        self._topic = topic
        self._queue_file = self._dir / f"{topic}.jsonl"
        self._lock = threading.Lock()
        self._dir.mkdir(parents=True, exist_ok=True)

    def enqueue(self, key: bytes, value: bytes) -> None:
        """Appends a failed message to the disk queue."""
        record = {
            "key": key.decode("utf-8", errors="replace"),
            "value": value.decode("utf-8", errors="replace"),
            "ts": time.time(),
        }
        with self._lock:
            with open(self._queue_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

    def dequeue_all(self) -> List[Tuple[bytes, bytes]]:
        """Reads and clears all messages from the disk queue.

        Returns:
            List of (key, value) tuples
        """
        with self._lock:
            if not self._queue_file.exists():
                return []
            messages = []
            try:
                with open(self._queue_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            key = rec.get("key", "").encode("utf-8")
                            value = rec.get("value", "").encode("utf-8")
                            messages.append((key, value))
                        except json.JSONDecodeError:
                            continue
                # Clearing the queue after successful read
                self._queue_file.unlink()
            except FileNotFoundError:
                pass
            return messages

    def size(self) -> int:
        """Returns approximate number of messages in queue."""
        with self._lock:
            if not self._queue_file.exists():
                return 0
            try:
                with open(self._queue_file, "r", encoding="utf-8") as f:
                    return sum(1 for line in f if line.strip())
            except FileNotFoundError:
                return 0


# ------------- Rate Throttler -------------
@dataclass
class ProduceStats:
    """Tracks produce success/failure statistics for rate throttling."""

    success_count: int = 0
    failure_count: int = 0
    window_start: float = field(default_factory=time.time)
    window_seconds: float = 60.0  # 1 minute window

    def record_success(self) -> None:
        """Records a successful produce."""
        self._maybe_reset_window()
        self.success_count += 1

    def record_failure(self) -> None:
        """Records a failed produce."""
        self._maybe_reset_window()
        self.failure_count += 1

    def _maybe_reset_window(self) -> None:
        """Resets counters if window has elapsed."""
        now = time.time()
        if now - self.window_start > self.window_seconds:
            self.success_count = 0
            self.failure_count = 0
            self.window_start = now

    @property
    def failure_ratio(self) -> float:
        """Returns failure ratio in current window."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.0
        return self.failure_count / total

    @property
    def total(self) -> int:
        """Returns total messages in current window."""
        return self.success_count + self.failure_count


class RateThrottler:
    """Throttles produce rate based on failure ratio."""

    def __init__(
        self,
        min_delay_ms: int = 0,
        max_delay_ms: int = 5000,
        failure_threshold: float = 0.1,
    ):
        self._min_delay_ms = min_delay_ms
        self._max_delay_ms = max_delay_ms
        self._failure_threshold = failure_threshold
        self._stats = ProduceStats()

    def record_success(self) -> None:
        """Records successful produce."""
        self._stats.record_success()

    def record_failure(self) -> None:
        """Records failed produce."""
        self._stats.record_failure()

    def maybe_throttle(self) -> None:
        """Applies throttle delay if failure ratio exceeds threshold."""
        if self._stats.failure_ratio > self._failure_threshold:
            # Scaling delay based on failure ratio
            ratio = min(self._stats.failure_ratio, 1.0)
            delay_ms = self._min_delay_ms + ((self._max_delay_ms - self._min_delay_ms) * ratio)
            time.sleep(delay_ms / 1000.0)

    @property
    def stats(self) -> ProduceStats:
        """Returns current produce statistics."""
        return self._stats


# ------------- Kafka Connection Health Check -------------
class KafkaHealthChecker:
    """Monitors Kafka connection health and triggers reconnection when needed.

    Uses consecutive failure counting to detect broker unavailability.
    Triggers reconnect callback after threshold is exceeded.
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        check_interval_secs: float = 30.0,
    ):
        self._failure_threshold = int(os.getenv("VLC_KAFKA_FAILURE_THRESHOLD", str(failure_threshold)))
        self._check_interval = float(os.getenv("VLC_KAFKA_CHECK_INTERVAL_SECS", str(check_interval_secs)))
        self._consecutive_failures = 0
        self._last_success_time = time.time()
        self._last_check_time = 0.0
        self._lock = threading.Lock()

    def record_success(self) -> None:
        """Records successful Kafka operation."""
        with self._lock:
            self._consecutive_failures = 0
            self._last_success_time = time.time()

    def record_failure(self) -> None:
        """Records failed Kafka operation."""
        with self._lock:
            self._consecutive_failures += 1

    def needs_reconnect(self) -> bool:
        """Returns True if consecutive failures exceed threshold."""
        with self._lock:
            return self._consecutive_failures >= self._failure_threshold

    def reset(self) -> None:
        """Resets failure counter after reconnection."""
        with self._lock:
            self._consecutive_failures = 0
            self._last_check_time = time.time()

    def should_check(self) -> bool:
        """Returns True if enough time has passed since last check."""
        now = time.time()
        with self._lock:
            if now - self._last_check_time >= self._check_interval:
                self._last_check_time = now
                return True
        return False

    @property
    def consecutive_failures(self) -> int:
        """Returns current consecutive failure count."""
        with self._lock:
            return self._consecutive_failures

    @property
    def seconds_since_success(self) -> float:
        """Returns seconds since last successful operation."""
        with self._lock:
            return time.time() - self._last_success_time


# ------------- Resilient Producer Wrapper -------------
class ResilientProducer:
    """Wraps confluent_kafka.Producer with resilience features.

    Features:
    - Tracks delivery failures via callback
    - Queues failed messages to disk (DLQ)
    - Retries from disk queue on next produce cycle
    - Implements rate throttling based on failure ratio
    - Auto-reconnects when Kafka broker becomes unavailable
    """

    def __init__(
        self,
        producer: Producer,
        topic: str,
        dlq_dir: Optional[str] = None,
        throttle_on_failures: bool = True,
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        self._producer = producer
        self._topic = topic
        self._dlq = DiskQueue(dlq_dir, topic)
        self._throttler = RateThrottler() if throttle_on_failures else None
        self._pending: Dict[str, Tuple[bytes, bytes]] = {}
        self._lock = threading.Lock()
        # Storing config for reconnection
        self._producer_config = producer_config or {}
        self._health_checker = KafkaHealthChecker()
        self._reconnect_backoff = ExponentialBackoff(RetryConfig(
            base_delay_ms=5000,
            max_delay_ms=60000,
            max_retries=10,
        ))
        self._reconnect_attempts = 0

    def _delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        """Handles delivery reports from Kafka."""
        key = msg.key() if msg.key() else b""
        value = msg.value() if msg.value() else b""
        msg_id = f"{key.decode('utf-8', errors='replace')}:{hash(value)}"
        with self._lock:
            self._pending.pop(msg_id, None)
        if err:
            # Delivery failed - queue to DLQ
            self._dlq.enqueue(key, value)
            if self._throttler:
                self._throttler.record_failure()
            self._health_checker.record_failure()
            print(f"[resilience] delivery failed: {err}, queued to DLQ")
        else:
            if self._throttler:
                self._throttler.record_success()
            self._health_checker.record_success()
            self._reconnect_attempts = 0  # reset on success

    def _check_and_reconnect(self) -> bool:
        """Checks connection health and reconnects if needed.

        Returns:
            True if reconnection was performed, False otherwise.
        """
        if not self._health_checker.needs_reconnect():
            return False
        if not self._producer_config:
            print("[resilience] cannot reconnect: no producer config stored")
            return False
        print(
            f"[resilience] Kafka unhealthy ({self._health_checker.consecutive_failures} failures, "
            f"{self._health_checker.seconds_since_success:.0f}s since success), reconnecting..."
        )
        # Applying backoff before reconnection attempt
        if self._reconnect_attempts > 0:
            self._reconnect_backoff.sleep(min(self._reconnect_attempts - 1, 9))
        self._reconnect_attempts += 1
        try:
            # Flushing any remaining messages to DLQ
            with self._lock:
                for key, value in self._pending.values():
                    self._dlq.enqueue(key, value)
                self._pending.clear()
            # Creating new producer
            self._producer = Producer(self._producer_config)
            self._health_checker.reset()
            print("[resilience] Kafka producer reconnected successfully")
            return True
        except KafkaException as e:
            print(f"[resilience] reconnection failed: {e}")
            return False

    def check_health(self) -> bool:
        """Proactively checks Kafka broker health.

        Uses list_topics() with short timeout to verify broker connectivity.
        Returns True if healthy, False otherwise.
        """
        if not self._health_checker.should_check():
            return True  # skip check, assume healthy
        try:
            # list_topics with 5s timeout to verify connectivity
            self._producer.list_topics(timeout=5)
            self._health_checker.record_success()
            return True
        except KafkaException as e:
            self._health_checker.record_failure()
            print(f"[resilience] health check failed: {e}")
            return False

    def produce(self, key: bytes, value: bytes) -> None:
        """Produces a message with delivery tracking and auto-reconnect."""
        # Checking connection health and reconnecting if needed
        self._check_and_reconnect()
        msg_id = f"{key.decode('utf-8', errors='replace')}:{hash(value)}"
        with self._lock:
            self._pending[msg_id] = (key, value)
        # Applying throttle if needed
        if self._throttler:
            self._throttler.maybe_throttle()
        try:
            self._producer.produce(
                self._topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
        except KafkaException as e:
            # Producer-level error (e.g., queue full) - queue to DLQ
            with self._lock:
                self._pending.pop(msg_id, None)
            self._dlq.enqueue(key, value)
            self._health_checker.record_failure()
            print(f"[resilience] produce error: {e}, queued to DLQ")

    def flush(self, timeout: float = 30.0) -> int:
        """Flushes pending messages with timeout.

        Returns:
            Number of messages still in queue (0 = all delivered)
        """
        try:
            remaining = self._producer.flush(timeout)
        except KafkaException as e:
            print(f"[resilience] flush error: {e}")
            remaining = len(self._pending)
        if remaining > 0:
            # Some messages didn't get delivered - queue pending to DLQ
            with self._lock:
                for key, value in self._pending.values():
                    self._dlq.enqueue(key, value)
                self._pending.clear()
            self._health_checker.record_failure()
            print(f"[resilience] flush timeout, {remaining} msgs queued to DLQ")
        else:
            self._health_checker.record_success()
        return remaining

    def retry_dlq(self) -> int:
        """Retries all messages from the DLQ.

        Returns:
            Number of messages retried
        """
        messages = self._dlq.dequeue_all()
        for key, value in messages:
            self.produce(key, value)
        if messages:
            print(f"[resilience] retrying {len(messages)} msgs from DLQ")
        return len(messages)

    @property
    def dlq_size(self) -> int:
        """Returns number of messages in DLQ."""
        return self._dlq.size()

    @property
    def stats(self) -> Optional[ProduceStats]:
        """Returns produce statistics if throttling is enabled."""
        return self._throttler.stats if self._throttler else None

    @property
    def topic(self) -> str:
        """Returns the topic name."""
        return self._topic

    @property
    def is_healthy(self) -> bool:
        """Returns True if producer is in healthy state."""
        return not self._health_checker.needs_reconnect()
