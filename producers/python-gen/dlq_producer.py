"""
DLQ Producer - writes failed events to Dead Letter Queue with retry + circuit breaker.
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import fluss
import pyarrow as pa
import pymysql

from circuit_breaker import CircuitBreaker, CircuitState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Transient error - retry with backoff."""
    pass


class FatalError(Exception):
    """Fatal error - send directly to DLQ."""
    pass


FLUSS_COORDINATOR_URL = "coordinator-server:9123"
TABLE_NAME = "fluss_user_logs"
TRACK_PATH = "/app/data/dataset.csv"


def classify_error(e: Exception) -> Exception:
    """Classify exception as retryable or fatal."""
    error_str = str(e).lower()
    # Fatal errors - DLQ immediately
    if any(x in error_str for x in [
        "table not exist",
        "schema mismatch",
        "invalid partition",
        "column count mismatch",
        "does not exist",
    ]):
        return FatalError(str(e))
    # Retryable errors - retry with backoff
    if any(x in error_str for x in [
        "connection",
        "timeout",
        "network",
        "unavailable",
        "busy",
        "resource",
        "memory",
        "deadlock",
    ]):
        return RetryableError(str(e))
    # Unknown - treat as retryable by default
    return RetryableError(str(e))


@dataclass
class TokenBucket:
    """Thread-safe token bucket for rate limiting."""
    rate: float
    capacity: float
    _tokens: float
    _last_refill: float

    def __init__(self, rate: float, capacity: Optional[float] = None):
        self.rate = rate
        self.capacity = capacity or rate
        self._tokens = self.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> float:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_refill = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0
            wait = (tokens - self._tokens) / self.rate
            self._tokens = 0.0
            return wait


SCHEMA = pa.schema([
    ("session_id", pa.string()),
    ("user_id", pa.string()),
    ("track_id", pa.string()),
    ("sequence_number", pa.int32()),
    ("action", pa.string()),
    ("listen_duration_sec", pa.int32()),
    ("is_replay", pa.bool_()),
    ("last_play_gap_min", pa.int32()),
    ("previous_action", pa.string()),
    ("user_state", pa.string()),
    ("ts", pa.timestamp("ms")),
])


class DLQProducer:
    def __init__(
        self,
        target_eps: int,
        num_user_ids: int = 100,
        num_track_ids: int = 1000,
        batch_size: int = 100,
        coordinator_url: str = FLUSS_COORDINATOR_URL,
        max_retries: int = 3,
        base_backoff: float = 1.0,
        circuit_threshold: int = 5,
        circuit_timeout: int = 60,
        fail_ratio: float = 0.0,
        dlq_host: str = "starrocks-fe",
        dlq_port: int = 9030,
        dlq_user: str = "root",
        dlq_password: str = "",
    ):
        self.target_eps = target_eps
        self.num_user_ids = num_user_ids
        self.num_track_ids = num_track_ids
        self.batch_size = batch_size
        self.coordinator_url = coordinator_url
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.fail_ratio = fail_ratio

        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_threshold,
            recovery_timeout=circuit_timeout,
        )
        self.bucket = TokenBucket(rate=target_eps)
        self._shutdown = asyncio.Event()
        self._conn: Optional[fluss.FlussConnection] = None
        self._writer = None
        self._total_sent = 0
        self._total_dlq = 0
        self._total_retries = 0
        self._start_time: Optional[float] = None

        # DLQ connection
        self.dlq_conn = pymysql.connect(
            host=dlq_host, port=dlq_port, user=dlq_user, password=dlq_password
        )

        self.track_ids: list[str] = []
        self._load_track_ids()

    def _load_track_ids(self) -> None:
        try:
            import polars as pl
            df = pl.read_csv(TRACK_PATH)
            self.track_ids = df["track_id"].to_list()[:self.num_track_ids]
        except Exception:
            self.track_ids = [f"track_{i}" for i in range(self.num_track_ids)]
        logger.info(f"Loaded {len(self.track_ids)} track IDs")

    async def connect(self) -> None:
        config = fluss.Config({"bootstrap.servers": self.coordinator_url})
        for i in range(10):
            try:
                self._conn = await fluss.FlussConnection.create(config)
                self._table = await self._conn.get_table(fluss.TablePath("fluss", TABLE_NAME))
                self._writer = self._table.new_append().create_writer()
                logger.info("Connected to Fluss")
                return
            except Exception as e:
                if i == 9:
                    raise
                logger.warning(f"Connect retry {i+1}/10: {e}")
                await asyncio.sleep(2)

    def _should_fail(self) -> bool:
        """For testing - randomly simulate failures."""
        import random
        return random.random() < self.fail_ratio

    async def _write(self, batch) -> None:
        """Actual write to Fluss."""
        # Simulate failure for testing
        if self.fail_ratio > 0 and self._should_fail():
            raise RetryableError("simulated transient failure")
        pb = self._make_batch(batch[0], len(batch))
        self._writer.write_arrow_batch(pb)
        self._writer.flush()

    def _write_dlq(self, payload: dict, error_type: str, error_msg: str, retry_count: int) -> None:
        """Write failed event to DLQ table."""
        try:
            cursor = self.dlq_conn.cursor()
            cursor.execute(
                """INSERT INTO music_db.fact_events_dlq
                (session_id, user_id, track_id, sequence_number, payload, error_type, error_msg, retry_count, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    payload.get("session_id", ""),
                    payload.get("user_id", ""),
                    payload.get("track_id", ""),
                    payload.get("sequence_number", 0),
                    json.dumps(payload),
                    error_type,
                    error_msg[:500] if error_msg else "",
                    retry_count,
                    datetime.now(),
                )
            )
            self.dlq_conn.commit()
            cursor.close()
            self._total_dlq += 1
        except Exception as e:
            logger.error(f"DLQ write failed: {e}")

    def _make_batch(self, start_seq: int, count: int) -> pa.RecordBatch:
        now_ms = int(time.time() * 1000)
        session_id = str(uuid.uuid4())[:12]
        user_id = f"user_{start_seq % self.num_user_ids}"
        track_id = self.track_ids[start_seq % len(self.track_ids)]
        action = "completed"
        return pa.RecordBatch.from_arrays([
            pa.array([session_id] * count),
            pa.array([user_id] * count),
            pa.array([track_id] * count),
            pa.array(list(range(start_seq, start_seq + count)), type=pa.int32()),
            pa.array([action] * count),
            pa.array([180] * count, type=pa.int32()),
            pa.array([False] * count, type=pa.bool_()),
            pa.array([-1] * count, type=pa.int32()),
            pa.array(["null"] * count),
            pa.array(["chill"] * count),
            pa.array([datetime.fromtimestamp(now_ms / 1000)] * count),
        ], schema=SCHEMA)

    async def write_with_retry_and_dlq(self, batch, payload: dict) -> bool:
        """Write with retry, circuit breaker, and DLQ fallback."""
        # Circuit breaker check
        if self.circuit_breaker.is_open():
            self._write_dlq(payload, "CircuitOpenError", "Circuit breaker open", 0)
            return False

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                await self._write(batch)
                self.circuit_breaker.record_success()
                return True
            except Exception as e:
                classified = classify_error(e)
                last_err = classified
                if isinstance(classified, FatalError):
                    break
                self._total_retries += 1
                backoff = self.base_backoff * (2 ** attempt)
                logger.warning(f"Write attempt {attempt + 1} failed, retrying in {backoff}s: {e}")
                await asyncio.sleep(backoff)

        # All retries exhausted or fatal error
        self.circuit_breaker.record_failure()
        error_type = type(last_err).__name__ if last_err else "UnknownError"
        error_msg = str(last_err) if last_err else ""
        self._write_dlq(payload, error_type, error_msg, attempt + 1)
        return False

    async def run(self) -> None:
        await self.connect()
        self._start_time = time.monotonic()
        seq = 0
        report_interval = 5.0
        last_report = self._start_time
        last_count = 0

        logger.info(f"Starting DLQ producer at {self.target_eps} EPS (fail_ratio={self.fail_ratio})")
        batch = []
        batch_seqs = []

        while not self._shutdown.is_set():
            wait_time = await self.bucket.acquire()
            if wait_time > 0:
                await asyncio.sleep(wait_time)

            batch.append(seq)
            batch_seqs.append(seq)
            seq += 1

            if len(batch) >= self.batch_size:
                payload = {"session_id": str(uuid.uuid4()), "seq_start": batch[0], "count": len(batch)}
                success = await self.write_with_retry_and_dlq(batch, payload)
                if not success:
                    pass  # already logged in DLQ write
                self._total_sent += len(batch)
                batch = []
                batch_seqs = []

            now = time.monotonic()
            if now - last_report >= report_interval:
                elapsed = now - self._start_time
                actual_eps = (self._total_sent - last_count) / (now - last_report)
                cb_state = self.circuit_breaker.state.value
                logger.info(
                    f"sent={self._total_sent} dlq={self._total_dlq} retries={self._total_retries} "
                    f"rate={actual_eps:.0f}/s circuit={cb_state} elapsed={elapsed:.0f}s"
                )
                last_report = now
                last_count = self._total_sent

        if batch:
            payload = {"session_id": str(uuid.uuid4()), "seq_start": batch[0], "count": len(batch)}
            await self.write_with_retry_and_dlq(batch, payload)
            self._total_sent += len(batch)

    def stop(self) -> None:
        self._shutdown.set()

    def get_stats(self) -> dict:
        elapsed = (time.monotonic() - self._start_time) if self._start_time else 1
        return {
            "total_sent": self._total_sent,
            "total_dlq": self._total_dlq,
            "total_retries": self._total_retries,
            "elapsed_sec": elapsed,
            "avg_eps": self._total_sent / elapsed if elapsed > 0 else 0,
            "circuit_state": self.circuit_breaker.state.value,
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="DLQ Producer with retry + circuit breaker")
    parser.add_argument("--coordinator-url", type=str, default=FLUSS_COORDINATOR_URL)
    parser.add_argument("--eps", type=int, default=1000)
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--tracks", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--warmup-sec", type=int, default=30)
    parser.add_argument("--duration-sec", type=int, default=60)
    parser.add_argument("--fail-ratio", type=float, default=0.0, help="Ratio of events to simulate failure (0.0-1.0)")
    parser.add_argument("--data-path", type=str, default=None)
    args = parser.parse_args()

    producer = DLQProducer(
        target_eps=args.eps,
        num_user_ids=args.users,
        num_track_ids=args.tracks,
        batch_size=args.batch_size,
        coordinator_url=args.coordinator_url,
        fail_ratio=args.fail_ratio,
        dlq_host="starrocks-fe",
        dlq_port=9030,
    )

    logger.info(f"WARMUP: {args.warmup_sec}s at {args.eps} EPS")
    warmup_task = asyncio.create_task(producer.run())
    await asyncio.sleep(args.warmup_sec)

    producer._total_sent = 0
    producer._total_dlq = 0
    producer._total_retries = 0
    producer._start_time = time.monotonic()
    logger.info(f"MEASUREMENT: {args.duration_sec}s starting now")

    measure_task = asyncio.create_task(producer.run())
    await asyncio.sleep(args.duration_sec)
    producer.stop()
    await measure_task

    stats = producer.get_stats()
    logger.info(f"DONE: sent={stats['total_sent']} dlq={stats['total_dlq']} retries={stats['total_retries']} rate={stats['avg_eps']:.0f}/s circuit={stats['circuit_state']}")
    print(f"\n=== RESULT: {stats['avg_eps']:.0f} EPS avg, {stats['total_dlq']} DLQ, {stats['total_retries']} retries ===")


if __name__ == "__main__":
    asyncio.run(main())
