"""
Benchmark Producer - High-throughput load generator for Fluss.
No session logic, pure throughput. Token-bucket ratelimiter for precise EPS.
"""

import argparse
import asyncio
import time
import uuid
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import fluss
import pyarrow as pa
import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

FLUSS_COORDINATOR_URL = "coordinator-server:9123"
TABLE_NAME = "fluss_user_logs"
TRACK_PATH = "/app/data/dataset.csv"


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark producer for Fluss")
    parser.add_argument("--eps", type=int, default=5000, help="Target events per second")
    parser.add_argument("--users", type=int, default=100, help="Number of distinct user IDs")
    parser.add_argument("--tracks", type=int, default=1000, help="Number of distinct track IDs")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for writes")
    parser.add_argument("--warmup-sec", type=int, default=120, help="Warmup seconds before measurement")
    parser.add_argument("--duration-sec", type=int, default=300, help="Measurement duration in seconds")
    parser.add_argument("--coordinator-url", type=str, default=None, help="Fluss coordinator URL (default: coordinator-server:9123)")
    return parser.parse_args()

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


@dataclass
class TokenBucket:
    """Thread-safe token bucket for rate limiting."""
    rate: float  # tokens per second
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
        """Acquire tokens, return time to wait if needed."""
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


class ThroughputProducer:
    def __init__(
        self,
        target_eps: int,
        num_user_ids: int = 100,
        num_track_ids: int = 1000,
        batch_size: int = 100,
        coordinator_url: str = FLUSS_COORDINATOR_URL,
    ):
        self.target_eps = target_eps
        self.num_user_ids = num_user_ids
        self.num_track_ids = num_track_ids
        self.batch_size = batch_size
        self.coordinator_url = coordinator_url
        self.bucket = TokenBucket(rate=target_eps)
        self._shutdown = asyncio.Event()
        self._conn: Optional[fluss.FlussConnection] = None
        self._writer = None
        self._total_sent = 0
        self._start_time: Optional[float] = None

        # Pre-load track IDs from CSV for realistic distribution
        self.track_ids: list[str] = []
        self._load_track_ids()

    def _load_track_ids(self) -> None:
        p = TRACK_PATH
        if not p.startswith("/"):
            p = f"/app/data/{TRACK_PATH}" if not TRACK_PATH.startswith("/") else TRACK_PATH
        try:
            df = pl.read_csv(p)
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

    async def run(self) -> None:
        await self.connect()
        self._start_time = time.monotonic()
        seq = 0
        report_interval = 5.0
        last_report = self._start_time
        last_count = 0

        logger.info(f"Starting throughput test at {self.target_eps} EPS")
        batch = []
        batch_seqs = []

        while not self._shutdown.is_set():
            wait_time = await self.bucket.acquire()
            if wait_time > 0:
                await asyncio.sleep(wait_time)

            batch.append(seq)
            seq += 1

            if len(batch) >= self.batch_size:
                pb = self._make_batch(batch[0], len(batch))
                self._writer.write_arrow_batch(pb)
                self._writer.flush()
                self._total_sent += len(batch)
                batch = []
                batch_seqs = []

            now = time.monotonic()
            if now - last_report >= report_interval:
                elapsed = now - self._start_time
                actual_eps = (self._total_sent - last_count) / (now - last_report)
                logger.info(
                    f"sent={self._total_sent} rate={actual_eps:.0f}/s "
                    f"elapsed={elapsed:.0f}s"
                )
                last_report = now
                last_count = self._total_sent

        # flush remaining
        if batch:
            pb = self._make_batch(batch[0], len(batch))
            self._writer.write_arrow_batch(pb)
            self._writer.flush()
            self._total_sent += len(batch)

    def stop(self) -> None:
        self._shutdown.set()

    def get_stats(self) -> dict:
        elapsed = (time.monotonic() - self._start_time) if self._start_time else 1
        return {
            "total_sent": self._total_sent,
            "elapsed_sec": elapsed,
            "avg_eps": self._total_sent / elapsed if elapsed > 0 else 0,
        }


async def main():
    args = parse_args()
    coordinator_url = args.coordinator_url or FLUSS_COORDINATOR_URL

    producer = ThroughputProducer(
        target_eps=args.eps,
        num_user_ids=args.users,
        num_track_ids=args.tracks,
        batch_size=args.batch_size,
        coordinator_url=coordinator_url,
    )

    # Warmup phase
    logger.info(f"WARMUP: {args.warmup_sec}s at {args.eps} EPS (no metrics)")
    warmup_task = asyncio.create_task(producer.run())
    await asyncio.sleep(args.warmup_sec)

    # Measurement phase
    producer._total_sent = 0
    producer._start_time = time.monotonic()
    logger.info(f"MEASUREMENT: {args.duration_sec}s starting now")

    async def measure():
        interval = 10.0
        next_report = time.monotonic() + interval
        samples = []
        while time.monotonic() - producer._start_time < args.duration_sec:
            await asyncio.sleep(min(interval, args.duration_sec - (time.monotonic() - producer._start_time)))
            elapsed = time.monotonic() - producer._start_time
            stats = producer.get_stats()
            eps = stats["total_sent"] / elapsed if elapsed > 0 else 0
            samples.append({"elapsed": elapsed, "eps": eps, "total": stats["total_sent"]})
            logger.info(f"[{elapsed:.0f}s] rate={eps:.0f}/s total={stats['total_sent']}")
        return samples

    measure_task = asyncio.create_task(measure())

    try:
        samples = await measure_task
    finally:
        producer.stop()
        await warmup_task

    final_stats = producer.get_stats()
    logger.info(f"DONE: {final_stats['total_sent']} events in {final_stats['elapsed_sec']:.0f}s = {final_stats['avg_eps']:.0f} avg EPS")
    print(f"\n=== RESULT: {final_stats['avg_eps']:.0f} EPS avg ===")


if __name__ == "__main__":
    asyncio.run(main())
