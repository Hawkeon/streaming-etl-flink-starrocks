"""
Behavioral Event Producer for Fluss.
User State Machine with transition probabilities, replay dynamics, session-aware events.

Schema (15 fields):
  session_id, user_id, track_id, track_name, artist_name
  sequence_number, action, listen_duration_sec
  is_replay, last_play_gap_min, previous_action
  user_state, track_genre, track_tempo, ts
"""

import fluss
import pyarrow as pa
import asyncio
import random
import logging
import uuid
from datetime import datetime, timezone

def now_ms() -> int:
    """Return current timestamp in milliseconds."""
    return int(datetime.now().timestamp() * 1000)
from dataclasses import dataclass
from pathlib import Path

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

FLUSS_COORDINATOR_URL = "coordinator-server:9123"
TABLE_NAME = "fluss_user_logs"  # table in default database
TRACK_PATH = "/app/data/dataset.csv"
SPEED_FACTOR = 60.0  # 1 simulated hour = 1 real minute


# =============================================================================
# Transition Matrix
# =============================================================================
TRANSITION = {
    "first":     {"completed": 0.40, "skip": 0.50, "like": 0.10},
    "completed": {"completed": 0.20, "skip": 0.25, "like": 0.40, "switch": 0.10, "dislike": 0.05},
    "skip":      {"completed": 0.15, "skip": 0.55, "like": 0.10, "switch": 0.15, "dislike": 0.05},
    "like":      {"completed": 0.45, "skip": 0.10, "like": 0.25, "switch": 0.15, "dislike": 0.05},
    "dislike":   {"completed": 0.10, "skip": 0.65, "like": 0.05, "switch": 0.15, "dislike": 0.05},
    "switch":    {"completed": 0.30, "skip": 0.30, "like": 0.20, "switch": 0.10, "dislike": 0.10},
}

MOOD_MOD = {
    "distracted": {"skip": +0.20, "completed": -0.10},
    "looping":    {"skip": -0.10},
    "focused":    {"completed": +0.15, "skip": -0.10},
    "chill":      {},
    "energetic":  {"completed": +0.10},
}


@dataclass
class Track:
    track_id: str
    track_name: str
    artist_name: str
    duration_ms: int
    genre: str
    tempo: float
    popularity: int


@dataclass
class PlayRecord:
    ts: int  # epoch milliseconds
    action: str
    listen_ratio: float
    seq: int


class Session:
    def __init__(self, user_id: str, tier: str):
        self.user_id = user_id
        self.tier = tier
        self.session_id = str(uuid.uuid4())[:12]
        self.mood = random.choice(["chill", "energetic", "distracted", "focused"])
        self.history: dict[str, list[PlayRecord]] = {}
        self.seq = 0
        self.completed = 0
        self.skips = 0

    def record(self, track_id: str, action: str, ratio: float):
        self.seq += 1
        if track_id not in self.history:
            self.history[track_id] = []
        self.history[track_id].append(PlayRecord(now_ms(), action, ratio, self.seq))
        if action == "completed":
            self.completed += 1
        elif action == "skip":
            self.skips += 1

    def update_mood(self, last_action: str):
        total = sum(len(v) for v in self.history.values())
        if not total:
            return
        if self.seq >= 2 and self.history:
            last_track = None
            for lst in self.history.values():
                if lst[-1].ts == max(v[-1].ts for v in self.history.values()):
                    last_track = lst[-1]
                    break
            if last_track and last_track.action in ("completed", "like"):
                self.mood = "looping"
        if total >= 3 and self.skips / total > 0.6:
            self.mood = "distracted"
        elif total >= 3 and self.completed / total > 0.7:
            self.mood = "focused"
        elif last_action == "skip":
            self.mood = "distracted"

    def replay_info(self, track_id: str):
        h = self.history.get(track_id, [])
        if not h:
            return False, -1, None
        last = h[-1]
        gap = int((datetime.now().timestamp() * 1000 - last.ts) / 60000)
        return True, gap, last.action

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq


def next_action(prev: str | None, mood: str, gap: int, replay: bool) -> str:
    base = TRANSITION.get(prev, TRANSITION["first"]).copy()
    mods = MOOD_MOD.get(mood, {})
    for k, delta in mods.items():
        if k in base:
            base[k] = max(0.0, min(1.0, base[k] + delta))
    total = sum(base.values())
    probs = [base[k] / total for k in base]
    return random.choices(list(base.keys()), weights=probs)[0]


def listen_ratio_for(action: str) -> float:
    if action in ("completed", "like"):
        return random.betavariate(5, 2)
    elif action == "skip":
        return random.betavariate(2, 4)
    elif action == "dislike":
        return random.uniform(0.02, 0.30)
    return random.betavariate(3, 3)


def pick_track(
    session: Session,
    all_tracks: list[Track],
    weighted: list[Track],
    prev: Track | None,
) -> Track:
    r = random.random()
    # 15% replay same
    if r < 0.15 and prev and (session.mood == "looping" or random.random() < 0.3):
        return prev
    # 25% same genre/tempo
    if r < 0.40 and prev:
        candidates = [t for t in all_tracks if t.genre == prev.genre and abs(t.tempo - prev.tempo) < 20]
        if candidates:
            return random.choice(candidates)
    # 60% popularity-weighted discovery
    return random.choice(weighted)


async def connect() -> fluss.FlussConnection:
    config = fluss.Config({"bootstrap.servers": FLUSS_COORDINATOR_URL})
    for i in range(10):
        try:
            return await fluss.FlussConnection.create(config)
        except Exception as e:
            if i == 9:
                raise
            logger.warning(f"Connect retry {i+1}/10: {e}")
            await asyncio.sleep(2)


def load_tracks() -> tuple[list[Track], list[Track]]:
    df = pl.read_csv(TRACK_PATH, infer_schema=True)
    tracks = [
        Track(
            track_id=r["track_id"],
            track_name=r["track_name"],
            artist_name=r["artists"],
            duration_ms=r["duration_ms"],
            genre=r.get("track_genre", "unknown"),
            tempo=r.get("tempo", 120.0),
            popularity=r["popularity"],
        )
        for r in df.iter_rows(named=True)
    ]
    weighted = []
    for t in tracks:
        weighted.extend([t] * max(1, int(t.popularity ** 0.6)))
    logger.info(f"Tracks: {len(tracks)} | Weighted pool: {len(weighted)}")
    return tracks, weighted


def load_users() -> list[tuple[str, str]]:
    p = Path(__file__).parent.parent.parent / "seeds" / "dim_user.csv"
    if p.exists():
        df = pl.read_csv(p)
        return [(r["user_id"], r["subscription_tier"]) for r in df.iter_rows(named=True)]
    tiers = ["free", "premium", "vip"]
    w = [0.60, 0.30, 0.10]
    return [(f"user_{i}", random.choices(tiers, weights=w)[0]) for i in range(1, 1001)]


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


async def main():
    tracks, weighted = load_tracks()
    users = load_users()
    logger.info(f"Loaded {len(users)} users")

    conn = await connect()
    logger.info("Connected to Fluss")

    if "." in TABLE_NAME:
        db, tn = TABLE_NAME.split(".")
        table = await conn.get_table(fluss.TablePath(db, tn))
    else:
        # Use 'fluss' database as that's what Fluss SQL client created
        table = await conn.get_table(fluss.TablePath("fluss", TABLE_NAME))
    writer = table.new_append().create_writer()

    sessions_started = 0
    events_total = 0
    last_report = datetime.now()

    try:
        while True:
            uid, tier = random.choice(users)
            session = Session(uid, tier)
            n_tracks = random.randint(5, 12)
            prev: Track | None = None

            for _ in range(1, n_tracks + 1):
                track = pick_track(session, tracks, weighted, prev)
                replay, gap, prev_act = session.replay_info(track.track_id)
                action = next_action(prev_act, session.mood, gap, replay)
                ratio = listen_ratio_for(action)
                dur_ms = int(track.duration_ms * ratio)
                dur_sec = dur_ms // 1000
                seq = session.next_seq()

                session.record(track.track_id, action, ratio)
                session.update_mood(action)
                prev = track

                batch = pa.RecordBatch.from_arrays([
                    pa.array([session.session_id]),
                    pa.array([session.user_id]),
                    pa.array([track.track_id]),
                    pa.array([seq], type=pa.int32()),
                    pa.array([action]),
                    pa.array([dur_sec], type=pa.int32()),
                    pa.array([replay], type=pa.bool_()),
                    pa.array([gap], type=pa.int32()),
                    pa.array([prev_act or "null"]),
                    pa.array([session.mood]),
                    pa.array([datetime.fromtimestamp(now_ms() / 1000)]),
                ], schema=SCHEMA)
                writer.write_arrow_batch(batch)
                writer.flush()

                events_total += 1
                await asyncio.sleep(dur_sec / SPEED_FACTOR)

            sessions_started += 1

            elapsed = (datetime.now() - last_report).total_seconds()
            if elapsed >= 10:
                rate = events_total / elapsed
                logger.info(
                    f"Sessions: {sessions_started} | Events: {events_total} "
                    f"({rate:.1f}/s) | {session.user_id} [{session.mood}] {n_tracks} tracks"
                )
                events_total = 0
                last_report = datetime.now()

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())