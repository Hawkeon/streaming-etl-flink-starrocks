
CREATE DATABASE IF NOT EXISTS music_db;
USE music_db;

CREATE TABLE IF NOT EXISTS dim_user (
    user_id VARCHAR(50) NOT NULL,
    user_name VARCHAR(100),
    subscription_tier VARCHAR(20),
    avg_daily_activities INT,
    registration_date VARCHAR(20)
)
PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES ("enable_persistent_index" = "true", "replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_track (
    track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(500),
    artists VARCHAR(1000),
    album_name VARCHAR(500),
    popularity INT,
    duration_ms INT,
    explicit BOOLEAN,
    danceability DOUBLE,
    energy DOUBLE,
    `key` INT,
    loudness DOUBLE,
    mode INT,
    speechiness DOUBLE,
    acousticness DOUBLE,
    instrumentalness DOUBLE,
    liveness DOUBLE,
    valence DOUBLE,
    tempo DOUBLE,
    time_signature INT,
    track_genre VARCHAR(100)
)
PRIMARY KEY(track_id)
DISTRIBUTED BY HASH(track_id) BUCKETS 3
PROPERTIES ("enable_persistent_index" = "true", "replication_num" = "1");

CREATE TABLE IF NOT EXISTS fact_events (
    session_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    track_id VARCHAR(50) NOT NULL,
    ts DATETIME NOT NULL,
    sequence_number INT,
    action VARCHAR(20),
    listen_duration_sec INT,
    is_replay BOOLEAN,
    last_play_gap_min INT,
    previous_action VARCHAR(20),
    user_state VARCHAR(20)
)
DUPLICATE KEY(session_id, user_id, track_id, ts)
PARTITION BY RANGE(ts) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS fact_events_enriched (
    session_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    track_id VARCHAR(50) NOT NULL,
    ts DATETIME NOT NULL,
    sequence_number INT,
    action VARCHAR(20),
    listen_duration_sec INT,
    is_replay BOOLEAN,
    last_play_gap_min INT,
    previous_action VARCHAR(20),
    user_state VARCHAR(20),
    user_name VARCHAR(100),
    subscription_tier VARCHAR(20),
    avg_daily_activities INT,
    registration_date VARCHAR(20),
    track_name VARCHAR(500),
    artists VARCHAR(1000),
    album_name VARCHAR(500),
    popularity INT,
    duration_ms INT,
    explicit BOOLEAN,
    danceability DOUBLE,
    energy DOUBLE,
    `key` INT,
    loudness DOUBLE,
    mode INT,
    speechiness DOUBLE,
    acousticness DOUBLE,
    instrumentalness DOUBLE,
    liveness DOUBLE,
    valence DOUBLE,
    tempo DOUBLE,
    time_signature INT,
    track_genre VARCHAR(100)
)
DUPLICATE KEY(session_id, user_id, track_id, ts)
PARTITION BY RANGE(ts) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "replication_num" = "1"
);