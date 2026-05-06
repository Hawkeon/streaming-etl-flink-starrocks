-- =========================================================
-- SYSTEM INITIALIZATION
-- =========================================================

CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;

-- Table: user_logs
-- Behavioral event schema with session context and replay tracking (keys only)
CREATE TABLE IF NOT EXISTS user_logs (
    user_id STRING,
    track_id STRING,
    session_id STRING,
    sequence_number INT,
    `action` STRING,
    listen_duration_sec INT,
    is_replay BOOLEAN,
    last_play_gap_min INT,
    previous_action STRING,
    user_state STRING,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'log.format' = 'ARROW'
);

-- Table: user_scores (Primary Key = Upsert model)
CREATE TABLE IF NOT EXISTS user_scores (
    user_id STRING,
    track_id STRING,
    score DOUBLE,
    last_updated TIMESTAMP(3),
    PRIMARY KEY (user_id, track_id) NOT ENFORCED
) WITH (
    'log.format' = 'ARROW'
);

-- =========================================================
-- STARROCKS SINK TABLES
-- =========================================================
USE CATALOG default_catalog;

CREATE TABLE IF NOT EXISTS sr_dim_user (
    user_id STRING,
    user_name STRING,
    subscription_tier STRING,
    avg_daily_activities INT,
    registration_date STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-fe:9030',
    'load-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'dim_user',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.buffer-flush.interval-ms' = '5000'
);

CREATE TABLE IF NOT EXISTS sr_dim_track (
    track_id STRING,
    track_name STRING,
    artists STRING,
    album_name STRING,
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
    track_genre STRING,
    PRIMARY KEY (track_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-fe:9030',
    'load-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'dim_track',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.buffer-flush.interval-ms' = '5000'
);

CREATE TABLE IF NOT EXISTS sr_fact_events (
    session_id STRING,
    ts TIMESTAMP(3),
    user_id STRING,
    track_id STRING,
    sequence_number INT,
    action STRING,
    listen_duration_sec INT,
    is_replay BOOLEAN,
    last_play_gap_min INT,
    previous_action STRING,
    user_state STRING,
    PRIMARY KEY (user_id, track_id, ts, session_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-fe:9030',
    'load-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'fact_events',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.buffer-flush.interval-ms' = '5000'
);