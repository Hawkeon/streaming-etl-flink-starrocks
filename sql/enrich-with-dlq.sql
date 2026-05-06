-- Enrich events with DLQ sink (dual-write pattern)
-- Good events go to fact_events_enriched
-- Bad events (NULL user_id/track_id) go to fact_events_dlq
-- Run after dlq-schema.sql and flink-init.sql

CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123');
USE CATALOG fluss_catalog;
CREATE TABLE IF NOT EXISTS fluss_user_logs (user_id STRING, track_id STRING, session_id STRING, sequence_number INT, `action` STRING, listen_duration_sec INT, is_replay BOOLEAN, last_play_gap_min INT, previous_action STRING, user_state STRING, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) WITH ('log.format' = 'ARROW');
USE CATALOG default_catalog;

-- DLQ table for failed events
CREATE TABLE IF NOT EXISTS sr_fact_events_dlq (
    session_id STRING,
    user_id STRING,
    track_id STRING,
    sequence_number INT,
    action STRING,
    listen_duration_sec INT,
    is_replay BOOLEAN,
    last_play_gap_min INT,
    previous_action STRING,
    user_state STRING,
    ts TIMESTAMP(3),
    payload STRING,
    error_type VARCHAR(64),
    error_msg TEXT,
    retry_count INT,
    created_at TIMESTAMP,
    PRIMARY KEY (session_id, sequence_number, ts) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-fe:9030',
    'scan-url' = 'starrocks-fe:8030',
    'load-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'fact_events_dlq',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.strip_outer_brace' = 'true'
);

-- Good events: user_id and track_id not NULL
INSERT INTO sr_fact_events_enriched
SELECT
    l.session_id,
    l.ts,
    l.user_id,
    l.track_id,
    l.sequence_number,
    l.action,
    l.listen_duration_sec,
    l.is_replay,
    l.last_play_gap_min,
    l.previous_action,
    l.user_state,
    u.user_name,
    u.subscription_tier,
    u.avg_daily_activities,
    u.registration_date,
    t.track_name,
    t.artists,
    t.album_name,
    t.popularity,
    t.duration_ms,
    t.explicit,
    t.danceability,
    t.energy,
    t.`key`,
    t.loudness,
    t.mode,
    t.speechiness,
    t.acousticness,
    t.instrumentalness,
    t.liveness,
    t.valence,
    t.tempo,
    t.time_signature,
    t.track_genre
FROM fluss_catalog.`fluss`.fluss_user_logs l
LEFT JOIN sr_dim_user AS u ON l.user_id = u.user_id
LEFT JOIN sr_dim_track AS t ON l.track_id = t.track_id
WHERE l.user_id IS NOT NULL AND l.track_id IS NOT NULL;

-- Bad events: NULL user_id or track_id → DLQ
INSERT INTO sr_fact_events_dlq
SELECT
    l.session_id,
    l.user_id,
    l.track_id,
    l.sequence_number,
    l.action,
    l.listen_duration_sec,
    l.is_replay,
    l.last_play_gap_min,
    l.previous_action,
    l.user_state,
    l.ts,
    CAST(ROW(l.session_id, l.user_id, l.track_id, l.sequence_number, l.action, l.listen_duration_sec, l.is_replay, l.last_play_gap_min, l.previous_action, l.user_state, l.ts) AS STRING),
    CASE
        WHEN l.user_id IS NULL THEN 'MissingUserIdError'
        WHEN l.track_id IS NULL THEN 'MissingTrackIdError'
        ELSE 'ValidationError'
    END,
    CASE
        WHEN l.user_id IS NULL THEN 'user_id is NULL'
        WHEN l.track_id IS NULL THEN 'track_id is NULL'
        ELSE 'validation failed'
    END,
    0,
    CURRENT_TIMESTAMP
FROM fluss_catalog.`fluss`.fluss_user_logs l
WHERE l.user_id IS NULL OR l.track_id IS NULL;
