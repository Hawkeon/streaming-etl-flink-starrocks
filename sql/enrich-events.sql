-- Enrich events with user/track dimension data (full pipeline with catalog)
-- Run after flink-to-starrocks.sql

CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123');
USE CATALOG fluss_catalog;
CREATE TABLE IF NOT EXISTS fluss_user_logs (user_id STRING, track_id STRING, session_id STRING, sequence_number INT, `action` STRING, listen_duration_sec INT, is_replay BOOLEAN, last_play_gap_min INT, previous_action STRING, user_state STRING, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) WITH ('log.format' = 'ARROW');
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
    'scan-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'dim_user',
    'username' = 'root',
    'password' = ''
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
    'scan-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'dim_track',
    'username' = 'root',
    'password' = ''
);

CREATE TABLE IF NOT EXISTS sr_fact_events_enriched (
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
    user_name STRING,
    subscription_tier STRING,
    avg_daily_activities INT,
    registration_date STRING,
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
    PRIMARY KEY (user_id, track_id, ts, session_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-fe:9030',
    'load-url' = 'starrocks-fe:8030',
    'database-name' = 'music_db',
    'table-name' = 'fact_events_enriched',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.strip_outer_brace' = 'true'
);

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
LEFT JOIN sr_dim_track AS t ON l.track_id = t.track_id;
