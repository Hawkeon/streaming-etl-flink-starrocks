-- Flink Streaming Job: fluss_user_logs -> StarRocks fact_events (non-enriched)
-- Creates catalog + table + INSERT in one session

CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123');
USE CATALOG fluss_catalog;
CREATE TABLE IF NOT EXISTS fluss_user_logs (user_id STRING, track_id STRING, session_id STRING, sequence_number INT, `action` STRING, listen_duration_sec INT, is_replay BOOLEAN, last_play_gap_min INT, previous_action STRING, user_state STRING, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) WITH ('log.format' = 'ARROW');
USE CATALOG default_catalog;

CREATE TABLE sr_fact_events (
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
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.strip_outer_brace' = 'true'
);

INSERT INTO sr_fact_events
SELECT
    session_id,
    ts,
    user_id,
    track_id,
    sequence_number,
    action,
    listen_duration_sec,
    is_replay,
    last_play_gap_min,
    previous_action,
    user_state
FROM fluss_catalog.`fluss`.fluss_user_logs;
