-- DLQ (Dead Letter Queue) table for failed events
CREATE TABLE IF NOT EXISTS sr_fact_events_dlq (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id STRING,
    user_id STRING,
    track_id STRING,
    sequence_number INT,
    payload STRING,
    error_type VARCHAR(64),
    error_msg TEXT,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
