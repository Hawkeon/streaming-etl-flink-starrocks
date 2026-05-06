# Graph Report - D:\code\DEproj02  (2026-05-02)

## Corpus Check
- 10 files · ~15,854 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 124 nodes · 223 edges · 15 communities detected
- Extraction: 80% EXTRACTED · 20% INFERRED · 0% AMBIGUOUS · INFERRED: 44 edges (avg confidence: 0.63)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]

## God Nodes (most connected - your core abstractions)
1. `CircuitBreaker` - 20 edges
2. `CircuitState` - 15 edges
3. `DLQProducer` - 14 edges
4. `main()` - 13 edges
5. `main()` - 11 edges
6. `ThroughputProducer` - 11 edges
7. `TokenBucket` - 8 edges
8. `main()` - 8 edges
9. `run_cmd()` - 7 edges
10. `run_tier()` - 7 edges

## Surprising Connections (you probably didn't know these)
- `get_fact_events_count()` --calls--> `connect()`  [INFERRED]
  D:\code\DEproj02\benchmark\benchmark_orchestrator.py → D:\code\DEproj02\producers\python-gen\event_producer.py
- `main()` --calls--> `parse_args()`  [INFERRED]
  D:\code\DEproj02\benchmark\benchmark_orchestrator.py → D:\code\DEproj02\producers\python-gen\benchmark_producer.py
- `RetryableError` --uses--> `CircuitState`  [INFERRED]
  D:\code\DEproj02\producers\python-gen\dlq_producer.py → D:\code\DEproj02\producers\python-gen\circuit_breaker.py
- `FatalError` --uses--> `CircuitState`  [INFERRED]
  D:\code\DEproj02\producers\python-gen\dlq_producer.py → D:\code\DEproj02\producers\python-gen\circuit_breaker.py
- `DLQProducer` --uses--> `CircuitState`  [INFERRED]
  D:\code\DEproj02\producers\python-gen\dlq_producer.py → D:\code\DEproj02\producers\python-gen\circuit_breaker.py

## Hyperedges (group relationships)
- **Real-time Data Generation and Ingestion Flow** — producer_py, dataset_csv, polars_df, table_user_logs, apache_fluss [INFERRED]
- **Lakehouse Infrastructure Stack** — docker_compose, apache_fluss, apache_flink, starrocks, minio_rustfs [INFERRED]

## Communities

### Community 0 - "Community 0"
Cohesion: 0.17
Nodes (21): generate_report(), get_docker_stats(), get_fact_events_count(), get_flink_metrics(), load_dim_tables(), main(), Load dimension tables into StarRocks., Submit Flink SQL job via docker exec. (+13 more)

### Community 1 - "Community 1"
Cohesion: 0.18
Nodes (9): main(), parse_args(), Benchmark Producer - High-throughput load generator for Fluss. No session logic,, Thread-safe token bucket for rate limiting., Thread-safe token bucket for rate limiting., Acquire tokens, return time to wait if needed., Acquire tokens, return time to wait if needed., ThroughputProducer (+1 more)

### Community 2 - "Community 2"
Cohesion: 0.19
Nodes (13): connect(), listen_ratio_for(), load_tracks(), load_users(), main(), next_action(), now_ms(), pick_track() (+5 more)

### Community 3 - "Community 3"
Cohesion: 0.19
Nodes (8): CircuitBreaker, Circuit breaker that opens after failure_threshold failures,     stays open for, Call on successful operation., Call on failed operation., Returns True if circuit is OPEN (fail fast).         If OPEN and recovery timeou, Returns True if in half-open state (allowing test calls)., Write failed event to DLQ table., Write with retry, circuit breaker, and DLQ fallback.

### Community 4 - "Community 4"
Cohesion: 0.31
Nodes (8): classify_error(), FatalError, DLQ Producer - writes failed events to Dead Letter Queue with retry + circuit br, Transient error - retry with backoff., Fatal error - send directly to DLQ., Classify exception as retryable or fatal., RetryableError, Exception

### Community 5 - "Community 5"
Cohesion: 0.28
Nodes (5): CircuitState, Circuit Breaker - prevents cascade failures when downstream is unhealthy., Thread-safe token bucket for rate limiting., TokenBucket, Enum

### Community 6 - "Community 6"
Cohesion: 0.42
Nodes (9): Apache Flink - Stream Processing Engine, Apache Fluss - Streaming Storage, Data Pipeline - Real-time Streaming Flow, Docker Compose - Infrastructure Orchestration, Lakehouse Architecture Pattern, MinIO/RustFS - S3 Object Storage, README - Project Documentation, StarRocks - OLAP Database (+1 more)

### Community 7 - "Community 7"
Cohesion: 0.36
Nodes (1): DLQProducer

### Community 8 - "Community 8"
Cohesion: 0.4
Nodes (2): For testing - randomly simulate failures., Actual write to Fluss.

### Community 9 - "Community 9"
Cohesion: 0.67
Nodes (3): polars - DataFrame Library, pyfluss[pyarrow] - Fluss Python Client, requirements.txt - Python Dependencies

### Community 10 - "Community 10"
Cohesion: 1.0
Nodes (0): 

### Community 11 - "Community 11"
Cohesion: 1.0
Nodes (0): 

### Community 12 - "Community 12"
Cohesion: 1.0
Nodes (0): 

### Community 13 - "Community 13"
Cohesion: 1.0
Nodes (1): Thread-safe token bucket for rate limiting.

### Community 14 - "Community 14"
Cohesion: 1.0
Nodes (1): Acquire tokens, return time to wait if needed.

## Knowledge Gaps
- **23 isolated node(s):** `Poll URL until it returns 200 or timeout.`, `Get container resource stats.`, `Get Flink job metrics via REST API.`, `Load dimension tables into StarRocks.`, `Submit Flink SQL job via docker exec.` (+18 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 10`** (1 nodes): `check_count.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 11`** (1 nodes): `load_dim_track_clean.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 12`** (1 nodes): `load_dim_user.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 13`** (1 nodes): `Thread-safe token bucket for rate limiting.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 14`** (1 nodes): `Acquire tokens, return time to wait if needed.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DLQProducer` connect `Community 7` to `Community 8`, `Community 3`, `Community 4`, `Community 5`?**
  _High betweenness centrality (0.251) - this node is a cross-community bridge._
- **Why does `run_cmd()` connect `Community 0` to `Community 1`, `Community 7`?**
  _High betweenness centrality (0.227) - this node is a cross-community bridge._
- **Why does `get_fact_events_count()` connect `Community 0` to `Community 2`?**
  _High betweenness centrality (0.220) - this node is a cross-community bridge._
- **Are the 14 inferred relationships involving `CircuitBreaker` (e.g. with `RetryableError` and `FatalError`) actually correct?**
  _`CircuitBreaker` has 14 INFERRED edges - model-reasoned connections that need verification._
- **Are the 13 inferred relationships involving `CircuitState` (e.g. with `RetryableError` and `FatalError`) actually correct?**
  _`CircuitState` has 13 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `DLQProducer` (e.g. with `CircuitBreaker` and `CircuitState`) actually correct?**
  _`DLQProducer` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Poll URL until it returns 200 or timeout.`, `Get container resource stats.`, `Get Flink job metrics via REST API.` to the rest of the system?**
  _23 weakly-connected nodes found - possible documentation gaps or missing edges._