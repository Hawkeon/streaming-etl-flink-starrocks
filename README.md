# Real-Time Streaming Data Pipeline

A production-grade lakehouse-style streaming ETL pipeline that ingests behavioral events and enriches them with user/track dimensions in real-time — built for scalability, fault tolerance, and observability.

## Architecture

```
Producer ──[Arrow/Flux]──► Fluss ──[Flink Source]──► Flink ──[JOIN]──► StarRocks
                                   │                              │
                         dim_user ◄─┘                    fact_events_enriched
                         dim_track ◄─┘                          │
                                                                ▼
                                           DLQ (failed events) ◄─┘
                                    circuit_breaker + retry
```

**Data flow:**
1. **Producer** writes events to Fluss in Arrow format (~15K events/sec).
2. **Fluss** stores the streaming log with sub-second read latency.
3. **Flink** reads from Fluss, enriches events via LEFT JOIN on `dim_user` + `dim_track`, writes to StarRocks.
4. **DLQ Producer** handles failures — retry with exponential backoff, circuit breaker, dead letter queue.

## Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| Streaming Storage | Apache Fluss 0.9 | Arrow-log streaming with S3 backend |
| Stream Processing | Apache Flink 1.19 | Real-time enrichment, JOINs, fan-out |
| OLAP Store | StarRocks 3.3 | Columnar queries, materialized views |
| Object Storage | RustFS (MinIO-compatible) | S3 backend for Fluss |
| Producer | Python 3.12 + pyfluss | Behavioral event generation |

## Key Features

- **15K+ events/sec** sustained throughput (benchmark-verified).
- **Fault-tolerant** producer: circuit breaker + exponential backoff retry + DLQ.
- **Real-time enrichment**: LEFT JOIN with `dim_user` (1K rows) and `dim_track` (79K rows).
- **Dual-write pattern**: good events → enriched table, bad events → DLQ.
- **RocksDB state backend** with incremental checkpoints for Flink state persistence.
- **4-tier benchmark suite**: light (2K) → medium (5K) → heavy (10K) → max (15K) EPS.

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for one-command setup.

## Project Structure

```
infrastructure/          # docker-compose.yml, Fluss/StarRocks/Flink cluster
producers/python-gen/    # Event producers (basic + DLQ + benchmark)
benchmark/               # Orchestrated benchmark suite + results
sql/                     # StarRocks schema, Flink SQL scripts
analytics/starrocks/     # DAU and analytics queries
```

## Production-Grade Patterns

| Pattern | Implementation |
|---------|---------------|
| Circuit Breaker | `circuit_breaker.py` — CLOSED/OPEN/HALF_OPEN states |
| Retry + Backoff | `dlq_producer.py` — 3 retries, exponential backoff (1s→2s→4s) |
| Dead Letter Queue | `sql/dlq-schema.sql` + dual-write in Flink SQL |
| Rate Limiting | Token bucket in producer (configurable EPS) |
| State Persistence | RocksDB with incremental checkpoints |

## Benchmark Results

| Tier | Target EPS | Actual EPS |
|------|-----------|------------|
| Light | 2,000 | 2,554 |
| Medium | 5,000 | 5,310 |
| Heavy | 10,000 | 10,396 |
| Max | 15,000 | 15,419 |

Full results in [benchmark/PERFORMANCE_REPORT.md](benchmark/PERFORMANCE_REPORT.md).