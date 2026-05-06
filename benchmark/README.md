# Benchmark Results

**Architecture:** Fluss 0.9 (Arrow log) → Flink 1.19 → StarRocks 3.3
**Date:** 2026-05-02
**Commit:** `6b6a698`

## Throughput — All Tiers

| Tier | Target EPS | Actual EPS | Status |
|------|-----------|------------|--------|
| Light | 2,000 | **2,554** | ✅ |
| Medium | 5,000 | **5,310** | ✅ |
| Heavy | 10,000 | **10,396** | ✅ |
| Max | 15,000 | **15,419** | ✅ |

Pipeline sustains 15,419 events/sec at max tier — exceeds 15K target.

## Resource Utilization (at 15K EPS)

| Component | CPU% | Memory |
|-----------|------|--------|
| StarRocks BE | ~4% | ~570 MB / 13.5 GB |
| StarRocks FE | ~16% | ~2.2 GB / 13.5 GB |
| Flink TaskManager | ~10% | ~3.8 GB / 4 GB |
| Fluss Coordinator | ~2% | ~336 MB / 13.5 GB |
| RustFS | ~1% | ~145 MB / 13.5 GB |

## Event Storage

| Table | Rows |
|-------|------|
| `fact_events_enriched` | 857,490+ |

Enrichment LEFT JOINs dim_user (1K rows) + dim_track (79K rows) successfully.

## Daily Capacity Estimate

**15,419 EPS × 86,400 sec ≈ 1.33 billion events/day**

## Run the Benchmark

```bash
python benchmark/benchmark_orchestrator.py --tier 4
```

Output saved to `benchmark/results/`.

## Architecture

```
Producer (pyfluss)
    ↓ Arrow/Flux
Fluss (coordinator-server:9123)
    ↓ Flink Source
Flink (enrichment job)
    ↓ StarRocks sink
fact_events_enriched
    ↓
DLQ (failed events)
```
