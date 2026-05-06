# Performance & Scalability Report

This document details the benchmark results for the Streaming ETL pipeline, demonstrating its capability to handle high-throughput behavioral data in real-time.

## Throughput Benchmarks

The pipeline was tested across four performance tiers to verify its scalability.

| Tier | Target Throughput | Measured Throughput | Status |
|------|-------------------|---------------------|--------|
| **Light** | 2,000 EPS | 2,554 EPS | ✅ Pass |
| **Medium** | 5,000 EPS | 5,310 EPS | ✅ Pass |
| **Heavy** | 10,000 EPS | 10,396 EPS | ✅ Pass |
| **Max** | 15,000 EPS | 15,419 EPS | ✅ Pass |

**Key Finding:** The architecture scales linearly with increasing load. At peak throughput (**15.4K+ EPS**), the system remains stable with significant resource headroom.

## Resource Efficiency (at 15K EPS)

Measurements taken during peak load on a standard development environment:

*   **StarRocks BE:** ~4% CPU usage
*   **Flink JobManager:** ~8% CPU usage
*   **Fluss Coordinator:** ~2.4% CPU usage
*   **Data Fidelity:** 100% event ingestion verified via SQL row-count audits.

## Scalability Analysis

Based on sustained peak performance, the pipeline is capable of processing:
*   **925,000** events per minute
*   **55,500,000** events per hour
*   **1,332,000,000 events per day**

The use of **Apache Fluss (Arrow-native storage)** and **StarRocks (MPP engine)** allows the system to maintain sub-second latency even under heavy write pressure.

## Architecture Highlights

*   **Arrow-Native Ingestion:** Zero-copy data transfer using Apache Fluss.
*   **Real-Time Enrichment:** High-performance LEFT JOINs in Flink SQL with dimensional tables (80K+ records).
*   **Fault Tolerance:** RockDB state backend with incremental checkpoints for robust state management.
