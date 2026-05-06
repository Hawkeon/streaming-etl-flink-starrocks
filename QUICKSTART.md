# Quick Start

## 1. Start Infrastructure

```bash
cd infrastructure
docker-compose up -d
```

Wait 45 seconds for all services to initialize.

## 2. Copy Flink Connector JARs

The Flink SQL client needs the Fluss and StarRocks connector JARs:

```bash
docker cp lib/fluss-flink-1.19-0.9.0-incubating.jar sql-client:/opt/flink/lib/
docker cp lib/flink-connector-starrocks-1.2.14_flink-1.19.jar sql-client:/opt/flink/lib/
docker restart sql-client
```

## 3. Initialize StarRocks Schema

```bash
docker exec starrocks-fe mysql -uroot -h127.0.0.1 -P9030 < ../sql/starrocks-schema.sql
```

## 4. Load Dimension Tables

```bash
cd ../seeds
pip install pymysql -q
python load_dim_user.py
python load_dim_track_clean.py
```

Verify:
```bash
python check_count.py
```
Expected: ~1,000 users, ~79,000 tracks.

## 5. Start Flink Streaming Job

```bash
docker exec sql-client bash -c "cd /opt/flink && ./bin/sql-client.sh -f /opt/flink/init/flink-init.sql"
```

This creates:
- `fluss_catalog.fluss.flux_user_logs` — source table
- `sr_dim_user`, `sr_dim_track` — StarRocks dimension tables
- `sr_fact_events_enriched` — sink table
- INSERT job: Fluss → Flink (enrich) → StarRocks

## 6. Start Event Producer

```bash
cd ../producers/python-gen
pip install pyfluss[pyarrow] polars pymysql -q
python dlq_producer.py --eps 1000 --warmup-sec 10 --duration-sec 60
```

Or for benchmark mode (4 tiers, full metrics):
```bash
cd ..
python ../benchmark/benchmark_orchestrator.py --tier 4
```

## 7. Verify Data Flow

Check Fluss:
```bash
docker exec coordinator-server fluss table describe fluss.flux_user_logs
```

Check StarRocks enriched events:
```bash
docker exec starrocks-fe mysql -uroot -h127.0.0.1 -P9030 -e \
  "SELECT COUNT(*) FROM music_db.fact_events_enriched;"
```

Wait 10–20 seconds. Rows should increment every few seconds.

## Stop Everything

```bash
docker-compose down -v
```

## Troubleshooting

**StarRocks BE not healthy:**
```bash
docker restart starrocks-be starrocks-fe
docker exec starrocks-fe mysql -uroot -h127.0.0.1 -P9030 -e \
  "ALTER SYSTEM DROP BACKEND IF EXISTS 'starrocks-be:8040'; ALTER SYSTEM ADD BACKEND 'starrocks-be:8040';"
```

**No data in StarRocks:**
1. Check Flink job: http://localhost:8083 (should show RUNNING job)
2. Check producer logs for errors
3. Wait 20 seconds — StarRocks has 5s buffer flush interval

**Flink job failed:**
```bash
docker exec sql-client bash -c "cd /opt/flink && ./bin/sql-client.sh -e 'SHOW JOBS;'"
```

**sql-client JARs missing after restart:**
The `sql-client-init.sh` script handles JAR copying on startup. If tables fail to create, run step 2 again manually.
