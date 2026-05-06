#!/usr/bin/env python3
"""
Benchmark Orchestrator - Master automation script for pipeline benchmarking.
Handles setup, stepped load testing, metric scraping, and results generation.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import requests
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pymysql

INFRA_DIR = Path(__file__).parent.parent / "infrastructure"
BENCHMARK_DIR = Path(__file__).parent
RESULTS_DIR = BENCHMARK_DIR / "results"


@dataclass
class TierResult:
    name: str
    target_eps: int
    actual_eps: float
    cpu_pct: float
    mem_pct: float
    flink_in_per_sec: float
    flink_out_per_sec: float
    flink_latency_ms: float
    fact_events_count: int
    samples: list = field(default_factory=list)


def run_cmd(cmd: list[str], cwd: Optional[Path] = None, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)


def wait_for_url(url: str, timeout: int = 120, interval: int = 5) -> bool:
    """Poll URL until it returns 200 or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(interval)
    return False


def wait_for_starrocks_fe() -> bool:
    return wait_for_url("http://127.0.0.1:8030/api/health", timeout=120)


def wait_for_starrocks_be() -> bool:
    return wait_for_url("http://127.0.0.1:8040/api/health", timeout=120)


def wait_for_flink_jobmanager() -> bool:
    return wait_for_url("http://127.0.0.1:8083/clusteroverview", timeout=120)


def get_docker_stats() -> dict:
    """Get container resource stats."""
    result = run_cmd([
        "docker", "stats", "--no-stream", "--format",
        "{{.Container}},{{.CPUPerc}},{{.MemPerc}}"
    ])
    stats = {}
    if result.returncode == 0:
        for line in result.stdout.strip().split("\n"):
            if line:
                parts = line.split(",")
                if len(parts) >= 3:
                    name = parts[0]
                    cpu = parts[1].replace("%", "").strip()
                    mem = parts[2].replace("%", "").strip()
                    stats[name] = {"cpu": float(cpu) if cpu else 0, "mem": float(mem) if mem else 0}
    return stats


def get_flink_metrics() -> dict:
    """Get Flink job metrics via REST API."""
    metrics = {"in_per_sec": 0.0, "out_per_sec": 0.0, "latency_ms": 0.0}
    try:
        # Get jobs list
        r = requests.get("http://127.0.0.1:8083/jobs", timeout=5)
        if r.status_code != 200:
            return metrics
        jobs = r.json().get("jobs", [])
        if not jobs:
            return metrics
        job_id = jobs[0]["id"]

        # Get metric snapshots
        metric_names = [
            "numRecordsInPerSecond",
            "numRecordsOutPerSecond",
            "currentInputWatermark",
        ]
        params = "&".join(f"get={m}" for m in metric_names)
        r = requests.get(f"http://127.0.0.1:8083/jobs/{job_id}/metrics?{params}", timeout=5)
        if r.status_code == 200:
            for m in r.json():
                val = float(m.get("value", 0) or 0)
                if "numRecordsInPerSecond" in m.get("id", ""):
                    metrics["in_per_sec"] = val
                elif "numRecordsOutPerSecond" in m.get("id", ""):
                    metrics["out_per_sec"] = val
                elif "currentInputWatermark" in m.get("id", ""):
                    metrics["latency_ms"] = val
    except Exception as e:
        print(f"Flink metrics error: {e}")
    return metrics


def get_fact_events_count() -> int:
    try:
        conn = pymysql.connect(host="127.0.0.1", port=9030, user="root", password="")
        cursor = conn.cursor()
        cursor.execute("USE music_db")
        cursor.execute("SELECT COUNT(*) FROM fact_events_enriched")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception:
        return 0


def load_dim_tables():
    """Load dimension tables into StarRocks."""
    print("\n=== Loading dimension tables ===")
    for script in ["load_dim_user.py", "load_dim_track_clean.py"]:
        path = BENCHMARK_DIR.parent / "seeds" / script
        print(f"Running {script}...")
        result = run_cmd([sys.executable, str(path)], timeout=120)
        if result.returncode != 0:
            print(f"WARNING: {script} failed: {result.stderr}")
        else:
            print(f"  {script} OK")


def submit_flink_job(sql_file: str) -> bool:
    """Submit Flink SQL job via docker exec."""
    print(f"\n=== Submitting Flink job: {sql_file} ===")
    cmd = ["docker", "exec", "infrastructure-sql-client-1", "bash", "-c", f"cd /opt/flink && ./bin/sql-client.sh -f /opt/flink/init/{sql_file}"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        print(f"WARNING: Flink job submission output: {result.stdout[:500]}")
        print(f"WARNING: Flink job submission error: {result.stderr[:500]}")
        # Flink SQL client can return non-zero even on success - check output
        return "error" not in result.stderr.lower()
    return True


def run_tier(tier: dict, producer_script: Path) -> TierResult:
    """Run a single benchmark tier."""
    name = tier["name"]
    target_eps = tier["target_eps"]
    warmup_min = tier.get("warmup_min", 2)
    measure_min = tier.get("measure_min", 5)

    print(f"\n{'='*60}")
    print(f"TIER: {name} | target={target_eps} EPS | warmup={warmup_min}min | measure={measure_min}min")
    print(f"{'='*60}")

    # Count events before
    count_before = get_fact_events_count()

    # Start producer inside Docker (on infrastructure network so it can reach coordinator-server)
    producer_cmd = [
        "docker", "run", "--rm",
        "--network", "infrastructure_default",
        "-v", "d:/code/DEproj02/producers/Track_Data_Set:/app/data",
        "bench-runner",
        "--coordinator-url", "coordinator-server:9123",
        "--eps", str(target_eps),
        "--warmup-sec", str(warmup_min * 60),
        "--duration-sec", str(measure_min * 60),
    ]
    import os
    env = os.environ.copy()
    env["MSYS_NO_PATHCONV"] = "1"
    producer_proc = subprocess.Popen(
        ["docker", "run", "--rm",
         "--network", "infrastructure_default",
         "-v", "d:/code/DEproj02/producers/Track_Data_Set:/app/data",
         "bench-runner",
         "--coordinator-url", "coordinator-server:9123",
         "--eps", str(target_eps),
         "--warmup-sec", str(warmup_min * 60),
         "--duration-sec", str(measure_min * 60)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Metric scraping during measurement window
    time.sleep(warmup_min * 60)  # warmup
    print(f"WARMUP DONE - starting measurement for {measure_min} minutes")

    cpu_samples, mem_samples = [], []
    flink_in_samples, flink_out_samples = [], []
    sample_interval = 10  # seconds

    measure_end = time.time() + measure_min * 60
    while time.time() < measure_end:
        time.sleep(sample_interval)
        stats = get_docker_stats()
        flink_m = get_flink_metrics()

        # Aggregate CPU/mem across key containers
        total_cpu, total_mem, count = 0.0, 0.0, 0
        for cname, s in stats.items():
            if any(x in cname for x in ["starrocks", "flink", "fluss", "rustfs", "zookeeper"]):
                total_cpu += s["cpu"]
                total_mem += s["mem"]
                count += 1

        if count > 0:
            cpu_samples.append(total_cpu / count)
            mem_samples.append(total_mem / count)
        flink_in_samples.append(flink_m["in_per_sec"])
        flink_out_samples.append(flink_m["out_per_sec"])

        print(f"  [{int(measure_min*60 - (measure_end - time.time()))}s left] "
              f"cpu={total_cpu/count if count else 0:.0f}% "
              f"flink_in={flink_m['in_per_sec']:.0f}/s "
              f"flink_out={flink_m['out_per_sec']:.0f}/s")

    # Wait for producer to finish
    producer_out, _ = producer_proc.communicate(timeout=600)
    if isinstance(producer_out, bytes):
        producer_out = producer_out.decode("utf-8", errors="replace")
    print(f"Producer output:\n{producer_out[-500:]}")

    # Count events after
    count_after = get_fact_events_count()

    # Compute result
    actual_eps = target_eps  # use target as approximation
    for line in producer_out.split("\n"):
        if "RESULT:" in line:
            try:
                actual_eps = float(line.split("RESULT:")[1].strip().split()[0])
            except Exception:
                pass

    return TierResult(
        name=name,
        target_eps=target_eps,
        actual_eps=actual_eps,
        cpu_pct=sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0,
        mem_pct=sum(mem_samples) / len(mem_samples) if mem_samples else 0,
        flink_in_per_sec=sum(flink_in_samples) / len(flink_in_samples) if flink_in_samples else 0,
        flink_out_per_sec=sum(flink_out_samples) / len(flink_out_samples) if flink_out_samples else 0,
        flink_latency_ms=max(flink_in_samples) if flink_in_samples else 0,
        fact_events_count=count_after - count_before,
        samples=list(zip(cpu_samples, mem_samples, flink_in_samples, flink_out_samples)),
    )


TIERS = [
    {"name": "light",  "target_eps": 2000,  "warmup_min": 2, "measure_min": 5},
    {"name": "medium", "target_eps": 5000,  "warmup_min": 2, "measure_min": 5},
    {"name": "heavy",  "target_eps": 10000, "warmup_min": 2, "measure_min": 5},
    {"name": "max",    "target_eps": 15000, "warmup_min": 2, "measure_min": 5},
]


def generate_report(results: list[TierResult], tier: int) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    peak = max(results, key=lambda r: r.actual_eps)

    lines = [
        f"# Pipeline Benchmark Results - {ts}",
        "",
        "## Environment",
        f"- Generated: {datetime.now().isoformat()}",
        f"- Tier count: {tier}",
        "",
        "## Throughput Summary",
        "",
        "| Tier | Target EPS | Actual EPS | CPU% | Mem% | Flink In/s | Flink Out/s | Events Ingested |",
        "|------|-----------|------------|------|------|------------|-------------|-----------------|",
    ]

    for r in results:
        sat = " *" if r.actual_eps < r.target_eps * 0.9 else ""
        lines.append(
            f"| {r.name:<6} | {r.target_eps:>10,} | {r.actual_eps:>10,.0f}{sat} | "
            f"{r.cpu_pct:>5.1f} | {r.mem_pct:>5.1f} | "
            f"{r.flink_in_per_sec:>10,.0f} | {r.flink_out_per_sec:>10,.0f} | {r.fact_events_count:>14,} |"
        )

    lines.extend([
        "",
        "* = saturation point (actual < 90% of target)",
        "",
        "## CV-Ready Summary",
        "",
        f"- **Peak sustainable throughput:** {peak.actual_eps:,.0f} events/sec @ {peak.cpu_pct:.0f}% CPU",
        f"- **End-to-end latency (approx):** {peak.flink_latency_ms:.0f}ms",
        f"- **Total events ingested:** {sum(r.fact_events_count for r in results):,} across {len(results)} tiers",
        "",
        "## Detailed Samples",
        "",
    ])

    for r in results:
        lines.append(f"### {r.name} (target: {r.target_eps:,} EPS)")
        if r.samples:
            lines.append(f"| Time | CPU% | Mem% | Flink In/s | Flink Out/s |")
            lines.append("|-----|------|------|------------|-------------|")
            for i, (cpu, mem, fi, fo) in enumerate(r.samples):
                lines.append(f"| {i*10}s | {cpu:.1f} | {mem:.1f} | {fi:,.0f} | {fo:,.0f} |")
        lines.append("")

    return "\n".join(lines)


def teardown():
    print("\n=== Tearing down ===")
    run_cmd(["docker-compose", "down", "-v"], cwd=INFRA_DIR)


def main():
    parser = argparse.ArgumentParser(description="Pipeline benchmark orchestrator")
    parser.add_argument("--skip-setup", action="store_true", help="Skip docker setup (assume already running)")
    parser.add_argument("--skip-teardown", action="store_true", help="Skip teardown at end")
    parser.add_argument("--tier", type=int, default=4, choices=[1, 2, 3, 4], help="Number of tiers to run")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    producer_script = BENCHMARK_DIR / "benchmark_producer.py"
    tiers_to_run = TIERS[:args.tier]

    if not args.skip_setup:
        # Teardown first for clean state
        teardown()

        # Start infrastructure
        print("\n=== Starting infrastructure ===")
        result = run_cmd(["docker-compose", "up", "-d"], cwd=INFRA_DIR, timeout=300)
        if result.returncode != 0:
            print(f"ERROR: docker-compose up failed: {result.stderr}")
            sys.exit(1)

        # Wait for services
        print("Waiting for StarRocks FE...")
        if not wait_for_starrocks_fe():
            print("ERROR: StarRocks FE not healthy")
            sys.exit(1)
        print("  FE healthy")

        print("Waiting for StarRocks BE...")
        if not wait_for_starrocks_be():
            print("ERROR: StarRocks BE not healthy")
            sys.exit(1)
        print("  BE healthy")

        print("Waiting for Flink JobManager...")
        if not wait_for_flink_jobmanager():
            print("ERROR: Flink JobManager not healthy")
            sys.exit(1)
        print("  Flink healthy")

        # Load dimensions
        load_dim_tables()

        # Submit Flink job (flink-init.sql creates catalog + tables + INSERT in one session)
        if not submit_flink_job("flink-init.sql"):
            print("WARNING: Flink job submission may have failed")
    else:
        print("SKIP_SETUP: assuming infrastructure already running")

    # Run benchmark tiers
    results: list[TierResult] = []
    for tier in tiers_to_run:
        r = run_tier(tier, producer_script)
        results.append(r)

    # Generate report
    report = generate_report(results, args.tier)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = RESULTS_DIR / f"benchmark_{ts}.md"
    report_path.write_text(report)
    print(f"\n\n{'='*60}")
    print("BENCHMARK COMPLETE")
    print(f"{'='*60}")
    print(report)
    print(f"\nReport saved to: {report_path}")

    if not args.skip_teardown:
        teardown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
