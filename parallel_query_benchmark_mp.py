"""
Multiprocessing variant of parallel_query_benchmark.py.

Key differences vs the threading version:
- Uses ProcessPoolExecutor (one OS process per worker) instead of ThreadPoolExecutor.
- Each worker process creates its own MongoClient — PyMongo clients must NOT be
  shared/forked across processes.
- PyMongo CommandLogger is registered inside each worker process individually.
- No shared lock is needed; results are collected via future.result() in the main process.
"""

import argparse
import logging
import os
import random
import statistics
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient
import pymongo.monitoring


# ---------------------------------------------------------------------------
# PyMongo event listener (registered once per worker process)
# ---------------------------------------------------------------------------

class CommandLogger(pymongo.monitoring.CommandListener):
    """Log all MongoDB commands in the current process."""

    def __init__(self, logger):
        self.logger = logger

    def started(self, event):
        self.logger.info(
            f"[COMMAND_STARTED] cmd={event.command_name} request_id={event.request_id}"
        )

    def succeeded(self, event):
        self.logger.info(
            f"[COMMAND_SUCCEEDED] cmd={event.command_name} "
            f"duration_µs={event.duration_micros} request_id={event.request_id}"
        )

    def failed(self, event):
        self.logger.warning(
            f"[COMMAND_FAILED] cmd={event.command_name} "
            f"failure={event.failure} request_id={event.request_id}"
        )


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def setup_logging(log_file: str = "benchmark_mp.log") -> logging.Logger:
    """Configure file + console logging."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = []

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    root_logger.addHandler(fh)

    return root_logger


def _setup_worker_logging(log_file: str, worker_id: int) -> logging.Logger:
    """Minimal logging setup inside a worker process."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = []

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    root_logger.addHandler(fh)

    driver_logger = logging.getLogger("pymongo.driver")
    driver_logger.setLevel(logging.DEBUG)
    driver_logger.handlers = []
    driver_logger.propagate = True

    pymongo.monitoring.register(CommandLogger(driver_logger))

    return logging.getLogger(f"worker.{worker_id}")


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

SYMBOL_POOL = [
    "KFINTECH.NS",
    "ABCAPITAL.NS",
    "FSL.NS",
    "BRITANNIA.NS",
    "EIEL.NS",
    "BAJAJ-AUTO.NS",
    "LICHSGFIN.NS",
    "SWSOLAR.NS",
    "PHOENIXLTD.NS",
    "ARVINDFASN.NS",
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class WorkerStats:
    worker_id: int
    queries: int
    errors: int
    client_latencies_ms: List[float]
    binsize_latencies_ms: Dict[int, List[float]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def _make_client(mongo_uri: str) -> MongoClient:
    """Create a MongoClient suitable for a single worker process."""
    return MongoClient(
        mongo_uri,
        appname="parallel-query-benchmark-mp",
        maxPoolSize=10,   # Each process needs far fewer connections than the threaded version
        minPoolSize=1,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        retryReads=True,
        retryWrites=True,
    )


# ---------------------------------------------------------------------------
# Worker functions (run in child processes)
# ---------------------------------------------------------------------------

def run_worker(
    worker_id: int,
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    symbols: List[str],
    projection_doc: Dict[str, int],
    stop_at: float,
    wait_ms: float,
    log_file: str,
) -> WorkerStats:
    """Find-query worker — runs in its own OS process."""
    worker_logger = _setup_worker_logging(log_file, worker_id)
    worker_logger.info(
        f"Worker {worker_id} started (pid={os.getpid()}) - using {db_name}.{collection_name}"
    )

    client = _make_client(mongo_uri)
    coll = client[db_name][collection_name]

    queries = 0
    errors = 0
    client_latencies_ms: List[float] = []

    try:
        while time.time() < stop_at:
            try:
                filter_doc = {
                    "t": random.choice(symbols),
                    "ts": {
                        "$gte": datetime(2026, 6, 2, 9, 15),
                        "$lt": datetime(2026, 6, 2, 15, 30),
                    },
                }
                start = time.perf_counter()
                _ = list(coll.find(filter_doc, projection_doc,sort=[("ts", 1)]))
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                client_latencies_ms.append(elapsed_ms)

                queries += 1
                time.sleep(wait_ms / 1000.0)

            except Exception as e:
                worker_logger.error(f"Query failed: {e}", exc_info=True)
                errors += 1
    finally:
        client.close()

    worker_logger.info(f"Worker {worker_id} finished - {queries} queries, {errors} errors")
    return WorkerStats(
        worker_id=worker_id,
        queries=queries,
        errors=errors,
        client_latencies_ms=client_latencies_ms,
        binsize_latencies_ms={},
    )


def _resolve_ts_bounds(mongo_uri: str, db_name: str, collection_name: str) -> Tuple[datetime, datetime]:
    """Fetch timestamp bounds. Runs in the main process."""
    fallback_max = datetime.now()
    fallback_min = fallback_max - timedelta(days=30)

    client = _make_client(mongo_uri)
    try:
        coll = client[db_name][collection_name]
        result = list(
            coll.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "min_ts": {"$min": "$ts"},
                            "max_ts": {"$max": "$ts"},
                        }
                    }
                ]
            )
        )
        if not result:
            return fallback_min, fallback_max
        min_ts = result[0].get("min_ts")
        max_ts = result[0].get("max_ts")
        if isinstance(min_ts, datetime) and isinstance(max_ts, datetime):
            return min_ts, max_ts
        return fallback_min, fallback_max
    finally:
        client.close()


def _pick_random_window(
    min_ts: datetime, max_ts: datetime, bin_size_minutes: int
) -> Tuple[datetime, datetime]:
    total_seconds = max((max_ts - min_ts).total_seconds(), 1.0)
    bins_in_window = random.randint(40, 200)
    window_seconds = bin_size_minutes * 60 * bins_in_window
    if total_seconds <= window_seconds:
        return min_ts, max_ts
    offset_seconds = random.uniform(0.0, total_seconds - window_seconds)
    start_ts = min_ts + timedelta(seconds=offset_seconds)
    end_ts = start_ts + timedelta(seconds=window_seconds)
    return start_ts, end_ts


def run_aggregate_worker(
    worker_id: int,
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    symbols: List[str],
    stop_at: float,
    wait_ms: float,
    min_ts: datetime,
    max_ts: datetime,
    log_file: str,
) -> WorkerStats:
    """Aggregation worker — runs in its own OS process."""
    worker_logger = _setup_worker_logging(log_file, worker_id)
    worker_logger.info(
        f"Agg worker {worker_id} started (pid={os.getpid()}) - using {db_name}.{collection_name}"
    )

    client = _make_client(mongo_uri)
    coll = client[db_name][collection_name]

    queries = 0
    errors = 0
    client_latencies_ms: List[float] = []
    binsize_latencies_ms: Dict[int, List[float]] = {5: [], 15: [], 30: []}

    try:
        while time.time() < stop_at:
            try:
                symbol = random.choice(symbols)
                bin_size_minutes = random.choice([5, 15, 30])
                start_ts, end_ts = _pick_random_window(min_ts, max_ts, bin_size_minutes)

                pipeline = [
                    {
                        "$match": {
                            "t": symbol,
                            "ts": {"$gte": start_ts, "$lt": end_ts},
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "$dateTrunc": {
                                    "date": "$ts",
                                    "unit": "minute",
                                    "binSize": bin_size_minutes,
                                }
                            },
                            "o": {"$first": "$o"},
                            "h": {"$max": "$h"},
                            "l": {"$min": "$l"},
                            "c": {"$last": "$c"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]

                start = time.perf_counter()
                result = list(coll.aggregate(pipeline))
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                client_latencies_ms.append(elapsed_ms)
                binsize_latencies_ms.setdefault(bin_size_minutes, []).append(elapsed_ms)

                if queries % 10 == 0:
                    worker_logger.debug(
                        f"aggregate: bin_size={bin_size_minutes}m, "
                        f"stages={len(pipeline)}, docs={len(result)}, elapsed_ms={elapsed_ms:.2f}"
                    )

                queries += 1
                time.sleep(wait_ms / 1000.0)

            except Exception as e:
                worker_logger.error(f"Aggregation failed: {e}", exc_info=True)
                errors += 1
    finally:
        client.close()

    worker_logger.info(f"Agg worker {worker_id} finished - {queries} queries, {errors} errors")
    return WorkerStats(
        worker_id=worker_id,
        queries=queries,
        errors=errors,
        client_latencies_ms=client_latencies_ms,
        binsize_latencies_ms=binsize_latencies_ms,
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _format_metric(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def _print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header_row = "| " + " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))) + " |"

    print(sep)
    print(header_row)
    print(sep)
    for row in rows:
        print("| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(headers))) + " |")
    print(sep)


def summarize(
    workers: List[WorkerStats],
    duration_seconds: int,
    benchmark_start: datetime,
    benchmark_end: datetime,
    section_name: str,
) -> None:
    total_queries = sum(w.queries for w in workers)
    total_errors = sum(w.errors for w in workers)
    all_latencies = [x for w in workers for x in w.client_latencies_ms]
    qps = total_queries / duration_seconds if duration_seconds > 0 else 0.0

    print(f"\n=== {section_name} Summary ===")
    print(f"Start time: {benchmark_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time:   {benchmark_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total queries: {total_queries}")
    print(f"Total errors:  {total_errors}")
    print(f"Duration (s):  {duration_seconds}")
    print(f"Throughput (QPS): {qps:.2f}")

    if all_latencies:
        print("\nClient Latency (ms):")
        print(f"  avg: {statistics.mean(all_latencies):.2f}")
        print(f"  p50: {percentile(all_latencies, 0.50):.2f}")
        print(f"  p95: {percentile(all_latencies, 0.95):.2f}")
        print(f"  p99: {percentile(all_latencies, 0.99):.2f}")
        print(f"  max: {max(all_latencies):.2f}")


def summarize_aggregate(
    workers: List[WorkerStats],
    duration_seconds: int,
    benchmark_start: datetime,
    benchmark_end: datetime,
) -> None:
    summarize(workers, duration_seconds, benchmark_start, benchmark_end, "Aggregate Benchmark")

    merged: Dict[int, List[float]] = {5: [], 15: [], 30: []}
    for worker in workers:
        for binsize, latencies in worker.binsize_latencies_ms.items():
            merged.setdefault(binsize, []).extend(latencies)

    all_latencies = [x for w in workers for x in w.client_latencies_ms]
    total_queries = len(all_latencies)
    total_qps = total_queries / duration_seconds if duration_seconds > 0 else 0.0

    rows: List[List[str]] = [
        [
            "overall",
            str(total_queries),
            _format_metric(total_qps),
            _format_metric(statistics.mean(all_latencies) if all_latencies else None),
            _format_metric(percentile(all_latencies, 0.50)),
            _format_metric(percentile(all_latencies, 0.95)),
            _format_metric(percentile(all_latencies, 0.99)),
            _format_metric(max(all_latencies) if all_latencies else None),
        ]
    ]

    for binsize in [5, 15, 30]:
        latencies = merged.get(binsize, [])
        q_count = len(latencies)
        qps_bin = q_count / duration_seconds if duration_seconds > 0 else 0.0
        rows.append(
            [
                f"{binsize}m",
                str(q_count),
                _format_metric(qps_bin),
                _format_metric(statistics.mean(latencies) if latencies else None),
                _format_metric(percentile(latencies, 0.50)),
                _format_metric(percentile(latencies, 0.95)),
                _format_metric(percentile(latencies, 0.99)),
                _format_metric(max(latencies) if latencies else None),
            ]
        )

    print("\nAggregate Metrics by Bin Size:")
    _print_table(
        headers=["segment", "queries", "qps", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"],
        rows=rows,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Multiprocessing variant: run random symbol queries in parallel "
            "OS processes and measure latency."
        )
    )
    parser.add_argument("--users", type=int, default=20, help="Number of parallel worker processes")
    parser.add_argument(
        "--minutes",
        type=int,
        default=2,
        choices=[1, 2, 3, 4, 5, 10],
        help="Test duration in minutes (1 to 10)",
    )
    parser.add_argument("--db", default="ohcl_data", help="Database name")
    parser.add_argument("--collection", default="1d_stocks", help="Collection name")
    parser.add_argument(
        "--agg-collection",
        default="7d_stocks",
        help="Collection name for aggregate benchmark",
    )
    parser.add_argument(
        "--find-wait-ms",
        type=float,
        default=1.0,
        help="Wait time after each find query in milliseconds",
    )
    parser.add_argument(
        "--agg-wait-ms",
        type=float,
        default=10.0,
        help="Wait time after each aggregate query in milliseconds",
    )

    args = parser.parse_args()

    load_dotenv()

    log_file = f"logs/benchmark_mp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logging(log_file=log_file)
    logger.info("Multiprocessing benchmark started")

    mongo_uri = os.getenv("MONGO_ATLAS_URI", "mongodb://localhost:27017")
    duration_seconds = args.minutes * 60
    symbols = SYMBOL_POOL
    projection_doc = {"_id": 0, "t": 0}

    print("Starting multiprocessing benchmark with settings:")
    print(f"  users (processes)={args.users}")
    print(f"  minutes={args.minutes}")
    print(f"  db={args.db}")
    print(f"  collection={args.collection}")
    print(f"  agg_collection={args.agg_collection}")
    print(f"  symbol_pool_size={len(symbols)}")
    print(f"  find_wait_ms={args.find_wait_ms}")
    print(f"  agg_wait_ms={args.agg_wait_ms}")

    # Verify connectivity from the main process before spawning children
    try:
        client = _make_client(mongo_uri)
        client.admin.command("ping")
        logger.info("Successfully connected to MongoDB")
        try:
            info = client.server_info()
            logger.info(
                f"MongoDB host={info.get('host', 'unknown')}, version={info.get('version', 'unknown')}"
            )
        except Exception as e:
            logger.warning(f"Could not retrieve server info: {e}")
        client.close()
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)
        return

    # ------------------------------------------------------------------
    # Find benchmark
    # ------------------------------------------------------------------
    find_start = datetime.now()
    find_stop_at = time.time() + duration_seconds

    find_workers: List[WorkerStats] = []
    with ProcessPoolExecutor(max_workers=args.users) as executor:
        futures = [
            executor.submit(
                run_worker,
                worker_id=i + 1,
                mongo_uri=mongo_uri,
                db_name=args.db,
                collection_name=args.collection,
                symbols=symbols,
                projection_doc=projection_doc,
                stop_at=find_stop_at,
                wait_ms=args.find_wait_ms,
                log_file=log_file,
            )
            for i in range(args.users)
        ]
        for future in as_completed(futures):
            find_workers.append(future.result())

    find_end = datetime.now()
    find_workers.sort(key=lambda x: x.worker_id)
    summarize(find_workers, duration_seconds, find_start, find_end, section_name="Find Benchmark")

    # ------------------------------------------------------------------
    # Aggregate benchmark
    # ------------------------------------------------------------------
    agg_min_ts, agg_max_ts = _resolve_ts_bounds(mongo_uri, args.db, args.agg_collection)
    print("\nStarting aggregate benchmark with settings:")
    print(f"  db={args.db}")
    print(f"  collection={args.agg_collection}")
    print("  bin_sizes=[5, 15, 30]")
    print(f"  ts_range=[{agg_min_ts.isoformat()} to {agg_max_ts.isoformat()}]")

    agg_start = datetime.now()
    agg_stop_at = time.time() + duration_seconds

    agg_workers: List[WorkerStats] = []
    with ProcessPoolExecutor(max_workers=args.users) as executor:
        futures = [
            executor.submit(
                run_aggregate_worker,
                worker_id=i + 1,
                mongo_uri=mongo_uri,
                db_name=args.db,
                collection_name=args.agg_collection,
                symbols=symbols,
                stop_at=agg_stop_at,
                wait_ms=args.agg_wait_ms,
                min_ts=agg_min_ts,
                max_ts=agg_max_ts,
                log_file=log_file,
            )
            for i in range(args.users)
        ]
        for future in as_completed(futures):
            agg_workers.append(future.result())

    agg_end = datetime.now()
    agg_workers.sort(key=lambda x: x.worker_id)
    summarize_aggregate(agg_workers, duration_seconds, agg_start, agg_end)


if __name__ == "__main__":
    main()
