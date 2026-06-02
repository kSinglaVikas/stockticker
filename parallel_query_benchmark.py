import argparse
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient


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


@dataclass
class WorkerStats:
    worker_id: int
    queries: int
    errors: int
    client_latencies_ms: List[float]
    binsize_latencies_ms: Dict[int, List[float]] = field(default_factory=dict)


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


def run_worker(
    worker_id: int,
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    symbols: List[str],
    projection_doc: Dict[str, int],
    stop_at: float,
    wait_ms: float,
) -> WorkerStats:
    client = MongoClient(mongo_uri, appname="parallel-query-benchmark")
    coll = client[db_name][collection_name]

    queries = 0
    errors = 0
    client_latencies_ms: List[float] = []

    while time.time() < stop_at:
        start = time.perf_counter()
        try:
            filter_doc = {"t": random.choice(symbols)}
            _ = coll.find_one(filter_doc, projection_doc, sort=[("ts", 1)])
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            client_latencies_ms.append(elapsed_ms)
            queries += 1
            time.sleep(wait_ms / 1000.0)

        except Exception:
            errors += 1

    client.close()

    return WorkerStats(
        worker_id=worker_id,
        queries=queries,
        errors=errors,
        client_latencies_ms=client_latencies_ms,
        binsize_latencies_ms={},
    )


def _resolve_ts_bounds(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
) -> Tuple[datetime, datetime]:
    """Fetch timestamp bounds for random aggregate windows."""
    fallback_max = datetime.now()
    fallback_min = fallback_max - timedelta(days=30)

    client = MongoClient(mongo_uri, appname="parallel-query-benchmark")
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
    min_ts: datetime,
    max_ts: datetime,
    bin_size_minutes: int,
) -> Tuple[datetime, datetime]:
    total_seconds = max((max_ts - min_ts).total_seconds(), 1.0)
    # Use a random window spanning a practical number of bins.
    bins_in_window = random.randint(20, 80)
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
) -> WorkerStats:
    client = MongoClient(mongo_uri, appname="parallel-query-benchmark")
    coll = client[db_name][collection_name]

    queries = 0
    errors = 0
    client_latencies_ms: List[float] = []
    binsize_latencies_ms: Dict[int, List[float]] = {5: [], 15: [], 30: []}

    while time.time() < stop_at:
        start = time.perf_counter()
        try:
            symbol = random.choice(symbols)
            bin_size_minutes = random.choice([5, 15, 30])
            start_ts, end_ts = _pick_random_window(min_ts, max_ts, bin_size_minutes)

            pipeline = [
                {
                    "$match": {
                        "t": symbol,
                        "ts": {
                            "$gte": start_ts,
                            "$lt": end_ts,
                        },
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

            _ = list(coll.aggregate(pipeline))
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            client_latencies_ms.append(elapsed_ms)
            binsize_latencies_ms.setdefault(bin_size_minutes, []).append(elapsed_ms)
            queries += 1
            time.sleep(wait_ms / 1000.0)
        except Exception:
            errors += 1

    client.close()

    return WorkerStats(
        worker_id=worker_id,
        queries=queries,
        errors=errors,
        client_latencies_ms=client_latencies_ms,
        binsize_latencies_ms=binsize_latencies_ms,
    )


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

    all_client_latencies = [x for w in workers for x in w.client_latencies_ms]
    qps = total_queries / duration_seconds if duration_seconds > 0 else 0.0

    print(f"\n=== {section_name} Summary ===")
    print(f"Start time: {benchmark_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {benchmark_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total queries: {total_queries}")
    print(f"Total errors: {total_errors}")
    print(f"Duration (s): {duration_seconds}")
    print(f"Throughput (QPS): {qps:.2f}")

    if all_client_latencies:
        print("\nClient Latency (ms):")
        print(f"  avg: {statistics.mean(all_client_latencies):.2f}")
        print(f"  p50: {percentile(all_client_latencies, 0.50):.2f}")
        print(f"  p95: {percentile(all_client_latencies, 0.95):.2f}")
        print(f"  p99: {percentile(all_client_latencies, 0.99):.2f}")
        print(f"  max: {max(all_client_latencies):.2f}")


def summarize_aggregate(
    workers: List[WorkerStats],
    duration_seconds: int,
    benchmark_start: datetime,
    benchmark_end: datetime,
) -> None:
    summarize(
        workers,
        duration_seconds,
        benchmark_start,
        benchmark_end,
        section_name="Aggregate Benchmark",
    )

    merged: Dict[int, List[float]] = {5: [], 15: [], 30: []}
    for worker in workers:
        for binsize, latencies in worker.binsize_latencies_ms.items():
            merged.setdefault(binsize, []).extend(latencies)

    rows: List[List[str]] = []
    all_latencies = [x for w in workers for x in w.client_latencies_ms]
    total_queries = len(all_latencies)
    total_qps = total_queries / duration_seconds if duration_seconds > 0 else 0.0

    rows.append(
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
    )

    for binsize in [5, 15, 30]:
        latencies = merged.get(binsize, [])
        q_count = len(latencies)
        qps = q_count / duration_seconds if duration_seconds > 0 else 0.0
        rows.append(
            [
                f"{binsize}m",
                str(q_count),
                _format_metric(qps),
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

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run random symbol queries in parallel for multiple users and measure latency."
        )
    )
    parser.add_argument("--users", type=int, default=20, help="Number of parallel users")
    parser.add_argument(
        "--minutes",
        type=int,
        default=2,
        choices=[2, 3, 4, 5, 10],
        help="Test duration in minutes (2 to 10)",
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
        help="Wait time after each find_one query in milliseconds",
    )
    parser.add_argument(
        "--agg-wait-ms",
        type=float,
        default=10.0,
        help="Wait time after each aggregate query in milliseconds",
    )

    args = parser.parse_args()

    load_dotenv()
    mongo_uri = os.getenv("MONGO_ATLAS_URI", "mongodb://localhost:27017")

    duration_seconds = args.minutes * 60

    symbols = SYMBOL_POOL
    projection_doc = {"_id": 0}

    print("Starting benchmark with settings:")
    print(f"  users={args.users}")
    print(f"  minutes={args.minutes}")
    print(f"  db={args.db}")
    print(f"  collection={args.collection}")
    print(f"  agg_collection={args.agg_collection}")
    print(f"  symbol_pool_size={len(symbols)}")
    print(f"  symbols={symbols}")
    print(f"  find_wait_ms={args.find_wait_ms}")
    print(f"  agg_wait_ms={args.agg_wait_ms}")

    find_start = datetime.now()
    find_stop_at = time.time() + duration_seconds

    workers: List[WorkerStats] = []
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.users) as executor:
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
            )
            for i in range(args.users)
        ]

        for future in as_completed(futures):
            result = future.result()
            with lock:
                workers.append(result)

    find_end = datetime.now()
    workers.sort(key=lambda x: x.worker_id)
    summarize(
        workers,
        duration_seconds,
        find_start,
        find_end,
        section_name="FindOne Benchmark",
    )

    agg_min_ts, agg_max_ts = _resolve_ts_bounds(mongo_uri, args.db, args.agg_collection)
    print("\nStarting aggregate benchmark with settings:")
    print(f"  db={args.db}")
    print(f"  collection={args.agg_collection}")
    print("  bin_sizes=[5, 15, 30]")
    print(f"  ts_range=[{agg_min_ts.isoformat()} to {agg_max_ts.isoformat()}]")

    agg_start = datetime.now()
    agg_stop_at = time.time() + duration_seconds

    agg_workers: List[WorkerStats] = []
    with ThreadPoolExecutor(max_workers=args.users) as executor:
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
            )
            for i in range(args.users)
        ]

        for future in as_completed(futures):
            result = future.result()
            with lock:
                agg_workers.append(result)

    agg_end = datetime.now()
    agg_workers.sort(key=lambda x: x.worker_id)
    summarize_aggregate(
        agg_workers,
        duration_seconds,
        agg_start,
        agg_end,
    )


if __name__ == "__main__":
    main()
