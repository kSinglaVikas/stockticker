import argparse
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

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
class StreamingWorkerStats:
    worker_id: int
    queries: int
    errors: int
    rows_streamed: int
    total_latency_ms: List[float]
    first_row_latency_ms: List[float]
    binsize_total_latency_ms: Dict[int, List[float]] = field(default_factory=dict)


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


def _resolve_ts_bounds(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
) -> Tuple[datetime, datetime]:
    fallback_max = datetime.now()
    fallback_min = fallback_max - timedelta(days=30)

    client = MongoClient(mongo_uri, appname="parallel-query-benchmark-streaming")
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
    bins_in_window = random.randint(20, 80)
    window_seconds = bin_size_minutes * 60 * bins_in_window

    if total_seconds <= window_seconds:
        return min_ts, max_ts

    offset_seconds = random.uniform(0.0, total_seconds - window_seconds)
    start_ts = min_ts + timedelta(seconds=offset_seconds)
    end_ts = start_ts + timedelta(seconds=window_seconds)
    return start_ts, end_ts


def run_streaming_worker(
    worker_id: int,
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    symbols: List[str],
    stop_at: float,
    wait_ms: float,
    min_ts: datetime,
    max_ts: datetime,
    batch_size: int,
) -> StreamingWorkerStats:
    client = MongoClient(mongo_uri, appname="parallel-query-benchmark-streaming")
    coll = client[db_name][collection_name]

    queries = 0
    errors = 0
    rows_streamed = 0
    total_latency_ms: List[float] = []
    first_row_latency_ms: List[float] = []
    binsize_total_latency_ms: Dict[int, List[float]] = {5: [], 15: [], 30: []}

    while time.time() < stop_at:
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

            start = time.perf_counter()
            cursor = coll.aggregate(pipeline, batchSize=batch_size)
            query_rows = 0
            first_row_seen = False

            for _ in cursor:
                query_rows += 1
                if not first_row_seen:
                    first_row_seen = True
                    first_row_latency_ms.append((time.perf_counter() - start) * 1000.0)

            elapsed_ms = (time.perf_counter() - start) * 1000.0
            total_latency_ms.append(elapsed_ms)
            binsize_total_latency_ms.setdefault(bin_size_minutes, []).append(elapsed_ms)
            rows_streamed += query_rows
            queries += 1

            if not first_row_seen:
                # Empty result set: treat first-row latency as total latency.
                first_row_latency_ms.append(elapsed_ms)

            time.sleep(wait_ms / 1000.0)
        except Exception:
            errors += 1

    client.close()

    return StreamingWorkerStats(
        worker_id=worker_id,
        queries=queries,
        errors=errors,
        rows_streamed=rows_streamed,
        total_latency_ms=total_latency_ms,
        first_row_latency_ms=first_row_latency_ms,
        binsize_total_latency_ms=binsize_total_latency_ms,
    )


def summarize_streaming(
    workers: List[StreamingWorkerStats],
    duration_seconds: int,
    benchmark_start: datetime,
    benchmark_end: datetime,
) -> None:
    total_queries = sum(w.queries for w in workers)
    total_errors = sum(w.errors for w in workers)
    total_rows = sum(w.rows_streamed for w in workers)

    all_total_latencies = [x for w in workers for x in w.total_latency_ms]
    all_first_row_latencies = [x for w in workers for x in w.first_row_latency_ms]

    qps = total_queries / duration_seconds if duration_seconds > 0 else 0.0
    rows_per_sec = total_rows / duration_seconds if duration_seconds > 0 else 0.0

    print("\n=== Aggregate Streaming Benchmark Summary ===")
    print(f"Start time: {benchmark_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {benchmark_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total queries: {total_queries}")
    print(f"Total rows streamed: {total_rows}")
    print(f"Total errors: {total_errors}")
    print(f"Duration (s): {duration_seconds}")
    print(f"Throughput (QPS): {qps:.2f}")
    print(f"Rows/sec: {rows_per_sec:.2f}")

    merged: Dict[int, List[float]] = {5: [], 15: [], 30: []}
    for worker in workers:
        for binsize, latencies in worker.binsize_total_latency_ms.items():
            merged.setdefault(binsize, []).extend(latencies)

    rows: List[List[str]] = [
        [
            "overall",
            str(total_queries),
            _format_metric(qps),
            _format_metric(statistics.mean(all_total_latencies) if all_total_latencies else None),
            _format_metric(percentile(all_total_latencies, 0.50)),
            _format_metric(percentile(all_total_latencies, 0.95)),
            _format_metric(percentile(all_total_latencies, 0.99)),
            _format_metric(max(all_total_latencies) if all_total_latencies else None),
            _format_metric(statistics.mean(all_first_row_latencies) if all_first_row_latencies else None),
        ]
    ]

    for binsize in [5, 15, 30]:
        latencies = merged.get(binsize, [])
        q_count = len(latencies)
        bin_qps = q_count / duration_seconds if duration_seconds > 0 else 0.0
        rows.append(
            [
                f"{binsize}m",
                str(q_count),
                _format_metric(bin_qps),
                _format_metric(statistics.mean(latencies) if latencies else None),
                _format_metric(percentile(latencies, 0.50)),
                _format_metric(percentile(latencies, 0.95)),
                _format_metric(percentile(latencies, 0.99)),
                _format_metric(max(latencies) if latencies else None),
                "-",
            ]
        )

    print("\nStreaming Aggregate Metrics:")
    _print_table(
        headers=[
            "segment",
            "queries",
            "qps",
            "avg_total_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "max_ms",
            "avg_first_row_ms",
        ],
        rows=rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run streaming aggregate queries in parallel and measure latency."
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
    parser.add_argument(
        "--agg-collection",
        default="7d_stocks",
        help="Collection name for aggregate benchmark",
    )
    parser.add_argument(
        "--agg-wait-ms",
        type=float,
        default=10.0,
        help="Wait time after each aggregate query in milliseconds",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Mongo cursor batch size for streaming aggregate",
    )

    args = parser.parse_args()

    load_dotenv()
    mongo_uri = os.getenv("MONGO_ATLAS_URI", "mongodb://localhost:27017")

    duration_seconds = args.minutes * 60
    symbols = SYMBOL_POOL

    agg_min_ts, agg_max_ts = _resolve_ts_bounds(mongo_uri, args.db, args.agg_collection)

    print("Starting streaming aggregate benchmark with settings:")
    print(f"  users={args.users}")
    print(f"  minutes={args.minutes}")
    print(f"  db={args.db}")
    print(f"  collection={args.agg_collection}")
    print("  bin_sizes=[5, 15, 30]")
    print(f"  batch_size={args.batch_size}")
    print(f"  agg_wait_ms={args.agg_wait_ms}")
    print(f"  ts_range=[{agg_min_ts.isoformat()} to {agg_max_ts.isoformat()}]")

    benchmark_start = datetime.now()
    stop_at = time.time() + duration_seconds

    workers: List[StreamingWorkerStats] = []
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.users) as executor:
        futures = [
            executor.submit(
                run_streaming_worker,
                worker_id=i + 1,
                mongo_uri=mongo_uri,
                db_name=args.db,
                collection_name=args.agg_collection,
                symbols=symbols,
                stop_at=stop_at,
                wait_ms=args.agg_wait_ms,
                min_ts=agg_min_ts,
                max_ts=agg_max_ts,
                batch_size=args.batch_size,
            )
            for i in range(args.users)
        ]

        for future in as_completed(futures):
            result = future.result()
            with lock:
                workers.append(result)

    benchmark_end = datetime.now()
    workers.sort(key=lambda x: x.worker_id)
    summarize_streaming(
        workers,
        duration_seconds,
        benchmark_start,
        benchmark_end,
    )


if __name__ == "__main__":
    main()
