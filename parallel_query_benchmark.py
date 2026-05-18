import argparse
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

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
            _ = coll.find_one(filter_doc, projection_doc)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            client_latencies_ms.append(elapsed_ms)
            queries += 1

        except Exception:
            errors += 1

    client.close()

    return WorkerStats(
        worker_id=worker_id,
        queries=queries,
        errors=errors,
        client_latencies_ms=client_latencies_ms,
    )


def summarize(
    workers: List[WorkerStats],
    duration_seconds: int,
    benchmark_start: datetime,
    benchmark_end: datetime,
) -> None:
    total_queries = sum(w.queries for w in workers)
    total_errors = sum(w.errors for w in workers)

    all_client_latencies = [x for w in workers for x in w.client_latencies_ms]
    qps = total_queries / duration_seconds if duration_seconds > 0 else 0.0

    print("\n=== Benchmark Summary ===")
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

    args = parser.parse_args()

    load_dotenv()
    mongo_uri = os.getenv("MONGO_ATLAS_URI", "mongodb://localhost:27017")

    duration_seconds = args.minutes * 60
    benchmark_start = datetime.now()
    stop_at = time.time() + duration_seconds

    symbols = SYMBOL_POOL
    projection_doc = {"_id": 0}

    print("Starting benchmark with settings:")
    print(f"  users={args.users}")
    print(f"  minutes={args.minutes}")
    print(f"  db={args.db}")
    print(f"  collection={args.collection}")
    print(f"  symbol_pool_size={len(symbols)}")
    print(f"  symbols={symbols}")
    print(f"  starting time={benchmark_start.strftime('%Y-%m-%d %H:%M:%S')}")

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
                stop_at=stop_at,
            )
            for i in range(args.users)
        ]

        for future in as_completed(futures):
            result = future.result()
            with lock:
                workers.append(result)

    benchmark_end = datetime.now()
    workers.sort(key=lambda x: x.worker_id)
    summarize(workers, duration_seconds, benchmark_start, benchmark_end)


if __name__ == "__main__":
    main()
