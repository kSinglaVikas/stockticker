import os
import time
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

# --- Logging Setup ---
def setup_logging(log_file: str = "replay_minute_timeseries.log"):
    """Configure logging for app and MongoDB driver."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format))
    logger.addHandler(fh)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format))
    logger.addHandler(ch)
    
    logging.getLogger("pymongo").setLevel(logging.DEBUG)
    logging.getLogger("pymongo.connection").setLevel(logging.DEBUG)
    logging.getLogger("pymongo.topology").setLevel(logging.DEBUG)
    logging.getLogger("pymongo.command").setLevel(logging.DEBUG)
    
    return logger


DEFAULT_START_TS = "2026-05-07T03:45:00.000+00:00"
DEFAULT_END_TS = "2026-05-07T03:59:00.000+00:00"
DEFAULT_DB = "ohcl_data"
DEFAULT_SOURCE_COLLECTION = "1d_stocks"
DEFAULT_TARGET_COLLECTION = "1m_stocks_replay"
DEFAULT_WAIT_SECONDS = 59


def parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def get_mongo_uri() -> str:
    return (
        os.getenv("MONGO_ATLAS_URI")
        or "mongodb://localhost:27017"
    )


def get_env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def ensure_target_collection(
    db: Database,
    target_collection: str,
    drop_existing: bool,
) -> None:
    existing_collections = set(db.list_collection_names())

    if target_collection in existing_collections and drop_existing:
        db[target_collection].drop()
        existing_collections.remove(target_collection)

    if target_collection not in existing_collections:
        db.create_collection(
            target_collection,
            timeseries={
                "timeField": "ts",
                "metaField": "t",
                "granularity": "minutes",
            },
        )


def fetch_grouped_docs(
    source_coll: Collection,
    start_ts: datetime,
    end_ts: datetime,
) -> Dict[datetime, List[dict]]:
    grouped_docs: Dict[datetime, List[dict]] = defaultdict(list)

    cursor = source_coll.find(
        {"ts": {"$gte": start_ts, "$lte": end_ts}},
        {"_id": 0},
    ).sort("ts", 1)

    for doc in cursor:
        ts_value = doc.get("ts")
        if not isinstance(ts_value, datetime):
            continue

        minute_ts = ts_value.replace(second=0, microsecond=0)
        doc["ts"] = minute_ts
        grouped_docs[minute_ts].append(doc)

    return grouped_docs


def replay_grouped_docs(
    target_coll: Collection,
    grouped_docs: Dict[datetime, List[dict]],
    wait_seconds: int,
) -> tuple[int, float]:
    ordered_minutes = sorted(grouped_docs.keys())
    total_inserted = 0
    insert_many_elapsed_seconds = 0.0

    for idx, minute_ts in enumerate(ordered_minutes):
        docs = grouped_docs[minute_ts]
        if docs:
            insert_start = time.perf_counter()
            result = target_coll.insert_many(docs, ordered=False)
            insert_many_elapsed_seconds = time.perf_counter() - insert_start
            inserted = len(result.inserted_ids)
            total_inserted += inserted
            print(
                f"Inserted {inserted} docs for minute {minute_ts.isoformat()} "
                f"in {insert_many_elapsed_seconds:.2f} seconds "
                f"({idx + 1}/{len(ordered_minutes)})"
            )

        if idx < len(ordered_minutes) - 1 and wait_seconds > 0:
            print(f"Sleeping for {wait_seconds} seconds...")
            time.sleep(wait_seconds)

    return total_inserted, insert_many_elapsed_seconds


def main() -> None:
    # Initialize logging
    logger = setup_logging(log_file=f"replay_minute_timeseries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logger.info("Minute replay started")
    
    load_dotenv()

    db_name = os.getenv("REPLAY_DB", DEFAULT_DB)
    source_collection = os.getenv("REPLAY_SOURCE_COLLECTION", DEFAULT_SOURCE_COLLECTION)
    target_collection = os.getenv("REPLAY_TARGET_COLLECTION", DEFAULT_TARGET_COLLECTION)
    start_ts = parse_iso_datetime(os.getenv("REPLAY_START_TS", DEFAULT_START_TS))
    end_ts = parse_iso_datetime(os.getenv("REPLAY_END_TS", DEFAULT_END_TS))
    wait_seconds = int(os.getenv("REPLAY_WAIT_SECONDS", str(DEFAULT_WAIT_SECONDS)))
    drop_target = get_env_bool("REPLAY_DROP_TARGET", default=True)

    if end_ts < start_ts:
        raise ValueError("end-ts must be greater than or equal to start-ts")

    mongo_uri = get_mongo_uri()

    script_start = datetime.now(timezone.utc)
    print("Starting minute replay")
    print(f"Script start time (UTC): {script_start.isoformat()}")
    print(f"Source: {db_name}.{source_collection}")
    print(f"Target: {db_name}.{target_collection}")
    print(f"Range: {start_ts.isoformat()} to {end_ts.isoformat()}")
    print(f"Wait between minute batches: {wait_seconds}s")
    print(f"Drop target before replay: {drop_target}")

    client = MongoClient(mongo_uri, appname="minute-ts-replay")

    try:
        db = client[db_name]
        source_coll = db[source_collection]

        ensure_target_collection(
            db=db,
            target_collection=target_collection,
            drop_existing=drop_target,
        )
        target_coll = db[target_collection]

        grouped_docs = fetch_grouped_docs(
            source_coll=source_coll,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        if not grouped_docs:
            print("No matching documents found in the specified range.")
            return

        total_minutes = len(grouped_docs)
        total_docs = sum(len(v) for v in grouped_docs.values())
        print(f"Found {total_docs} docs across {total_minutes} minute buckets.")

        inserted_total, insert_many_elapsed_seconds = replay_grouped_docs(
            target_coll=target_coll,
            grouped_docs=grouped_docs,
            wait_seconds=wait_seconds,
        )

        script_end = datetime.now(timezone.utc)
        print("Replay complete")
        print(f"Script end time (UTC): {script_end.isoformat()}")
        print(f"insert_many time taken (s): {insert_many_elapsed_seconds:.2f}")
        print(f"Total inserted docs: {inserted_total}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
