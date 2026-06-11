import os
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from nsetools import Nse
import yfinance as yf
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

# --- Logging Setup ---
def setup_logging(log_file: str = "nse_yf_to_mongodb.log"):
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

load_dotenv()

# Initialize logging
logger = setup_logging(log_file=f"nse_yf_to_mongodb_{time.strftime('%Y%m%d_%H%M%S')}.log")
logger.info("NSE/Yahoo Finance data ingestion started")

MONGO_ATLAS_URI = os.getenv('MONGO_ATLAS_URI', 'mongodb://localhost:27017')
client = MongoClient(MONGO_ATLAS_URI)
db = client['ohcl_data']
coll_7d = db['7d_stocks']
coll_1d = db['1d_stocks']

# Create the 7-day time series collection if it doesn't exist
if '7d_stocks' not in db.list_collection_names():
    db.create_collection(
        '7d_stocks',
        timeseries={
            'timeField': 'ts',
            'metaField': 't',
            'granularity': 'minutes'
        }
    )

# Delete 1d_stocks collection if it exists to ensure it only contains the latest IST day data after this script runs.
if '1d_stocks' in db.list_collection_names():
    db.drop_collection('1d_stocks')

db.create_collection(
    '1d_stocks',
    timeseries={
        'timeField': 'ts',
        'metaField': 't',
        'granularity': 'minutes'
    }
)

nse = Nse()

stock_codes = nse.get_stock_codes()
tickers = []
for symbol in stock_codes:
    if symbol != 'SYMBOL':
        tickers.append(symbol + '.NS')
        tickers.append(symbol + '.BO')

print(f"Found {len(tickers)} tickers. Starting data fetch and insert...")

try:
    max_workers = max(1, int(os.getenv('YF_FETCH_WORKERS', '4')))
except ValueError:
    max_workers = 4

try:
    max_retries = max(0, int(os.getenv('YF_MAX_RETRIES', '4')))
except ValueError:
    max_retries = 4

try:
    backoff_base_seconds = max(0.1, float(os.getenv('YF_BACKOFF_BASE_SECONDS', '1.0')))
except ValueError:
    backoff_base_seconds = 1.0

try:
    backoff_jitter_seconds = max(0.0, float(os.getenv('YF_BACKOFF_JITTER_SECONDS', '0.5')))
except ValueError:
    backoff_jitter_seconds = 0.5


def fetch_and_insert_ticker(ticker_symbol):
    for attempt in range(max_retries + 1):
        try:
            ticker = yf.Ticker(ticker_symbol)
            data = ticker.history(period='7d', interval='1m')
            if data.empty:
                return ticker_symbol, 0, None

            # Round to 2 decimal places for OHLC and volume to reduce document size.
            data = data.round({'Open': 2, 'High': 2, 'Low': 2, 'Close': 2, 'Volume': 0})

            data = data.reset_index()
            docs_7d = []

            # Yahoo returns UTC; convert to IST for storage and day-level slicing.
            data['ts_ist'] = pd.to_datetime(data['Datetime'], utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)

            for _, row in data.iterrows():
                doc = {
                    "t": ticker_symbol,
                    "ts": row['ts_ist'],
                    "o": float(row['Open']),
                    "h": float(row['High']),
                    "l": float(row['Low']),
                    "c": float(row['Close']),
                    "v": float(row['Volume'])
                }
                docs_7d.append(doc)

            if docs_7d:
                coll_7d.insert_many(docs_7d)

            return ticker_symbol, len(docs_7d), None
        except Exception as e:
            error_msg = str(e)
            is_rate_limited = (
                'Too Many Requests' in error_msg
                or 'Rate limited' in error_msg
                or '429' in error_msg
            )

            if is_rate_limited and attempt < max_retries:
                sleep_seconds = (backoff_base_seconds * (2 ** attempt)) + random.uniform(0, backoff_jitter_seconds)
                time.sleep(sleep_seconds)
                continue

            return ticker_symbol, 0, error_msg

print(f"Using {max_workers} parallel workers for Yahoo fetch")
print(
    f"Retry config: max_retries={max_retries}, "
    f"backoff_base_seconds={backoff_base_seconds}, "
    f"backoff_jitter_seconds={backoff_jitter_seconds}"
)

counter = 0
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(fetch_and_insert_ticker, ticker_symbol) for ticker_symbol in tickers]

    for future in as_completed(futures):
        ticker_symbol, inserted_count, error = future.result()

        if error:
            print(f"Error fetching {ticker_symbol}: {error}")
        elif inserted_count == 0:
            print(f"No data for {ticker_symbol}")

        counter += 1
        if counter % 10 == 0:
            print(f"Processed {counter}/{len(tickers)} tickers...")

print("Done with 7-day data.")

# Build 1d_stocks from 7d_stocks using aggregation pipeline for latest IST date.
latest_date_pipeline = [
    {'$group': {'_id': None, 'latest_day_ist': {'$max': '$ts'}}}
]

latest_day_result = list(coll_7d.aggregate(latest_date_pipeline))

if latest_day_result:
    latest_day_ist = latest_day_result[0]['latest_day_ist']
    copy_pipeline = [
        {'$match': {'ts': latest_day_ist}},
        {'$project': {'_id': 0, 'day_ist': 0}}
    ]

    latest_day_docs = list(coll_7d.aggregate(copy_pipeline))
    if latest_day_docs:
        print(f"Inserting {len(latest_day_docs)} docs for latest IST day into 1d_stocks...")
        coll_1d.insert_many(latest_day_docs)
    print(f"Latest IST date in 1d_stocks: {latest_day_ist.date()}")
    print(f"Inserted {len(latest_day_docs)} docs into 1d_stocks")
else:
    print("No docs were inserted into 1d_stocks")
print(f"Data inserted to:")
print(f"  - 7d_stocks: Complete 7-day minute-level data")
print(f"  - 1d_stocks: Latest day's minute-level data (for benchmarking)")
