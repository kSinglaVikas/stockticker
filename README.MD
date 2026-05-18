# Stock Ticker - Time Series Data Pipeline

A Python project for fetching, benchmarking, and replaying stock market data (OHLC) in MongoDB using time series collections.

## Project Overview

This project contains scripts to:
- **nse_yf_to_mongodb.py**: Fetch 7-day minute-level OHLC data from Yahoo Finance and store in MongoDB time series collection
- **parallel_query_benchmark.py**: Run concurrent queries against the collection to measure latency and throughput
- **replay_minute_timeseries.py**: Replay historical minute data to simulate real-time data ingestion

## Prerequisites

- Python 3.8+
- MongoDB Atlas cluster (or local MongoDB instance)
- Internet connection (for Yahoo Finance API)

## Setup

### 1) Clone and Setup Python Environment

```bash
cd stockticker
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2) Configure Environment Variables

Copy the example file and update with your MongoDB connection string:

```bash
cp .env.example .env
```

Edit `.env` and set:

```env
MONGO_ATLAS_URI=mongodb+srv://<username>:<password>@<cluster-url>/?readPreference=nearest&appname=py-perf&ssl=true
```

Optional environment variables for `replay_minute_timeseries.py`:

```env
REPLAY_DB=ohcl_data
REPLAY_SOURCE_COLLECTION=1d_stocks
REPLAY_TARGET_COLLECTION=1m_stocks_replay
REPLAY_START_TS=2026-05-07T03:45:00.000+00:00
REPLAY_END_TS=2026-05-07T03:59:00.000+00:00
REPLAY_WAIT_SECONDS=59
REPLAY_DROP_TARGET=true
```

## Running Scripts

Make sure your virtual environment is activated:

```bash
source venv/bin/activate
```

### Workflow: Complete Pipeline

#### Step 1: Fetch NSE/Yahoo Finance Data

```bash
python3 nse_yf_to_mongodb.py
```

This fetches 7-day minute-level OHLC data for all NSE stocks and populates two collections:
- **`7d_stocks`**: Complete 7-day minute-level data for all tickers
- **`1d_stocks`**: Latest day's minute-level data (extracted from the 7-day fetch) for benchmarking

To speed up Yahoo fetch, run in parallel with configurable workers (default: 4):
Avoiding too many workers to prevent hitting Yahoo Finance rate limits. Adjust based on your network and system capabilities.

```bash
YF_FETCH_WORKERS=4 python3 nse_yf_to_mongodb.py
```

#### Step 2: Run Query Benchmark

Benchmark queries against the 1-day data:

```bash
python3 parallel_query_benchmark.py --users 20 --minutes 2 --db ohcl_data --collection 1d_stocks
```

Benchmark options:
- `--users`: Number of parallel query workers (default: 20)
- `--minutes`: Duration in minutes [2, 3, 4, 5, 10] (default: 2)
- `--db`: Database name (default: ohcl_data)
- `--collection`: Collection name (use `1d_stocks` for latest day data)

#### Step 3: Replay Minute Time Series (Optional)

Replay historical minute data to simulate real-time ingestion:

```bash
python3 replay_minute_timeseries.py
```

In another terminal, you can run the benchmark against the replay collection:

```bash
python3 parallel_query_benchmark.py --users 20 --minutes 15 --db ohcl_data --collection 1m_stocks_replay
```

This replays grouped minute data with configurable wait times between batches. Uses source collection (default: `1d_stocks`) and creates target collection (default: `1m_stocks_replay`). Customize via environment variables.

## MongoDB Collections

Both collections are time series collections with:
- **timeField**: `ts` (timestamp)
- **metaField**: `t` (ticker symbol)
- **granularity**: `minutes`

**Collections created by this project:**
- **`7d_stocks`**: Complete 7-day minute-level OHLC data for all tickers (for analysis/archival)
- **`1d_stocks`**: Latest day's minute-level OHLC data (for benchmarking performance)

## Troubleshooting

1. **Mongo connection errors**:
   - Verify `MONGO_ATLAS_URI` in `.env`
   - Check MongoDB Atlas network access list includes your IP
   - Ensure connectivity to MongoDB Atlas

2. **Yahoo Finance errors**:
   - Some tickers may not have data available
   - The script handles errors gracefully and continues

3. **Performance issues**:
   - For benchmark, use higher `--users` and `--minutes` values for more accurate results
   - Monitor CPU/memory usage during long runs

## Project Structure

- `nse_yf_to_mongodb.py` - Data ingestion script
- `parallel_query_benchmark.py` - Performance benchmarking tool
- `replay_minute_timeseries.py` - Time series replay utility
- `requirements.txt` - Python dependencies
- `.env` - Environment configuration (not in git)
- `Queries.md` - Useful MongoDB query examples
