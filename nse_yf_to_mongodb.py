import os
from nsetools import Nse
import yfinance as yf
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
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

# Create the 1-day time series collection if it doesn't exist
if '1d_stocks' not in db.list_collection_names():
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

counter = 0
for ticker_symbol in tickers:
    try:
        ticker = yf.Ticker(ticker_symbol)
        data = ticker.history(period='7d', interval='1m')
        if not data.empty:
            data = data.reset_index()
            docs_7d = []
            docs_1d = []
            
            # Get the latest date in the data
            data['date'] = data['Datetime'].dt.date
            latest_date = data['date'].max()
            
            for _, row in data.iterrows():
                doc = {
                    "t": ticker_symbol,
                    "ts": row['Datetime'],
                    "o": float(row['Open']),
                    "h": float(row['High']),
                    "l": float(row['Low']),
                    "c": float(row['Close']),
                    "v": float(row['Volume'])
                }
                docs_7d.append(doc)
                
                # Add to 1d collection only if it's from the latest day
                if row['date'] == latest_date:
                    docs_1d.append(doc)
            
            if docs_7d:
                coll_7d.insert_many(docs_7d)
            if docs_1d:
                coll_1d.insert_many(docs_1d)
        else:
            print(f"No data for {ticker_symbol}")
    except Exception as e:
        print(f"Error fetching {ticker_symbol}: {e}")
    counter += 1
    if counter % 10 == 0:
        print(f"Processed {counter}/{len(tickers)} tickers...")

print("Done.")
print(f"Data inserted to:")
print(f"  - 7d_stocks: Complete 7-day minute-level data")
print(f"  - 1d_stocks: Latest day's minute-level data (for benchmarking)")
