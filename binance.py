##########################################################################################################3
# Main file to get the data from binance
##########################################################################################################3
import requests
import json
from numpy import log10
import os
import pandas as pd
from datetime import datetime, timezone

DEBUG = True
DATA_FILE = "../Data/data_{}_{}.csv"
timeframe_to_seconds = {
    "1m": 60,        # 1 minute
    "3m": 180,       # 3 minutes
    "5m": 300,       # 5 minutes
    "15m": 900,      # 15 minutes
    "30m": 1800,     # 30 minutes
    "1h": 3600,      # 1 hour
    "2h": 7200,      # 2 hours
    "4h": 14400,     # 4 hours
    "6h": 21600,     # 6 hours
    "8h": 28800,     # 8 hours
    "12h": 43200,    # 12 hours
    "1d": 86400,     # 1 day
    "3d": 259200,    # 3 days
    "1w": 604800,    # 1 week
    "1M": 2592000    # 1 month (approximated as 30 days)
}

def get_data(symbol, interval, start_date=None, end_date=None):
    """Fetches and updates data for a given symbol and interval, optionally filtering by start and end dates."""
    if DEBUG:
        print("Loading data...")

    try:
        df = pd.read_csv(DATA_FILE.format(symbol, interval), index_col="time", parse_dates=True)

        last_time = df.index[-1]
        current_time = datetime.now(timezone.utc).replace(tzinfo=None)

        if DEBUG:
            print(f"Data found - Last time : {last_time}, current time : {current_time}")

        if (current_time - last_time).total_seconds() > 3600:
            if DEBUG:
                print("Last data is outdated. Fetching new data...")
            df = complete_prices(symbol, df, last_time, interval)
            df.to_csv(DATA_FILE.format(symbol, interval))
            if DEBUG:
                print("New data saved")
        else:
            if DEBUG:
                print("Data is up to date!")
    except FileNotFoundError:
        if DEBUG:
            print("Data not found. Fetching new data...")
        df = get_binance_data(symbol, interval)
        df.to_csv(DATA_FILE.format(symbol, interval))
        if DEBUG:
            print("Data saved")

    # Filter by start_date and end_date if provided
    if start_date:
        print("Cutting the start")
        df = df[df.index >= pd.to_datetime(start_date)]
    if end_date:
        print("Cutting the end")
        df = df[df.index <= pd.to_datetime(end_date)]

    return df

def complete_prices(symbol, df, last_time, interval):
    """Fetches and appends new data to the existing DataFrame."""
    new_df = get_binance_data(symbol, interval, start_time=last_time)
    if new_df is not None:
        df = pd.concat([df, new_df]).drop_duplicates().sort_index()
    return df

def get_binance_data(symbol, interval, start_time=None, end_time=None):
    """Fetches data from Binance within a specified time range."""
    if DEBUG: print(f"Fetching data from Binance for {symbol}...")

    base_url = 'https://api.binance.com/api/v1/klines'
    params = {'symbol': symbol, 'interval': interval, 'limit': 1000}

    if end_time:
        params['endTime'] = int(end_time.timestamp() * 1000)

    data = {"time": [], "open": [], "high": [], "low": [], "close": [], "volume": []}


    while True:
        if start_time and ("endTime" in params) and params["endTime"]/1000-(timeframe_to_seconds[interval]*1000)<(start_time.timestamp() * 1000):
            params["startTime"]=int(start_time.timestamp() * 1000)

        response=None
        try:
            response = requests.get(base_url, params=params)
        except Exception as e:
            print(f"Error in connection : {e}")
            return None
        if response.status_code != 200:
            if json.loads(response.text)["code"]==-1121:
                raise ValueError(f"WRONG SYMBOL : {symbol}")
            else:
                raise ValueError(f"Failed to fetch data: {response.status_code}, {response.text}")

        klines = response.json()
        if not klines:
            break

        for entry in klines[::-1]:
            data["time"].append(pd.Timestamp(entry[0], unit='ms'))
            data["open"].append(float(entry[1]))
            data["high"].append(float(entry[2]))
            data["low"].append(float(entry[3]))
            data["close"].append(float(entry[4]))
            data["volume"].append(float(entry[5]))

        params["endTime"] = klines[0][0]-(timeframe_to_seconds[interval]*1000)
        print(f"Got response, next stop {datetime.fromtimestamp(params['endTime']/1000)} ")

    data["time"]=data["time"][::-1]
    data["open"]=data["open"][::-1]
    data["high"]=data["high"][::-1]
    data["low"]=data["low"][::-1]
    data["close"]=data["close"][::-1]
    data["volume"]=data["volume"][::-1]

    df = pd.DataFrame(data)
    df.set_index('time', inplace=True)
    return df

def get_data_from_pairs(symbols, interval, start_date=None, end_date=None):
    return {k:get_data(k, interval, start_date, end_date) for k in symbols}

def get_symbol_filters(symbol):
    exchange_info=None
    if not os.path.isfile(DATA_FILE.format("filter","all")):
        exchange_info_endpoint = 'https://api.binance.com/api/v3/exchangeInfo'
        response = requests.get(exchange_info_endpoint)
        exchange_info = response.json()

        with open(DATA_FILE.format("filter","all"), 'w') as file:
            json.dump(exchange_info, file)
    else:
        with open(DATA_FILE.format("filter","all"), 'r') as file:
            exchange_info = json.load(file)
    return None if not exchange_info else next(item for item in exchange_info['symbols'] if item['symbol'] == symbol)['filters']

def get_rounded_qtty_and_price(symbol):
    """
    Return qtty and price size
    """
    filters = get_symbol_filters(symbol)
    step=1
    tick=1
    for f in filters:
        if f["filterType"]=='LOT_SIZE':
            step=float(f["stepSize"])
        elif f["filterType"]=='PRICE_FILTER':
            tick=float(f["tickSize"])
    return int(-log10(step)),tick,int(-log10(tick))

if __name__ == "__main__":
    df = get_data_from_pairs(["SOLUSDT"], "1h")
    print(df["SOLUSDT"].tail())
