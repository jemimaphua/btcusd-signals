import requests
import pandas as pd
import json

def test_endpoints():
    random_endpoint = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d"
    r = requests.get(random_endpoint)
    print(r.status_code)
    print(r.text[:200])

def pull_price(symbol, interval):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}"
    response = requests.get(url)
    data = json.loads(response.text)
    # print(data) # data returned has no column names
    df = pd.DataFrame(data, columns=["Open Time", "Open", "High", "Low"
                                     , "Close", "Volume", "Close Time"
                                     , "Quote Asset Volume", "Number of Trades"
                                     , "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume"
                                     , "To_drop"])
    df["Open Time"] = pd.to_datetime(df["Open Time"], unit="ms")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit="ms")
    return df.drop(columns=["To_drop"])

def calculate_ma(df):
    df["SMA20"] = df["Close"].rolling(window=20).mean() # why 20?
    # for general price direction
    return df

def calculate_bollinger_bands(df):
    df["SMA20"] = df["Close"].rolling(window=20).mean()
    df["Upper Band"] = df["SMA20"] + 2 * df["Close"].rolling(window=20).std()
    df["Lower Band"] = df["SMA20"] - 2 * df["Close"].rolling(window=20).std()
    # for volatility
    # price touching upper band = overbought, touching lower band = oversold
    return df

def pull_funding_rate(symbol = "BTCUSDT"):
    url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}"
    response = requests.get(url)
    data = json.loads(response.text)
    # print(data)
    if not data:  # Handle empty response
        return pd.DataFrame(columns=["fundingTime", "fundingRate", "symbol"])
    df = pd.DataFrame(data)
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
    return df

def get_open_interest(symbol = "BTCUSDT", period = "1d"):
    url = f"https://fapi.binance.com//futures/data/openInterestHist?symbol={symbol}&period={period}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    # print(data)
    if not data:  # Handle empty response
        return pd.DataFrame(columns=["symbol", "sumOpenInterest", "sumOpenInterestValue", "timestamp"])
    df = pd.DataFrame(data)
    df["timestamp"]  = pd.to_datetime(df["timestamp"], unit="ms")
    return df

def get_long_short_ratio(symbol = "BTCUSDT", period = "1d"):
    url = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period={period}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    # print(data)
    if not data:  # Handle empty response
        return pd.DataFrame(columns=["symbol", "longShortRatio", "longAccount", "shortAccount", "timestamp"])
    df = pd.DataFrame(data)
    df["timestamp"]  = pd.to_datetime(df["timestamp"], unit="ms")
    return df

# print(btc_klines := pull_price("BTCUSDT", "1h"))
# print(pull_funding_rate("BTCUSDT"))
# print(get_open_interest("BTCUSDT"))
# print(get_long_short_ratio("BTCUSDT"))

def build_price_df(symbol, interval):
    df = pull_price(symbol, interval)
    df = calculate_ma(df)
    df = calculate_bollinger_bands(df)
    return df

test_endpoints()

# get DFs
price_df = build_price_df("BTCUSDT", "1d")
funding_rate_df = pull_funding_rate("BTCUSDT")
open_interest_df = get_open_interest("BTCUSDT")
long_short_ratio_df = get_long_short_ratio("BTCUSDT")

# save as CSV
price_df.to_csv("data/price_data.csv", index=False)
funding_rate_df.to_csv("data/funding_rate_data.csv", index=False)
open_interest_df.to_csv("data/open_interest_data.csv", index=False)
long_short_ratio_df.to_csv("data/long_short_ratio_data.csv", index=False)
