import os
import io
import itertools
import duckdb
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone

# ---- Config from environment ----
TICKERS = os.getenv("TICKERS", "AAPL,MSFT,TSLA,SPY,QQQ").split(",")
DB_PATH = "data/market.duckdb"
EXPORT_CSV = "data/daily_metrics.csv"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "400"))  # historical backfill
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

os.makedirs("data", exist_ok=True)

def fetch_prices(tickers, period_days):
    # Fetch daily adjusted OHLCV for the last N days
    start = (datetime.now(timezone.utc) - timedelta(days=period_days)).date().isoformat()
    # .strip() to handle any extra spaces in ticker list
    df = yf.download(
        tickers=[t.strip() for t in tickers],
        start=start,
        interval="1d",
        group_by="ticker",
        threads=True,
        progress=False,
    )
    # Normalize to long format: date, ticker, open, high, low, close, volume
    frames = []
    if isinstance(df.columns, pd.MultiIndex):
        for t in tickers:
            if t.strip() not in df.columns.levels[0]:
                continue
            sub = df[t.strip()].reset_index()
            sub = sub.rename(columns=lambda x: x.lower())
            sub["ticker"] = t.strip()
            frames.append(sub)
    else:
        # Single ticker returns flat columns
        sub = df.reset_index()
        sub = sub.rename(columns=lambda x: x.lower())
        sub["ticker"] = tickers[0].strip()
        frames.append(sub)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"index": "date"})
    out = out.dropna(subset=["close"])
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize("UTC").dt.date
    cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
    return out[cols].sort_values(["ticker", "date"])


