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
            sub = df[t.strip()].reset_index().rename(columns=str.lower)
            sub["ticker"] = t.strip()
            frames.append(sub)
    else:
        # Single ticker returns flat columns
        sub = df.reset_index().rename(columns=str.lower)
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

def compute_tech(df):
    # df: date,ticker,open,high,low,close,volume
    df = df.copy()
    df["return_1d"] = df.groupby("ticker")["close"].pct_change()
    df["ma_7"]  = df.groupby("ticker")["close"].transform(lambda s: s.rolling(7).mean())
    df["ma_30"] = df.groupby("ticker")["close"].transform(lambda s: s.rolling(30).mean())
    df["vol_7"]  = df.groupby("ticker")["return_1d"].transform(lambda s: s.rolling(7).std())
    df["vol_30"] = df.groupby("ticker")["return_1d"].transform(lambda s: s.rolling(30).std())
    # RSI (Wilder’s)
    def rsi(series, n=14):
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/n, min_periods=n, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/n, min_periods=n, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        return 100 - (100 / (1 + rs))
    df["rsi"] = df.groupby("ticker")["close"].transform(lambda s: rsi(s, RSI_PERIOD))
    return df

def compute_corr(df):
    # 30-day rolling correlation of daily returns between all ticker pairs; long format
    returns = df.pivot(index="date", columns="ticker", values="return_1d").dropna(how="all")
    corr_rows = []
    for end_idx in range(len(returns)):
        window = returns.iloc[max(0, end_idx-29):end_idx+1]  # 30 rows incl end
        if len(window) < 15:  # need enough points
            continue
        c = window.corr(min_periods=10)
        d = window.index[-1]
        for a, b in itertools.combinations(sorted(window.columns), 2):
            val = c.loc[a, b]
            if pd.notna(val):
                corr_rows.append({"date": d, "ticker_a": a, "ticker_b": b, "corr_30d": float(val)})
    if not corr_rows:
        return pd.DataFrame(columns=["date","ticker_a","ticker_b","corr_30d"])
    corr_df = pd.DataFrame(corr_rows)
    corr_df["date"] = pd.to_datetime(corr_df["date"]).dt.date
    return corr_df

def init_db(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_prices (
            date DATE,
            ticker VARCHAR,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date DATE,
            ticker VARCHAR,
            return_1d DOUBLE,
            ma_7 DOUBLE, ma_30 DOUBLE,
            vol_7 DOUBLE, vol_30 DOUBLE,
            rsi DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS corr_30d (
            date DATE,
            ticker_a VARCHAR,
            ticker_b VARCHAR,
            corr_30d DOUBLE,
            PRIMARY KEY (date, ticker_a, ticker_b)
        );
    """)

def upsert_prices(con, prices):
    if prices.empty:
        return 0
    con.register("prices_df", prices)
    con.execute("""
        INSERT OR REPLACE INTO raw_prices
        SELECT * FROM prices_df;
    """)
    return len(prices)

def upsert_metrics(con, metrics):
    if metrics.empty:
        return 0
    cols = ["date","ticker","return_1d","ma_7","ma_30","vol_7","vol_30","rsi"]
    con.register("metrics_df", metrics[cols])
    con.execute("INSERT OR REPLACE INTO daily_metrics SELECT * FROM metrics_df;")
    # Export a friendly CSV snapshot for the last 120 days
    con.execute("""
        COPY (
            SELECT * FROM daily_metrics
            WHERE date >= current_date - INTERVAL 120 DAY
            ORDER BY date DESC, ticker
        ) TO ? WITH (HEADER, DELIMITER ',');
    """, [EXPORT_CSV])
    return len(metrics)

def upsert_corr(con, corr):
    if corr.empty:
        return 0
    con.register("corr_df", corr)
    con.execute("INSERT OR REPLACE INTO corr_30d SELECT * FROM corr_df;")
    return len(corr)

def main():
    prices = fetch_prices(TICKERS, LOOKBACK_DAYS)
    if prices.empty:
        print("No data fetched.")
        return
    metrics = compute_tech(prices)
    corr = compute_corr(metrics)

    con = duckdb.connect(DB_PATH)
    init_db(con)
    n_prices = upsert_prices(con, prices)
    n_metrics = upsert_metrics(con, metrics)
    n_corr = upsert_corr(con, corr)
    con.close()

    print(f"Fetched {len(prices)} price rows; upserted metrics {n_metrics}; correlations {n_corr}.")

if __name__ == "__main__":
    main()
