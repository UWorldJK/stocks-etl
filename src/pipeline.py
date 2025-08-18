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


def fetch_prices(tickers, period_days) -> pd.DataFrame:
    """
    Fetch daily adjusted OHLCV data for the given tickers over the specified period.
    Args:
        tickers (list): List of ticker symbols to fetch data for.
        period_days (int): Number of days to look back from today.

    Returns:
        pd.DataFrame: DataFrame containing date, ticker, open, high, low, close, volume.
    """
    # Fetch daily adjusted OHLCV for the last N days
    start = (
        (datetime.now(timezone.utc) - timedelta(days=period_days)).date().isoformat()
    )
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


def init_db(con):
    """
    Initialize the DuckDB database with the necessary tables.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection object.
        that will be used to execute SQL commands. Connects to our .duckdb file in
        the DBConnect file.
    """
    # this first table is used to handle the data grabbed from yfinance
    # and cleaned by fetch_prices
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_prices (
            date DATE,
            ticker VARCHAR,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date DATE,
            ticker VARCHAR,
            return_1d DOUBLE,
            ma_7 DOUBLE, ma_30 DOUBLE,
            vol_7 DOUBLE, vol_30 DOUBLE,
            rsi DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS corr_30d (
            date DATE,
            ticker_a VARCHAR,
            ticker_b VARCHAR,
            corr_30d DOUBLE,
            PRIMARY KEY (date, ticker_a, ticker_b)
        );
    """
    )


def upsert_raw_prices(con, prices):
    """
    Upsert daily prices into the raw_prices table.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection object.
        df (pd.DataFrame): DataFrame containing daily prices to upsert.
    """
    if prices.empty:
        return

    # this makes the price table we obtained from yfinance
    # usable within duckdb SQL commands
    # so this says register prices as a table in duckdb
    con.register("prices_df", prices)
    # this inserts everything from the prices_df table
    # into the raw_prices table, replacing any existing rows
    con.execute(
        """
        INSERT OR REPLACE INTO raw_prices
        SELECT * FROM prices_df;
    """
    )
    output_path = "data/raw_prices_export.csv"
    safe_path = output_path.replace("'", "''")  # escape quotes just in case
    con.execute(f"""
        COPY (
            SELECT *
            FROM raw_prices
            ORDER BY date DESC, ticker
        ) TO '{safe_path}' WITH (HEADER, DELIMITER ',');
    """)
    return len(prices)


def compute_tech(df):
    """
    Compute technical indicators for the given DataFrame.
    Args:
        df (pd.DataFrame): DataFrame containing daily prices with columns:
            date, ticker, open, high, low, close, volume.
            Returns:
        pd.DataFrame: DataFrame with additional columns for technical indicators:
            return_1d, ma_7, ma_30, vol_7, vol
    """
    df = df.copy()
    # The rolling standard deviation measures the variability (or volatility)
    # of a fixed number of consecutive data points in a time series.
    # It quantifies how much the values deviate from their rolling average.
    df["return_1d"] = df.groupby("ticker")["close"].pct_change()
    df["ma_7"] = df.groupby("ticker")["close"].transform(lambda s: s.rolling(7).mean())
    df["ma_30"] = df.groupby("ticker")["close"].transform(
        lambda s: s.rolling(30).mean()
    )
    df["vol_7"] = df.groupby("ticker")["return_1d"].transform(
        lambda s: s.rolling(7).std()
    )
    df["vol_30"] = df.groupby("ticker")["return_1d"].transform(
        lambda s: s.rolling(30).std()
    )

    # Calulcate RSI: Relative Strength Index
    def compute_rsi(series, n=14):
        # has a default value of 1 and sees difference with previous row
        delta = series.diff()
        # .where is used to replace negative values with 0
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()

        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    df["rsi"] = (
        df.groupby("ticker")["close"]
        .apply(lambda group: compute_rsi(group, 14))
        .reset_index(level=0, drop=True)  # Align the index with the original DataFrame
    )
    return df


def upsert_metrics(con, metrics):
    """
    Upsert daily metrics into the daily_metrics table.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection object.
        df (pd.DataFrame): DataFrame containing daily metrics to upsert.
    """
    if metrics.empty:
        return 0

    # Register the DataFrame as a DuckDB table
    cols = ["date", "ticker", "return_1d", "ma_7", "ma_30", "vol_7", "vol_30", "rsi"]
    con.register("metrics_df", metrics[cols])
    con.execute("INSERT OR REPLACE INTO daily_metrics SELECT * FROM metrics_df;")

    con.execute(f"""
        COPY (
            SELECT *
            FROM daily_metrics
            WHERE date >= current_date - INTERVAL 120 DAY
            ORDER BY date DESC, ticker
        ) TO '{EXPORT_CSV}' WITH (HEADER, DELIMITER ',');
    """)

    return len(metrics)


# Add this to the end of your existing pipeline.py file

def generate_charts():
    """Generate charts after the ETL pipeline completes."""
    try:
        # Import here to avoid dependency issues if matplotlib not available
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        from chart_generator import generate_charts_from_csv
        
        # Create charts directory
        os.makedirs("data/charts", exist_ok=True)
        
        # Generate charts from the CSV we just created
        if os.path.exists(EXPORT_CSV):
            print("📈 Generating visualization charts...")
            chart_paths = generate_charts_from_csv(EXPORT_CSV, "data/charts")
            
            print(f"✅ Generated {len(chart_paths)} charts:")
            for chart_path in chart_paths:
                print(f"   - {chart_path}")
            
            return chart_paths
        else:
            print(f"❌ CSV file not found: {EXPORT_CSV}")
            return []
            
    except ImportError as e:
        print(f"⚠️  Chart generation skipped - missing dependencies: {e}")
        return []
    except Exception as e:
        print(f"❌ Chart generation failed: {e}")
        return []

# Modify your main function to include chart generation
def main():
    print('getting prices...')
    prices = fetch_prices(TICKERS, LOOKBACK_DAYS)
    print(prices.head)
    print('fetching prices complete, computing metrics...')
    metrics = compute_tech(prices)
    con = duckdb.connect(DB_PATH)
    init_db(con)
    print('upserting metrics...')
    upsert_metrics(con, metrics)
    print('upserting raw prices...')
    upsert_raw_prices(con, prices)
    
    # Generate charts after data processing
    chart_paths = generate_charts()
    
    print(f"\n📋 ETL Pipeline Summary:")
    print(f"   Tickers processed: {', '.join(TICKERS)}")
    print(f"   Data exported to: {EXPORT_CSV}")
    print(f"   Charts generated: {len(chart_paths)}")
    print("🎉 ETL Pipeline completed successfully!")

if __name__== "__main__":
    print("Starting ETL pipeline...")
    main()