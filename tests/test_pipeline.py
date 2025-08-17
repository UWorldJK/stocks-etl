import pytest 
from pipeline import fetch_prices
TICKERS = os.getenv("TICKERS", "AAPL,MSFT,TSLA,SPY,QQQ").split(",")
DB_PATH = "data/market.duckdb"
EXPORT_CSV = "data/daily_metrics.csv"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "400"))  # historical backfill
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))


assert fetch_prices(TICKERS, LOOKBACK_DAYS) 
