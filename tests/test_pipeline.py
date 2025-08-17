import os
import pytest
from src.pipeline import fetch_prices

def test_fetch_prices_returns_data():
    tickers = os.getenv("TICKERS", "AAPL,MSFT,TSLA").split(",")
    lookback = int(os.getenv("LOOKBACK_DAYS", "7"))
    df = fetch_prices(tickers, lookback)
    assert df is not None
    assert len(df) > 0
    