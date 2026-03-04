
# 7-Factor Model Weights
WEIGHTS = {
    "price_performance": 0.25,
    "bullish_candles": 0.15,
    "ma_alignment": 0.15,
    "trend_consistency": 0.15,
    "volatility": 0.10,
    "volume": 0.10,
    "relative_strength": 0.10
}

# Benchmark
BENCHMARK_TICKER = "SPY"


def get_market_tickers():
    """
    Returns the full US ticker universe fetched from GitHub.
    The scanning universe is independent of the basket definitions —
    baskets are used only for sector attribution after scoring.
    """
    from ticker_universe import get_all_tickers
    return get_all_tickers()
