from dataclasses import dataclass, field
from typing import Dict

@dataclass
class ScreenerConfig:
    # General
    benchmark_ticker: str = "SPY"
    mcap_min: float = 1_000_000_000.0

    # 7-Factor Model Weights
    weights: Dict[str, float] = field(default_factory=lambda: {
        "price_performance": 0.25,
        "bullish_candles": 0.15,
        "ma_alignment": 0.15,
        "trend_consistency": 0.15,
        "volatility": 0.10,
        "volume": 0.10,
        "relative_strength": 0.10
    })

    # Timeline Thresholds
    lookback_months: int = 3
    atr_length: int = 14
    adr_length: int = 20
    volume_ma_length: int = 20
    volume_surge_ma_length: int = 50

    # Moving Average Profiles
    sma_fast: int = 10
    sma_medium: int = 20
    sma_trend: int = 30
    sma_slow: int = 50
    sma_200: int = 200
    ema_fast: int = 21

    # Trend consistency logic
    hhhl_lookback_days: int = 45
    hhhl_chunk_size: int = 5

cfg = ScreenerConfig()

# Backwards compatibility exports for unmodified files
WEIGHTS = cfg.weights
BENCHMARK_TICKER = cfg.benchmark_ticker

def get_market_tickers():
    """
    Returns the full US ticker universe fetched from GitHub.
    The scanning universe is independent of the basket definitions —
    baskets are used only for sector attribution after scoring.
    """
    from ticker_universe import get_all_tickers
    return get_all_tickers()
