
import pandas as pd
import numpy as np
from scipy.stats import linregress
from config import cfg


# ── helpers ──────────────────────────────────────────────────────────────────

def _valid_close(df: pd.DataFrame, min_rows: int = 2) -> pd.Series | None:
    """Return the Close series only if it has enough non-NaN values."""
    close = df.get("Close")
    if close is None:
        return None
    clean = close.dropna()
    return clean if len(clean) >= min_rows else None


def _ohlc_ok(df: pd.DataFrame, min_rows: int = 10) -> bool:
    """Return True if High and Low columns have at least min_rows valid rows."""
    for col in ("High", "Low"):
        if col not in df.columns or df[col].notna().sum() < min_rows:
            return False
    return True


def _volume_ok(df: pd.DataFrame, min_rows: int = 5) -> bool:
    """Return True if Volume column has at least min_rows valid rows."""
    return "Volume" in df.columns and df["Volume"].notna().sum() >= min_rows


# ── factor calculations ───────────────────────────────────────────────────────

def calc_price_performance(df, months=cfg.lookback_months):
    """
    Factor 1: Price Performance (25%)
    - 3M/1M/1W/3D returns
    - Linear regression R² and slope on Close
    """
    days = int(months * 21)
    if len(df) < days:
        return None

    close = df["Close"].ffill()          # forward-fill to bridge small gaps
    period_close = close.iloc[-days:]

    # Returns — use .dropna() endpoints to avoid NaN propagation
    clean = period_close.dropna()
    if len(clean) < 2:
        return None

    # last_close: absolute last available daily close in the full dataset
    last_close  = close.dropna().iloc[-1]
    start_price = clean.iloc[0]
    ret_3m = (last_close - start_price) / start_price if start_price != 0 else 0.0

    def _ret(n):
        seg = close.iloc[-n:].dropna()
        if len(seg) < 2:
            return 0.0
        return (seg.iloc[-1] - seg.iloc[0]) / seg.iloc[0] if seg.iloc[0] != 0 else 0.0

    ret_1m = _ret(21)
    ret_1w = _ret(5)
    ret_3d = _ret(3)
    ret_1d = _ret(2)

    # 21 EMA at the last available daily close
    ema21_series = close.ewm(span=21, adjust=False).mean()
    ema21        = float(ema21_series.dropna().iloc[-1])
    ema21_dist   = float((last_close - ema21) / ema21) if ema21 != 0 else 0.0

    # Linear regression (Full period)
    y = clean.values
    x = np.arange(len(y))
    slope, _, r_value, _, _ = linregress(x, y)
    r_squared = r_value ** 2

    # Linear regression (21 days)
    y_21 = clean.iloc[-21:].values if len(clean) >= 21 else clean.values
    x_21 = np.arange(len(y_21))
    _, _, r_value_21, _, _ = linregress(x_21, y_21)
    r_squared_21d = r_value_21 ** 2

    return {
        "3m_return":  ret_3m,
        "1m_return":  ret_1m,
        "1w_return":  ret_1w,
        "3d_return":  ret_3d,
        "1d_return":  ret_1d,
        "last_price": float(last_close),
        "ema21":      ema21,
        "ema21_dist": ema21_dist,
        "r_squared":  r_squared,
        "r_squared_21d": r_squared_21d,
        "slope":      slope,
    }


def calc_bullish_candles(df, months=cfg.lookback_months):
    """
    Factor 2: Bullish Candles (15%)
    - Ratio of green candles (Close > Open)
    - Strong bullish day count (>1% intraday gain)
    """
    days = int(months * 21)
    if len(df) < days:
        return None

    period_df = df.iloc[-days:].copy()
    period_df["Close"] = period_df["Close"].ffill()
    period_df["Open"]  = period_df["Open"].ffill()

    valid = period_df.dropna(subset=["Close", "Open"])
    if valid.empty:
        return {"bullish_ratio": 0.0, "strong_bullish_count": 0}

    green = valid[valid["Close"] > valid["Open"]]
    ratio = len(green) / len(valid)

    strong_bullish = valid[(valid["Close"] - valid["Open"]) / valid["Open"] > 0.01]
    strong_count = len(strong_bullish)

    return {
        "bullish_ratio": ratio,
        "strong_bullish_count": strong_count,
    }


def calc_ma_alignment(df):
    """
    Factor 3: MA Alignment (15%)
    - 10 > 20 > 50 alignment
    - Positive slopes on all three MAs
    """
    if len(df) < cfg.sma_slow:
        return None

    close = df["Close"].ffill()
    if close.dropna().shape[0] < cfg.sma_slow:
        return None

    ma10 = close.rolling(window=cfg.sma_fast).mean()
    ma20 = close.rolling(window=cfg.sma_medium).mean()
    ma30 = close.rolling(window=cfg.sma_trend).mean()
    ma50 = close.rolling(window=cfg.sma_slow).mean()

    current_ma10 = ma10.iloc[-1]
    current_ma20 = ma20.iloc[-1]
    current_ma30 = ma30.iloc[-1]
    current_ma50 = ma50.iloc[-1]

    if pd.isna(current_ma10) or pd.isna(current_ma20) or pd.isna(current_ma50):
        return None

    aligned = (current_ma10 > current_ma20) and (current_ma20 > current_ma50)

    slope10 = current_ma10 > ma10.iloc[-6]
    slope20 = current_ma20 > ma20.iloc[-6]
    slope50 = current_ma50 > ma50.iloc[-6]

    return {
        "aligned": aligned,
        "positive_slopes": (slope10 and slope20 and slope50),
        "ma10": current_ma10,
        "ma20": current_ma20,
        "ma30": float(current_ma30) if not pd.isna(current_ma30) else 0.0,
        "ma50": current_ma50,
    }


def calc_trend_consistency(df, months=cfg.lookback_months):
    """
    Factor 4: Trend Consistency (15%)
    - Higher Highs / Higher Lows ratio (trend structure)
    - Max drawdown from peak (pullback depth)

    HH/HL logic: split the lookback period into 4 consecutive chunks and
    count how many consecutive chunk-pairs show both a higher high AND a
    higher low.  Score = ratio of qualifying pairs (0.0 – 1.0).
    Fallback to % days above MA50 when High/Low data is unavailable.
    """
    days = int(months * 21)
    if len(df) < days:
        return None

    period_df = df.iloc[-days:].copy()
    period_df["Close"] = period_df["Close"].ffill()

    clean = period_df["Close"].dropna()
    if len(clean) < 10:
        return None

    # Max drawdown (pullback depth)
    rolling_max = clean.cummax()
    drawdown = (clean - rolling_max) / rolling_max
    max_drawdown = drawdown.min()

    # Higher Highs / Higher Lows — market days and chunk size driven from config
    hhhl_days  = cfg.hhhl_lookback_days
    chunk_size = cfg.hhhl_chunk_size
    n_chunks   = hhhl_days // chunk_size  # e.g., 45 / 5 = 9
    hhhl_df    = df.iloc[-hhhl_days:].copy()

    if chunk_size >= 1 and _ohlc_ok(hhhl_df, min_rows=chunk_size):
        hhhl_df["High"] = hhhl_df["High"].ffill()
        hhhl_df["Low"]  = hhhl_df["Low"].ffill()

        chunk_highs, chunk_lows = [], []
        for i in range(n_chunks):
            chunk = hhhl_df.iloc[i * chunk_size: (i + 1) * chunk_size]
            h = chunk["High"].dropna()
            l = chunk["Low"].dropna()
            if not h.empty and not l.empty:
                chunk_highs.append(float(h.max()))
                chunk_lows.append(float(l.min()))

        pairs = min(len(chunk_highs), len(chunk_lows)) - 1
        hhhl_count = sum(
            1 for i in range(pairs)
            if chunk_highs[i + 1] > chunk_highs[i] and chunk_lows[i + 1] > chunk_lows[i]
        )
        lllh_count = sum(
            1 for i in range(pairs)
            if chunk_highs[i + 1] < chunk_highs[i] and chunk_lows[i + 1] < chunk_lows[i]
        )
        consistency_score = float(hhhl_count / (n_chunks - 1)) if n_chunks > 1 else 0.0
        ll_lh_score      = float(lllh_count / (n_chunks - 1)) if n_chunks > 1 else 0.0
    else:
        # Fallback: % of days above MA50 when High/Low data is unavailable
        ma50 = period_df["Close"].rolling(window=cfg.sma_slow).mean()
        days_above = (period_df["Close"] > ma50).sum()
        consistency_score = float(days_above / len(period_df))
        ll_lh_score       = 1.0 - consistency_score  # mirror fallback for short


    return {
        "max_drawdown":    float(max_drawdown),
        "consistency_score": float(consistency_score),
        "ll_lh_score":     float(ll_lh_score),
    }


def calc_volatility(df, atr_length=cfg.atr_length, adr_length=cfg.adr_length):
    """
    Factor 5: Volatility (10%)
    - ATR stability (lower = steadier trend)
    - ADR% average daily range as % of close  (adr_length bars, default 20)
    - ATR% average true range as % of close   (atr_length bars, default 14)

    Guard: if High/Low data is missing or all-NaN (yfinance data quality issue),
    returns neutral defaults instead of propagating NaN.
    """
    min_bars = max(atr_length, adr_length) + 1
    if len(df) < min_bars:
        return None

    close = df["Close"].ffill()

    # ── Check OHLC data quality ──────────────────────────────────────────────
    if not _ohlc_ok(df, min_rows=max(atr_length, adr_length)):
        # High/Low unavailable — return neutral/fallback values
        # atr_stability = 1.0 (worst), adr_pct = atr_pct = 0.0
        return {
            "atr": 0.0,
            "atr_stability": 1.0,
            "adr_pct": 0.0,
            "atr_pct": 0.0,
        }

    high = df["High"]
    low  = df["Low"]
    prev_close = close.shift(1)

    # True Range
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    atr = tr.rolling(window=atr_length).mean()

    # ATR Stability: coefficient of variation over last 20 bars
    recent_atr = atr.iloc[-20:].dropna()
    if len(recent_atr) >= 2 and recent_atr.mean() != 0:
        stability = float(recent_atr.std() / recent_atr.mean())
    else:
        stability = 1.0

    # ADR% — average (High-Low)/Close over last `adr_length` days (default 20)
    # Use .dropna().iloc[-1] so a trailing NaN bar (partial-day yfinance row) is skipped.
    daily_range = high - low
    adr_series = (daily_range / close).rolling(window=adr_length).mean()
    adr_valid = adr_series.dropna()
    current_adr_pct = float(adr_valid.iloc[-1]) if not adr_valid.empty else 0.0

    # ATR% — ATR(atr_length) / last_close
    # Use .dropna().iloc[-1] for both ATR and Close to avoid NaN from a partial bar.
    atr_valid  = atr.dropna()
    close_valid = close.dropna()
    if not atr_valid.empty and not close_valid.empty and close_valid.iloc[-1] != 0:
        last_atr   = atr_valid.iloc[-1]
        last_close = close_valid.iloc[-1]
        current_atr_pct = float(last_atr / last_close)
    else:
        last_atr   = float('nan')
        last_close = float('nan')
        current_atr_pct = 0.0

    return {
        "atr": float(last_atr) if not pd.isna(last_atr) else 0.0,
        "atr_stability": stability,
        "adr_pct": current_adr_pct,
        "atr_pct": current_atr_pct,
    }


def calc_volume(df, length=cfg.volume_ma_length):
    """
    Factor 6: Volume (10%)
    - Up/down volume ratio
    - Recent volume surge vs rolling average

    Guard: if Volume is missing/NaN, returns neutral defaults.
    """
    if len(df) < length:
        return None

    # ── Check Volume data quality ────────────────────────────────────────────
    if not _volume_ok(df, min_rows=length):
        return {
            "up_down_ratio": 1.0,   # neutral
            "volume_surge": 1.0,    # neutral
        }

    period_df = df.iloc[-length:].copy()
    period_df["Close"]  = period_df["Close"].ffill()
    period_df["Open"]   = period_df["Open"].ffill()
    period_df["Volume"] = period_df["Volume"].fillna(0)

    up_vol   = period_df[period_df["Close"] > period_df["Open"]]["Volume"].sum()
    down_vol = period_df[period_df["Close"] < period_df["Open"]]["Volume"].sum()
    up_down_ratio = float(up_vol / down_vol) if down_vol != 0 else float(up_vol) if up_vol != 0 else 1.0

    # Use NaN-aware logic: treat Volume=0 on the last bar as a partial/missing bar.
    # Replace 0 with NaN before computing surge so the last real volume bar is used.
    vol_series = df["Volume"].replace(0, float("nan"))
    avg_vol_series = vol_series.rolling(window=length, min_periods=length // 2).mean()
    avg_vol     = avg_vol_series.dropna().iloc[-1] if not avg_vol_series.dropna().empty else None
    current_vol = vol_series.dropna().iloc[-1] if not vol_series.dropna().empty else 0
    surge = float(current_vol / avg_vol) if avg_vol and avg_vol != 0 else 1.0

    return {
        "up_down_ratio": up_down_ratio,
        "volume_surge": surge,
    }


def calc_relative_strength(df, benchmark_df, months=cfg.lookback_months):
    """
    Factor 7: Relative Strength (10%)
    - Return of the RS line (stock / benchmark) over the period
    """
    days = int(months * 21)
    if len(df) < days or len(benchmark_df) < days:
        return None

    stock_close = df["Close"].ffill().iloc[-days:]
    bench_close = benchmark_df["Close"].ffill().reindex(stock_close.index).ffill()

    # Drop rows where either side is NaN
    valid = pd.concat([stock_close, bench_close], axis=1).dropna()
    valid.columns = ["stock", "bench"]

    if len(valid) < 2:
        return {"rs_rating": 0.0}

    rs_line = valid["stock"] / valid["bench"]
    rs_ret = (rs_line.iloc[-1] - rs_line.iloc[0]) / rs_line.iloc[0] if rs_line.iloc[0] != 0 else 0.0

    return {"rs_rating": float(rs_ret)}
