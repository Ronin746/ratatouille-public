
import pandas as pd
import numpy as np


def normalize_series(series):
    """Normalize a pandas series to 0-100 percentile rank."""
    valid_series = series.fillna(series.min()) if series.count() > 0 else series.fillna(0)
    return valid_series.rank(pct=True) * 100


def calculate_scores(results_df, weights):
    """
    Calculate LONG final scores based on the 7 factors.
    High score = strong stock (good for buying).
    """
    df = results_df.copy()

    # 1. Price Performance (25%)
    pp_score = (
        normalize_series(df['3m_return']) +
        normalize_series(df['1m_return']) +
        normalize_series(df['r_squared']) +
        normalize_series(df['slope'])
    ) / 4.0

    # 2. Bullish Candles (15%)
    bc_score = (
        normalize_series(df['bullish_ratio']) +
        normalize_series(df['strong_bullish_count'])
    ) / 2.0

    # 3. MA Alignment (15%)
    ma_score = (
        normalize_series(df['ma_aligned'].astype(int)) +
        normalize_series(df['ma_positive_slopes'].astype(int))
    ) / 2.0

    # 4. Trend Consistency (15%)
    tc_score = (
        normalize_series(df['consistency_score']) +
        normalize_series(df['max_drawdown'])
    ) / 2.0

    # 5. Volatility (10%) — ATR stability (smoother ATR = better)
    vol_score = normalize_series(-df['atr_stability'])

    # 6. Volume (10%)
    v_score = (
        normalize_series(df['up_down_ratio']) +
        normalize_series(df['volume_surge'])
    ) / 2.0

    # 7. Relative Strength (10%)
    rs_score = normalize_series(df['rs_rating'])

    # Weighted Sum
    final_score = (
        pp_score * weights['price_performance'] +
        bc_score * weights['bullish_candles'] +
        ma_score * weights['ma_alignment'] +
        tc_score * weights['trend_consistency'] +
        vol_score * weights['volatility'] +
        v_score * weights['volume'] +
        rs_score * weights['relative_strength']
    )

    df['Final_Score'] = final_score
    df['Score_Price'] = pp_score
    df['Score_Candles'] = bc_score
    df['Score_MA'] = ma_score
    df['Score_Trend'] = tc_score
    df['Score_Vol'] = vol_score
    df['Score_Volume'] = v_score
    df['Score_RS'] = rs_score

    return df.sort_values(by='Final_Score', ascending=False)


def calculate_short_scores(results_df, weights):
    """
    Calculate SHORT scores based on the INVERTED 7 factors.
    High Short_Score = weak stock (good candidate for shorting).

    Inverted logic:
      - Price Performance: WORST returns rank highest
      - Bearish Candles: LOW bullish ratio = more bearish = higher short score
      - MA Misalignment: NOT aligned + negative slopes = higher short score
      - Trend Breakdown: LOW consistency + DEEP drawdown = higher short score
      - Volatility: Same (low stability still good for trading)
      - Volume: LOW up/down ratio (selling pressure) = higher short score
      - Relative Weakness: WORST vs benchmark = higher short score
    """
    df = results_df.copy()

    # 1. Price Weakness (25%) — worst returns = highest score
    pw_score = (
        normalize_series(-df['3m_return']) +
        normalize_series(-df['1m_return']) +
        normalize_series(-df['r_squared']) +
        normalize_series(-df['slope'])
    ) / 4.0

    # 2. Bearish Candles (15%) — low bullish ratio = bearish
    bear_score = (
        normalize_series(-df['bullish_ratio']) +
        normalize_series(-df['strong_bullish_count'])
    ) / 2.0

    # 3. MA Misalignment (15%) — NOT aligned, negative slopes
    ma_short = (
        normalize_series(1 - df['ma_aligned'].astype(int)) +
        normalize_series(1 - df['ma_positive_slopes'].astype(int))
    ) / 2.0

    # 4. Trend Breakdown (15%) — LL/LH structure + deep drawdown
    tb_score = (
        normalize_series(df['ll_lh_score']) +   # more LL/LH pairs = stronger downtrend
        normalize_series(-df['max_drawdown'])   # more negative DD = higher short score
    ) / 2.0

    # 5. Volatility (10%) — ATR stability (smoother ATR = better)
    vol_score = normalize_series(-df['atr_stability'])

    # 6. Selling Pressure (10%) — low up/down ratio = bearish volume
    sell_score = (
        normalize_series(-df['up_down_ratio']) +
        normalize_series(df['volume_surge'])  # high volume on weakness amplifies
    ) / 2.0

    # 7. Relative Weakness (10%) — worst vs benchmark
    rw_score = normalize_series(-df['rs_rating'])

    # Weighted Sum
    short_score = (
        pw_score * weights['price_performance'] +
        bear_score * weights['bullish_candles'] +
        ma_short * weights['ma_alignment'] +
        tb_score * weights['trend_consistency'] +
        vol_score * weights['volatility'] +
        sell_score * weights['volume'] +
        rw_score * weights['relative_strength']
    )

    df['Short_Score'] = short_score
    df['Short_Price'] = pw_score
    df['Short_Candles'] = bear_score
    df['Short_MA'] = ma_short
    df['Short_Trend'] = tb_score
    df['Short_Vol'] = vol_score
    df['Short_Volume'] = sell_score
    df['Short_RS'] = rw_score

    return df.sort_values(by='Short_Score', ascending=False)
