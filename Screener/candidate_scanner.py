"""
candidate_scanner.py — Daily EMA21/SMA30 Cross + Weekly Full Alignment + Daily VCP
====================================================================================
LONG  : Weekly full bull alignment (Price > SMA10w > SMA20w > SMA30w > SMA50w > SMA200w)
        + EMA21 daily crossed above SMA30 daily (≤15 sessions ago)
        + Daily SMA alignment scoring
        + Daily VCP scoring

SHORT : Weekly full bear alignment (mirror)
        + EMA21 daily crossed below SMA30 daily (≤15 sessions ago)
        + Daily bearish SMA alignment scoring
        + Daily distribution/breakdown VCP scoring

Score formula — each component raw 0–3 pts:
  SCORE   = align × 0.25 + cross × 0.35 + vcp × 0.40
  SCORE10 = (SCORE / 9) × 10   →  max = 10.0   threshold = 6.5

Categories:
  Long:  8.5–10 🟢 TOP CANDIDATE | 6.5–8.4 🟡 GOOD | <6.5 ❌ EXCLUDED
  Short: 8.5–10 🔴 TOP SHORT      | 6.5–8.4 🟠 GOOD SHORT | <6.5 ❌ EXCLUDED

Output columns:
  Ticker | Score/10 | 7-Factor | Price | Chg 1D% | Chg 1W% | Chg 1M% |
  ATR | Dist 21EMA% | Dist ATR 50SMA | Setup Notes
"""

import logging
import pytz
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

from config import cfg

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  DATA FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_weekly(ticker: str) -> pd.DataFrame | None:
    """
    Fetch 5 years of weekly OHLCV (need ≥200 bars for SMA200w).
    Handles yfinance 1.x tz-aware index and MultiIndex columns.
    Returns tz-naive DataFrame or None.
    """
    try:
        df = yf.download(
            ticker,
            period="5y",
            interval="1wk",
            progress=False,
            auto_adjust=True,
            repair=False,
            threads=False,
        )
        if df is None or df.empty:
            return None

        # Flatten MultiIndex if present
        if isinstance(df.columns, pd.MultiIndex):
            price_cols = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
            l0 = set(df.columns.get_level_values(0))
            if l0 <= price_cols:
                try:
                    df = df.xs(ticker, axis=1, level=1)
                except KeyError:
                    df = df.xs(ticker.upper(), axis=1, level=1)
            else:
                if ticker in df.columns.get_level_values(0):
                    df = df[ticker]
                else:
                    return None

        # Normalise Close
        if "Adj Close" in df.columns and "Close" not in df.columns:
            df = df.rename(columns={"Adj Close": "Close"})
        elif "Adj Close" in df.columns:
            df = df.drop(columns=["Adj Close"])

        # Strip timezone
        if df.index.tz is not None:
            df.index = (
                df.index.tz_convert(pytz.timezone("America/New_York"))
                         .tz_localize(None)
            )
        df.index = df.index.normalize()

        # Drop incomplete current week
        df = df[df.index < pd.Timestamp(datetime.now().date())]

        # Need 200+ bars for SMA200w; require a small buffer
        return df if len(df) >= 205 else None

    except Exception as e:
        logger.debug("[_fetch_weekly] %s: %s", ticker, e)
        return None


def _fetch_daily(ticker: str) -> pd.DataFrame | None:
    """
    Fetch ~6 months of daily OHLCV plus key indicators for cross detection and VCP.
    Computes EMA21, SMA10, SMA20, SMA30, SMA50, Vol50 on the full history.
    Returns last ~130 trading days (tz-naive) or None.
    """
    try:
        df = yf.download(
            ticker,
            period="1y",          # fetch 1y so rolling indicators are warm
            interval="1d",
            progress=False,
            auto_adjust=True,
            repair=False,
            threads=False,
        )
        if df is None or df.empty:
            return None

        # Flatten MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            price_cols = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
            l0 = set(df.columns.get_level_values(0))
            if l0 <= price_cols:
                try:
                    df = df.xs(ticker, axis=1, level=1)
                except KeyError:
                    df = df.xs(ticker.upper(), axis=1, level=1)
            else:
                if ticker in df.columns.get_level_values(0):
                    df = df[ticker]
                else:
                    return None

        # Normalise Close
        if "Adj Close" in df.columns and "Close" not in df.columns:
            df = df.rename(columns={"Adj Close": "Close"})
        elif "Adj Close" in df.columns:
            df = df.drop(columns=["Adj Close"])

        # Strip timezone
        if df.index.tz is not None:
            df.index = (
                df.index.tz_convert(pytz.timezone("America/New_York"))
                         .tz_localize(None)
            )
        df.index = df.index.normalize()

        # Compute indicators over full history (warm the rolling windows)
        close = df["Close"]
        df["EMA21"] = close.ewm(span=cfg.ema_fast, adjust=False).mean()
        df["SMA10"]  = close.rolling(cfg.sma_fast).mean()
        df["SMA20"]  = close.rolling(cfg.sma_medium).mean()
        df["SMA30"]  = close.rolling(cfg.sma_trend).mean()
        df["SMA50"]  = close.rolling(cfg.sma_slow).mean()
        df["Vol50"]  = df["Volume"].rolling(cfg.volume_surge_ma_length).mean()

        # Return last ~130 sessions (enough for 60-day VCP + 15-day cross window)
        return df.iloc[-130:].copy() if len(df) >= 130 else df.copy()

    except Exception as e:
        logger.debug("[_fetch_daily] %s: %s", ticker, e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  WEEKLY PRE-FILTER
# ─────────────────────────────────────────────────────────────────────────────

def _check_weekly_alignment(weekly_df: pd.DataFrame, side: str = "long"):
    """
    Returns (passes: bool, label: str).

    Long:  Price > SMA10w > SMA20w > SMA30w > SMA50w > SMA200w  (ALL 5 aligned bullishly)
    Short: Price < SMA10w < SMA20w < SMA30w < SMA50w < SMA200w  (ALL 5 aligned bearishly)
    """
    close = weekly_df["Close"].dropna()
    if len(close) < 205:
        return False, "Wk insufficient data"

    sma10  = close.rolling(cfg.sma_fast).mean()
    sma20  = close.rolling(cfg.sma_medium).mean()
    sma30  = close.rolling(cfg.sma_trend).mean()
    sma50  = close.rolling(cfg.sma_slow).mean()
    sma200 = close.rolling(cfg.sma_200).mean()

    p    = close.iloc[-1]
    s10  = sma10.iloc[-1]
    s20  = sma20.iloc[-1]
    s30  = sma30.iloc[-1]
    s50  = sma50.iloc[-1]
    s200 = sma200.iloc[-1]

    if any(pd.isna(v) for v in [p, s10, s20, s30, s50, s200]):
        return False, "Wk NaN"

    if side == "long":
        passes = bool(p > s10 > s20 > s30 > s50 > s200)
        label  = "Wk full align" if passes else "Wk no align"
    else:
        passes = bool(p < s10 < s20 < s30 < s50 < s200)
        label  = "Wk full bear align" if passes else "Wk no align"

    return passes, label


# ─────────────────────────────────────────────────────────────────────────────
#  EMA21 / SMA30 CROSS DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _find_cross(daily_hist: pd.DataFrame, side: str = "long"):
    """
    Detect the most recent EMA21/SMA30 cross in daily_hist.

    Returns (cross_days_ago: int | None, fresh_cross: bool, currently_crossed: bool).
      fresh_cross     = True if cross happened ≤3 sessions ago
      currently_crossed = True if EMA21 is on the correct side of SMA30 right now
    """
    df = daily_hist.dropna(subset=["EMA21", "SMA30"])
    if len(df) < 5:
        return None, False, False

    ema = df["EMA21"].values
    sma = df["SMA30"].values
    n   = len(df)

    if side == "long":
        currently_crossed = bool(ema[-1] > sma[-1])
        if not currently_crossed:
            return None, False, False
        # Find transitions: was below, now above
        cross_indices = [
            i for i in range(1, n)
            if ema[i] > sma[i] and ema[i - 1] <= sma[i - 1]
        ]
    else:
        currently_crossed = bool(ema[-1] < sma[-1])
        if not currently_crossed:
            return None, False, False
        # Find transitions: was above, now below
        cross_indices = [
            i for i in range(1, n)
            if ema[i] < sma[i] and ema[i - 1] >= sma[i - 1]
        ]

    if not cross_indices:
        # EMA has been on the correct side throughout the entire window
        # Cross happened before our lookback — treat as "old"
        return 20, False, True

    last_cross_pos  = cross_indices[-1]
    cross_days_ago  = n - 1 - last_cross_pos
    fresh_cross     = cross_days_ago <= 3

    return cross_days_ago, fresh_cross, True


def _vol_at_cross(daily_hist: pd.DataFrame, cross_days_ago: int) -> bool:
    """
    Returns True if volume on the cross day OR the day after was > 50-period avg.
    """
    if cross_days_ago is None or cross_days_ago > len(daily_hist) - 1:
        return False

    df = daily_hist.dropna(subset=["Volume", "Vol50"])
    if len(df) < cross_days_ago + 1:
        return False

    # cross day
    idx = -(cross_days_ago + 1)
    try:
        vol  = float(df["Volume"].iloc[idx])
        avg  = float(df["Vol50"].iloc[idx])
        if avg > 0 and vol > avg:
            return True
        # day after cross
        if cross_days_ago > 0 and abs(idx + 1) <= len(df):
            vol2 = float(df["Volume"].iloc[idx + 1])
            avg2 = float(df["Vol50"].iloc[idx + 1])
            return avg2 > 0 and vol2 > avg2
    except (IndexError, ValueError):
        pass

    return False


# ─────────────────────────────────────────────────────────────────────────────
#  SCORING — SMA ALIGNMENT (DAILY)
# ─────────────────────────────────────────────────────────────────────────────

def _score_sma_align(daily_hist: pd.DataFrame, side: str = "long"):
    """
    Returns (pts: int, sustained: bool).

    Long:
      Full (3 pt):    Price > SMA10d > EMA21d > SMA30d > SMA50d, all positive slopes
      Partial (2 pt): Price > EMA21d > SMA30d (cross has happened)
      Else:           0 pt  →  caller treats as EXCLUDE

    Short (mirror):
      Full (3 pt):    Price < SMA10d < EMA21d < SMA30d < SMA50d, all negative slopes
      Partial (2 pt): Price < EMA21d < SMA30d
      Else:           0 pt  →  EXCLUDE

    sustained = True if full alignment has held for ≥10 consecutive bars.
    """
    req = ["Close", "EMA21", "SMA30", "SMA50", "SMA10"]
    df  = daily_hist.dropna(subset=req)
    if len(df) < 11:
        return 0, False

    p    = float(df["Close"].iloc[-1])
    e21  = float(df["EMA21"].iloc[-1])
    s30  = float(df["SMA30"].iloc[-1])
    s50  = float(df["SMA50"].iloc[-1])
    s10  = float(df["SMA10"].iloc[-1])

    if p <= 0:
        return 0, False

    # Slope helper: positive if last bar > bar 6 sessions ago
    def pos_slope(col: str) -> bool:
        s = df[col]
        return bool(s.iloc[-1] > s.iloc[-6]) if len(s) >= 6 else False

    def neg_slope(col: str) -> bool:
        s = df[col]
        return bool(s.iloc[-1] < s.iloc[-6]) if len(s) >= 6 else False

    def check_sustained_long(df_sub: pd.DataFrame) -> bool:
        tail = df_sub.tail(12)
        if len(tail) < 11:
            return False
        return all(
            float(tail["Close"].iloc[i]) > float(tail["SMA10"].iloc[i])
            > float(tail["EMA21"].iloc[i]) > float(tail["SMA30"].iloc[i])
            > float(tail["SMA50"].iloc[i])
            for i in range(-10, 0)
        )

    def check_sustained_short(df_sub: pd.DataFrame) -> bool:
        tail = df_sub.tail(12)
        if len(tail) < 11:
            return False
        return all(
            float(tail["Close"].iloc[i]) < float(tail["SMA10"].iloc[i])
            < float(tail["EMA21"].iloc[i]) < float(tail["SMA30"].iloc[i])
            < float(tail["SMA50"].iloc[i])
            for i in range(-10, 0)
        )

    if side == "long":
        # Full alignment
        if p > s10 > e21 > s30 > s50:
            slopes_ok = (
                pos_slope("SMA10") and pos_slope("EMA21")
                and pos_slope("SMA30") and pos_slope("SMA50")
            )
            if slopes_ok:
                sustained = check_sustained_long(df)
                return 3, sustained
        # Partial alignment
        if p > e21 > s30:
            return 2, False
        return 0, False

    else:  # short
        # Full bearish alignment
        if p < s10 < e21 < s30 < s50:
            slopes_ok = (
                neg_slope("SMA10") and neg_slope("EMA21")
                and neg_slope("SMA30") and neg_slope("SMA50")
            )
            if slopes_ok:
                sustained = check_sustained_short(df)
                return 3, sustained
        # Partial bearish alignment
        if p < e21 < s30:
            return 2, False
        return 0, False


# ─────────────────────────────────────────────────────────────────────────────
#  SCORING — CROSS QUALITY
# ─────────────────────────────────────────────────────────────────────────────

def _score_cross_quality(
    daily_hist: pd.DataFrame,
    price: float,
    cross_days_ago,
    fresh_cross: bool,
    side: str = "long",
):
    """
    Returns pts: int (0–3).

    Long:
      3 pt: cross ≤5d + SMA30 slope >0 + dist price/SMA30 +1% to +15%
      2 pt: cross 5–15d + SMA30 slope >0 + dist ≤20%
      1 pt: EMA21 above SMA30 but old/flat cross
      0 pt: EMA21 below SMA30  (should not reach here — caller already checked)

    Short (mirror):
      3 pt: cross ≤5d + SMA30 slope <0 + dist price/SMA30 -1% to -15%
      2 pt: cross 5–15d + SMA30 slope <0 + dist ≥-20%
      1 pt: EMA21 below SMA30 but old/flat cross
    """
    df = daily_hist.dropna(subset=["EMA21", "SMA30"])
    if len(df) < 11:
        return 1  # insufficient history — give minimum passing score

    sma30 = df["SMA30"]
    # SMA30 slope over last 10 sessions
    sma30_slope_pct = (
        (float(sma30.iloc[-1]) - float(sma30.iloc[-11]))
        / float(sma30.iloc[-11]) * 100
        if float(sma30.iloc[-11]) > 0 else 0.0
    )

    s30     = float(sma30.iloc[-1])
    dist_pct = ((price - s30) / s30 * 100) if s30 > 0 else 0.0

    if side == "long":
        if cross_days_ago is not None and cross_days_ago <= 5:
            if sma30_slope_pct > 0 and 1.0 <= dist_pct <= 15.0:
                return 3
            elif sma30_slope_pct > 0:
                return 2
            else:
                return 1
        elif cross_days_ago is not None and cross_days_ago <= 15:
            if sma30_slope_pct > 0 and dist_pct <= 20.0:
                return 2
            else:
                return 1
        else:
            # Old cross (>15d) — still valid but lower quality
            return 1

    else:  # short
        if cross_days_ago is not None and cross_days_ago <= 5:
            if sma30_slope_pct < 0 and -15.0 <= dist_pct <= -1.0:
                return 3
            elif sma30_slope_pct < 0:
                return 2
            else:
                return 1
        elif cross_days_ago is not None and cross_days_ago <= 15:
            if sma30_slope_pct < 0 and dist_pct >= -20.0:
                return 2
            else:
                return 1
        else:
            return 1


# ─────────────────────────────────────────────────────────────────────────────
#  SCORING — LOW ATR PERCENTILE + 3 WEEKS TIGHT  (replacement for VCP daily)
# ─────────────────────────────────────────────────────────────────────────────

def _score_low_atr_percentile(daily_hist: pd.DataFrame) -> float:
    """
    Low ATR Percentile score [0.0 – 1.0].
    ATR% = (High - Low) / Close per day.
    Compares mean of last 10 days vs 60-day reference distribution.
      <= 25th percentile → 1.0
      >= 50th percentile → 0.0
      linear interpolation between.
    """
    req = ["High", "Low", "Close"]
    df = daily_hist.dropna(subset=req).tail(65)
    if len(df) < 15:
        return 0.0
    df = df.copy()
    df["_atr_pct"] = (df["High"] - df["Low"]) / df["Close"]
    ref    = df["_atr_pct"].iloc[:60]
    recent = df["_atr_pct"].iloc[-10:]
    if len(ref) < 10 or len(recent) < 5:
        return 0.0
    p25 = float(np.percentile(ref, 25))
    p50 = float(np.percentile(ref, 50))
    if p50 <= p25:
        return 0.0
    recent_avg = float(recent.mean())
    if recent_avg <= p25:
        return 1.0
    elif recent_avg >= p50:
        return 0.0
    return round(1.0 - (recent_avg - p25) / (p50 - p25), 4)


def _score_3weeks_tight(weekly_df: pd.DataFrame) -> float:
    """
    3 Weeks Tight (3WT) score [0.0 – 1.0].
    spread = (max_close - min_close) / min_close * 100 across last 3 weekly closes.
      <= 1.5% → 1.0 | 1.5–3% → 0.70 | 3–5% → 0.30 | > 5% → 0.0
    """
    close = weekly_df["Close"].dropna()
    if len(close) < 3:
        return 0.0
    last3 = close.iloc[-3:]
    lo, hi = float(last3.min()), float(last3.max())
    if lo <= 0:
        return 0.0
    spread = (hi - lo) / lo * 100.0
    if spread <= 1.5:
        return 1.0
    elif spread <= 3.0:
        return 0.70
    elif spread <= 5.0:
        return 0.30
    return 0.0


def _score_vcp_new(
    daily_hist: pd.DataFrame,
    weekly_df: pd.DataFrame | None,
    side: str = "long",
) -> tuple:
    """
    Replacement for _score_vcp_daily. Two sub-components (total weight = 0.40):
      - Low ATR Percentile  (weight 0.25 within the 0.40 block)
      - 3 Weeks Tight       (weight 0.15 within the 0.40 block)

    Returns (vcp_pts: float [0–3], label: str).
    vcp_pts is scaled to [0, 3] for compatibility with _compute_score.
    """
    low_atr = _score_low_atr_percentile(daily_hist)
    tight3w = _score_3weeks_tight(weekly_df) if weekly_df is not None and len(weekly_df) >= 3 else 0.0

    raw_contrib = low_atr * 0.25 + tight3w * 0.15   # range [0.0, 0.40]
    vcp_pts     = round(raw_contrib / 0.40 * 3.0, 4) # scale to [0, 3]

    atr_tag  = "LowATR✓" if low_atr >= 0.8 else ("LowATR~" if low_atr >= 0.4 else "LowATR✗")
    tight_tag = "3WT✓"   if tight3w >= 0.7 else ("3WT~"    if tight3w >= 0.3 else "3WT✗")
    label = f"{atr_tag} {tight_tag}"

    return vcp_pts, label


# ─────────────────────────────────────────────────────────────────────────────
#  SCORE FORMULA
# ─────────────────────────────────────────────────────────────────────────────

def _compute_score(align_pts: float, cross_pts: float, vcp_pts: float) -> float:
    """
    SCORE   = align×0.25 + cross×0.35 + vcp×0.40
    SCORE10 = (SCORE / 3) × 10   max = 10.0  (each component max pts = 3)
    """
    raw = align_pts * 0.25 + cross_pts * 0.35 + vcp_pts * 0.40
    return round(raw / 3.0 * 10.0, 2)


# ─────────────────────────────────────────────────────────────────────────────
#  FINVIZ MARKET CAP FILTER  (post-scan, top-300 only)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_finviz_mcap(s: str) -> float | None:
    """Parse Finviz market cap strings: '1.5B' → 1.5e9, '750.3M' → 7.5e8, etc."""
    s = s.strip().upper()
    if not s or s in ("-", "N/A", ""):
        return None
    try:
        for suffix, mult in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
            if s.endswith(suffix):
                return float(s[:-1]) * mult
        return float(s)
    except (ValueError, AttributeError):
        return None


def _finviz_mcap_filter(
    tickers: list[str],
    min_cap: float = 1_000_000_000,
    batch_size: int = 200,
) -> set[str]:
    """
    Query Finviz screener for market caps of given tickers using asynchronous aiohttp.
    Returns set of tickers with market cap >= min_cap.
    On network/parse error for a batch, those tickers are INCLUDED (fail-open)
    so no legitimate signal is silently dropped.
    """
    import asyncio
    try:
        import aiohttp
        from bs4 import BeautifulSoup as _BS
    except ImportError:
        logger.warning("[mcap] aiohttp/bs4 not available — skipping Finviz async filter")
        return set(tickers)

    if not tickers:
        return set()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    async def _fetch_batch(session: aiohttp.ClientSession, batch: list[str], batch_idx: int) -> set[str]:
        t_str = ",".join(batch)
        url = f"https://finviz.com/screener.ashx?v=111&t={t_str}"
        batch_passed = set()
        found_in_batch = set()
        
        try:
            # Finviz blocks parallel spam; use a micro-delay based on batch index
            await asyncio.sleep(0.5 * batch_idx)
            async with session.get(url, headers=headers, timeout=25) as resp:
                resp.raise_for_status()
                html = await resp.text()
                soup = _BS(html, "html.parser")

                rows = soup.find_all("tr", class_="screener-body-table-nw")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 7:
                        continue
                    ticker_cell = cells[1].get_text(strip=True)
                    mcap_cell   = cells[6].get_text(strip=True)
                    mcap_val    = _parse_finviz_mcap(mcap_cell)

                    if mcap_val is not None and mcap_val >= cfg.mcap_min:
                        batch_passed.add(ticker_cell)
                    found_in_batch.add(ticker_cell)

                not_found = set(batch) - found_in_batch
                if not_found:
                    logger.debug("[mcap] Finviz async: not found %s — included", not_found)
                    batch_passed.update(not_found)

                logger.info(
                    "[mcap] Finviz batch %d: %d found, %d passed ≥$1B",
                    batch_idx + 1,
                    len(found_in_batch),
                    sum(1 for t in found_in_batch if t in batch_passed)
                )
                return batch_passed

        except asyncio.TimeoutError as exc:
            logger.warning("[mcap] Finviz async timeout (%s…): %s", t_str[:40], exc)
        except Exception as exc:
            logger.warning("[mcap] Finviz async failure (%s…): %s", t_str[:40], exc)
        
        # Fail-open
        return set(batch)

    async def _run_all_batches() -> set[str]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(0, len(tickers), batch_size):
                batch = list(tickers[i : i + batch_size])
                tasks.append(_fetch_batch(session, batch, len(tasks)))
            
            # Run all Finviz batches in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            passed = set()
            for r in results:
                if isinstance(r, set):
                    passed.update(r)
                elif isinstance(r, Exception):
                    logger.warning("[mcap] Unexpected async gather exception: %s", r)
            return passed

    return asyncio.run(_run_all_batches())

# ─────────────────────────────────────────────────────────────────────────────
#  EXTRA FLAGS
# ─────────────────────────────────────────────────────────────────────────────

def _check_near_pivot(daily_hist: pd.DataFrame, price: float, thr: float = 0.02) -> bool:
    """True if price is within thr% of the most recent swing high (long context)."""
    if len(daily_hist) < 5:
        return False
    high = daily_hist["High"].values
    n    = len(high)
    for i in range(n - 2, max(n - 31, 1), -1):
        right = high[i + 1] if i + 1 < n else high[i]
        if high[i] >= high[i - 1] and high[i] >= right:
            if abs(price - high[i]) / high[i] <= thr:
                return True
    return False


def _check_vol_surge(row, daily_hist: pd.DataFrame) -> bool:
    """True if today's volume is >150% of the 50-period average."""
    vol = float(row.get("volume", 0.0))
    if "Vol50" in daily_hist.columns:
        avg = daily_hist["Vol50"].dropna()
        if not avg.empty and float(avg.iloc[-1]) > 0:
            return vol > float(avg.iloc[-1]) * 1.5
    return False


def _get_sector(ticker: str, basket_df) -> str:
    """Return sector/basket name for ticker, or empty string."""
    try:
        import sector_baskets
        bmap = sector_baskets.build_ticker_basket_map()
        return bmap.get(ticker.upper(), "")
    except Exception:
        pass
    try:
        if basket_df is not None and not basket_df.empty:
            for col in basket_df.columns:
                matches = basket_df[basket_df[col].astype(str).str.upper() == ticker.upper()]
                if not matches.empty:
                    return str(col)
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  SETUP NOTES BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_notes(
    stage: str,
    cross_days_ago,
    fresh_cross: bool,
    vcp_label: str,
    sector: str,
    weekly_label: str,
    sustained: bool,
    near_pivot: bool,
    vol_surge: bool,
) -> str:
    parts = [stage]

    if fresh_cross:
        parts.append("Fresh Cross ⚡")
    elif cross_days_ago is not None and cross_days_ago <= 15:
        parts.append(f"Cross {cross_days_ago}d ago")
    else:
        parts.append("Cross >15d")

    parts.append(vcp_label)

    if sector:
        parts.append(sector)

    parts.append(weekly_label)

    if sustained:
        parts.append("sustained_alignment")
    if vol_surge:
        parts.append("vol_surge")
    if near_pivot:
        parts.append("near_pivot")

    return " | ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _pct_change(series: pd.Series, periods: int) -> float:
    """Safe % change over `periods` bars. Returns 0.0 on insufficient data."""
    s = series.dropna()
    if len(s) <= periods:
        return 0.0
    base = float(s.iloc[-(periods + 1)])
    return round((float(s.iloc[-1]) / base - 1) * 100, 2) if base != 0 else 0.0


def _build_entry(
    ticker_str: str,
    final_score: float,
    row,
    daily_hist: pd.DataFrame,
    atr_abs: float,
    dist_50_atr: float,
    notes: str,
    category: str,
    side: str,
) -> dict:
    """Assemble the output-column dict for one candidate."""
    price   = float(row.get("last_price", 0.0))
    ema21   = float(row.get("ema21", 0.0))
    factor7 = float(row.get("Final_Score", 0.0))

    close_hist = daily_hist["Close"].dropna()
    chg_1d = _pct_change(close_hist, 1)
    chg_1w = _pct_change(close_hist, 5)
    chg_1m = _pct_change(close_hist, 21)

    dist_21ema_pct = round((price - ema21) / ema21 * 100, 2) if ema21 > 0 else 0.0

    return {
        "Ticker":         ticker_str,
        "Score/10":       final_score,
        "7-Factor":       factor7,
        "Price":          round(price, 2),
        "Chg 1D%":        chg_1d,
        "Chg 1W%":        chg_1w,
        "Chg 1M%":        chg_1m,
        "ATR%":           round(atr_abs / price * 100, 2) if price > 0 else 0.0,
        "Dist 21EMA%":    dist_21ema_pct,
        "Dist ATR 50SMA": round(dist_50_atr, 2),
        "Setup Notes":    notes,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN SCAN — LONG
# ─────────────────────────────────────────────────────────────────────────────

def scan_candidates(full_df, basket_df):
    """
    Long Big Winner candidates.
    Pipeline:
      1. Price ≥ $10
      1b. 7-Factor score ≥ 65 (pre-filter — skips yfinance fetches for weak tickers)
      2. Overextension filter (dist SMA50 daily > 4× ATR → HARD exclude)
      3. Weekly pre-filter: Price > SMA10w > SMA20w > SMA30w > SMA50w > SMA200w (MANDATORY)
      4. EMA21 daily must be above SMA30 daily, cross ≤15 sessions ago
      5. SMA Alignment daily scoring (0=exclude, 2=partial, 3=full)
      6. Cross Quality scoring (0–3)
      7. VCP daily scoring (0–3)
      8. SCORE10 = (align×0.25 + cross×0.35 + vcp×0.40) / 9 × 10   threshold 6.5

    Returns DataFrame with above-threshold candidates, then below-threshold appended.
    """
    candidates:      list[dict] = []
    below_threshold: list[dict] = []

    if full_df.empty:
        return pd.DataFrame()

    # ── Vectorized Pre-filtering (Replacing iterrows) ────────────────────────
    # Extract series safely, filling missing with 0.0
    price_s = full_df.get("last_price", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)
    fscore_s = full_df.get("Final_Score", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)
    atr_pct_s = full_df.get("atr_pct", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)
    ma50_s = full_df.get("ma50", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)

    # 1. Min price, 1b. Min Score, 2a. Min ATR
    mask = (price_s >= 10.0) & (fscore_s > 60.0) & (atr_pct_s >= 0.025)

    # Overextension filter
    atr_abs_s = price_s * atr_pct_s
    dist_50_s = pd.Series(0.0, index=full_df.index)
    valid_dist = (atr_abs_s > 0) & (ma50_s > 0)
    dist_50_s = dist_50_s.mask(valid_dist, (price_s - ma50_s) / atr_abs_s)
    mask = mask & (dist_50_s <= 4.0)

    filtered_indices = full_df[mask].index

    # Only loop networking/slow ops on the filtered set
    for ticker in filtered_indices:
        ticker_str = str(ticker)
        row = full_df.loc[ticker].to_dict()
        price = float(price_s.loc[ticker])
        atr_abs = float(atr_abs_s.loc[ticker])
        dist_50_atr = float(dist_50_s.loc[ticker])

        # ── 3. Weekly pre-filter (MANDATORY) ─────────────────────────────
        weekly_df = _fetch_weekly(ticker_str)
        if weekly_df is None:
            continue

        passes_weekly, weekly_label = _check_weekly_alignment(weekly_df, side="long")
        if not passes_weekly:
            continue

        # ── 4. Daily history fetch ────────────────────────────────────────
        daily_hist = _fetch_daily(ticker_str)
        if daily_hist is None or len(daily_hist) < 30:
            continue

        # ── 5. EMA21 must be above SMA30 daily (cross filter) ────────────
        cross_days_ago, fresh_cross, currently_crossed = _find_cross(
            daily_hist, side="long"
        )
        if not currently_crossed:
            continue

        # Exclude if cross is too old (>15 sessions) AND fresh window just closed
        if cross_days_ago is not None and cross_days_ago > 15:
            # Scored as 1pt cross quality — still allowed through
            pass

        # Confirm price is above both EMA21 and SMA30 (daily snapshot)
        ema21 = float(row.get("ema21", 0.0))
        ma30  = float(row.get("ma30", 0.0))
        if ema21 > 0 and ma30 > 0:
            if price < ema21 or price < ma30:
                continue  # price confirmation failed

        # ── 6. Scoring ───────────────────────────────────────────────────
        align_pts, sustained = _score_sma_align(daily_hist, side="long")
        if align_pts == 0:
            continue  # EXCLUDE — no partial or full alignment

        cross_pts = _score_cross_quality(
            daily_hist, price, cross_days_ago, fresh_cross, side="long"
        )
        vcp_pts, vcp_label = _score_vcp_new(daily_hist, weekly_df, side="long")

        final_score = _compute_score(align_pts, cross_pts, vcp_pts)

        # ── 7. Category ──────────────────────────────────────────────────
        if final_score >= 8.5:
            category = "🟢 TOP CANDIDATE"
        elif final_score >= 6.0:
            category = "🟡 GOOD"
        else:
            category = "Below Threshold"

        # ── 8. Extra flags ───────────────────────────────────────────────
        vol_surge  = _check_vol_surge(row, daily_hist)
        near_pivot = _check_near_pivot(daily_hist, price)
        sector     = _get_sector(ticker_str, basket_df)

        # Stage label: long weekly pre-filter = Stage 2
        stage_str = "Stage 2"

        notes = _build_notes(
            stage_str, cross_days_ago, fresh_cross, vcp_label,
            sector, weekly_label, sustained, near_pivot, vol_surge,
        )

        entry = _build_entry(
            ticker_str, final_score, row, daily_hist,
            atr_abs, dist_50_atr, notes, category, side="LONG",
        )

        if final_score >= 6.0:
            candidates.append(entry)
        else:
            below_threshold.append(entry)

    # ── Build result DataFrame (Score > 6.0 only) ───────────────────────────
    cand_df = (
        pd.DataFrame(candidates)
        .sort_values("Score/10", ascending=False)
        .reset_index(drop=True)
        if candidates else pd.DataFrame()
    )

    # ── Market cap filter ─────────────────────────────────────────────
    if not cand_df.empty:
        all_tickers = cand_df["Ticker"].tolist()
        logger.info("[LONG] Mcap check (yfinance) — %d candidates…", len(all_tickers))
        import data_fetcher
        mkt_caps = data_fetcher.fetch_market_caps(all_tickers, max_workers=20)
        # Use fail-closed to be strict about the minimum cap
        passed_mcap = {t for t, cap in mkt_caps.items() if cap >= 1_000_000_000}
        cand_df = cand_df[cand_df["Ticker"].isin(passed_mcap)].reset_index(drop=True)
        logger.info("[LONG] Final output after mcap filter: %d ≥$1B", len(cand_df))

    logger.info("[LONG] Done — %d candidates (Score > 6.0).", len(cand_df))
    return cand_df


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN SCAN — SHORT
# ─────────────────────────────────────────────────────────────────────────────

def scan_short_candidates(full_df, basket_df):
    """
    Short candidates.
    Pipeline (mirror of LONG):
      1. Price ≥ $30
      1b. Short 7-Factor score ≥ 65 (pre-filter — skips yfinance fetches for weak candidates)
      2. Overextension filter: (SMA50 - price) / ATR > 4.0 → HARD exclude
      3. Weekly pre-filter: Price < SMA10w < SMA20w < SMA30w < SMA50w < SMA200w (MANDATORY)
      4. EMA21 daily must be below SMA30 daily, cross ≤15 sessions ago
      5. SMA Alignment daily bearish scoring (0=exclude, 2=partial, 3=full)
      6. Cross Quality bearish scoring (0–3)
      7. VCP distribution daily scoring (0–3)
      8. SCORE10 = (align×0.25 + cross×0.35 + vcp×0.40) / 9 × 10   threshold 6.5

    Returns DataFrame with above-threshold candidates, then below-threshold appended.
    """
    candidates:      list[dict] = []
    below_threshold: list[dict] = []

    if full_df.empty:
        return pd.DataFrame()

    # ── Vectorized Pre-filtering (Replacing iterrows) ────────────────────────
    price_s = full_df.get("last_price", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)
    short_s = full_df.get("Short_Score", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)
    atr_pct_s = full_df.get("atr_pct", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)
    ma50_s = full_df.get("ma50", pd.Series(0.0, index=full_df.index)).fillna(0.0).astype(float)

    mask = (price_s >= 30.0) & (short_s > 60.0) & (atr_pct_s >= 0.025)

    atr_abs_s = price_s * atr_pct_s
    dist_50_s = pd.Series(0.0, index=full_df.index)
    dist_50_signed_s = pd.Series(0.0, index=full_df.index)
    
    valid_dist = (atr_abs_s > 0) & (ma50_s > 0)
    dist_50_s = dist_50_s.mask(valid_dist, (ma50_s - price_s) / atr_abs_s)
    dist_50_signed_s = dist_50_signed_s.mask(valid_dist, (price_s - ma50_s) / atr_abs_s)
    
    mask = mask & (dist_50_s <= 4.0)
    
    filtered_indices = full_df[mask].index

    for ticker in filtered_indices:
        ticker_str = str(ticker)
        row = full_df.loc[ticker].to_dict()
        price = float(price_s.loc[ticker])
        atr_abs = float(atr_abs_s.loc[ticker])
        dist_50_atr = float(dist_50_s.loc[ticker])
        dist_50_atr_signed = float(dist_50_signed_s.loc[ticker])

        # ── 3. Weekly pre-filter (MANDATORY) ─────────────────────────────
        weekly_df = _fetch_weekly(ticker_str)
        if weekly_df is None:
            continue

        passes_weekly, weekly_label = _check_weekly_alignment(weekly_df, side="short")
        if not passes_weekly:
            continue

        # ── 4. Daily history fetch ────────────────────────────────────────
        daily_hist = _fetch_daily(ticker_str)
        if daily_hist is None or len(daily_hist) < 30:
            continue

        # ── 5. EMA21 must be below SMA30 daily ───────────────────────────
        cross_days_ago, fresh_cross, currently_crossed = _find_cross(
            daily_hist, side="short"
        )
        if not currently_crossed:
            continue

        if cross_days_ago is not None and cross_days_ago > 15:
            pass  # Still valid — scored as 1pt cross quality

        # Confirm price below both EMA21 and SMA30 (daily snapshot)
        ema21 = float(row.get("ema21", 0.0))
        ma30  = float(row.get("ma30", 0.0))
        if ema21 > 0 and ma30 > 0:
            if price > ema21 or price > ma30:
                continue

        # ── 6. Scoring ───────────────────────────────────────────────────
        align_pts, sustained = _score_sma_align(daily_hist, side="short")
        if align_pts == 0:
            continue  # EXCLUDE

        cross_pts = _score_cross_quality(
            daily_hist, price, cross_days_ago, fresh_cross, side="short"
        )
        vcp_pts, vcp_label = _score_vcp_new(daily_hist, weekly_df, side="short")

        final_score = _compute_score(align_pts, cross_pts, vcp_pts)

        # ── 7. Category ──────────────────────────────────────────────────
        if final_score >= 8.5:
            category = "🔴 TOP SHORT"
        elif final_score >= 6.0:
            category = "🟠 GOOD SHORT"
        else:
            category = "Below Threshold"

        # ── 8. Extra flags ───────────────────────────────────────────────
        vol_surge  = _check_vol_surge(row, daily_hist)
        near_pivot = False  # not meaningful for short context
        sector     = _get_sector(ticker_str, basket_df)

        stage_str = "Stage 4"

        notes = _build_notes(
            stage_str, cross_days_ago, fresh_cross, vcp_label,
            sector, weekly_label, sustained, near_pivot, vol_surge,
        )

        entry = _build_entry(
            ticker_str, final_score, row, daily_hist,
            atr_abs, dist_50_atr_signed, notes, category, side="SHORT",
        )

        if final_score >= 6.0:
            candidates.append(entry)
        else:
            below_threshold.append(entry)

    # ── Build result DataFrame (Score > 6.0 only) ───────────────────────────
    cand_df = (
        pd.DataFrame(candidates)
        .sort_values("Score/10", ascending=False)
        .reset_index(drop=True)
        if candidates else pd.DataFrame()
    )

    # ── Market cap filter ─────────────────────────────────────────────
    if not cand_df.empty:
        all_tickers = cand_df["Ticker"].tolist()
        logger.info("[SHORT] Mcap check (yfinance) — %d candidates…", len(all_tickers))
        import data_fetcher
        mkt_caps = data_fetcher.fetch_market_caps(all_tickers, max_workers=20)
        passed_mcap = {t for t, cap in mkt_caps.items() if cap >= 1_000_000_000}
        cand_df = cand_df[cand_df["Ticker"].isin(passed_mcap)].reset_index(drop=True)
        logger.info("[SHORT] Final output after mcap filter: %d ≥$1B", len(cand_df))

    logger.info("[SHORT] Done — %d candidates (Score > 6.0).", len(cand_df))
    return cand_df
