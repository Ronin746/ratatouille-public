"""
backfill_market_history.py — Historical Market Breadth Backfiller

Downloads ~6 months of price history for all tickers from the latest CSV,
computes proxy breadth metrics (% above MA50, % below MA20), calibrates
against existing screener data, and backfills Archive/market_score_history.json
with entries going back ~90 days.

NOTE: Sector scores are NOT computed for estimated entries. They are derived
exclusively from real screener CSV data (by update_archive.py). Estimated
entries always have sectors={} and sector_avg=0.0.

Entries added by this script are tagged with "estimated": true to distinguish
them from real screener runs.

Usage:
    python backfill_market_history.py           # backfill 90 days
    python backfill_market_history.py --days 60 # backfill 60 days
    python backfill_market_history.py --dry-run # show stats without writing
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta

from config import cfg

# Ensure Unicode output works on Windows consoles (cp1252 can't encode emoji)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def last_completed_session_date():
    """
    Return the date (YYYY-MM-DD) of the last fully-completed NYSE trading session.

    NYSE closes at 16:00 ET.
      - Winter (EST, UTC-5): 16:00 ET = 21:00 UTC
      - Summer (EDT, UTC-4): 16:00 ET = 20:00 UTC

    We use 21:00 UTC as the conservative cutoff so we never treat a session
    as complete before it has actually finished (adds a 1-hour buffer in EDT).

    If the current UTC time is before 21:00, today's NYSE session has not
    yet closed, so we return the previous weekday's date.
    Calendar holidays (days with no price bar) are handled naturally because
    yfinance simply has no data for those dates.
    """
    now_utc = datetime.now(timezone.utc)
    NYSE_CLOSE_UTC_HOUR = 21   # conservative cutoff (21:00 UTC = 16:00 ET in winter)

    if now_utc.hour < NYSE_CLOSE_UTC_HOUR:
        # Today's session has not closed yet — step back one calendar day
        cutoff = (now_utc - timedelta(days=1)).date()
    else:
        cutoff = now_utc.date()

    # Walk backwards past any weekend days
    while cutoff.weekday() >= 5:   # Saturday=5, Sunday=6
        cutoff -= timedelta(days=1)

    return cutoff.strftime('%Y-%m-%d')

# Sector baskets are imported lazily inside functions that need them
# (avoids circular-import risk and keeps top-level imports light)


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def find_latest_csv():
    """Return (path, date_str) for the most recent screen_results_*.csv in Data/."""
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    data_dir     = os.path.join(parent_dir, 'Data')

    csv_files = sorted([
        f for f in os.listdir(data_dir)
        if f.startswith('screen_results_') and f.endswith('.csv')
    ])
    if not csv_files:
        raise FileNotFoundError(f'No screen_results_*.csv files found in {data_dir}')

    fname      = csv_files[-1]
    date_part  = fname.replace('screen_results_', '').replace('.csv', '')
    date_str   = f'{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}'
    return os.path.join(data_dir, fname), date_str


def get_tickers_and_sectors(csv_path):
    """
    Read the latest CSV.
    Returns (tickers: list[str], sector_map: dict[ticker -> sector_name]).
    """
    import pandas as pd
    df = pd.read_csv(csv_path, index_col=0)
    tickers = [str(t) for t in df.index.tolist() if t and str(t).strip()]

    sector_map = {}
    if 'Sector' in df.columns:
        sector_map = {str(k): str(v) for k, v in df['Sector'].items()
                      if v and str(v) != 'nan'}
    return tickers, sector_map


# ─────────────────────────────────────────────────────────────────────────────
# Price download
# ─────────────────────────────────────────────────────────────────────────────

def download_price_history(tickers, period='6mo', batch_size=400, max_retries=3):
    """
    Bulk-download adjusted closing prices via yfinance.
    Returns a DataFrame (dates × tickers).
    Failed batches are retried up to max_retries times with exponential backoff.
    """
    import time
    import yfinance as yf
    import pandas as pd

    # Only keep simple tickers (no index symbols, no crazy lengths)
    clean = [t for t in tickers
             if t and len(t) <= 6 and not t.startswith('^') and '/' not in t]
    clean = list(dict.fromkeys(clean))   # deduplicate while preserving order

    print(f'  {len(clean):,} tickers queued in batches of {batch_size}')

    all_closes = []
    failed_batches = []     # (batch_num, tickers) for final report
    total_batches = (len(clean) + batch_size - 1) // batch_size

    for i in range(0, len(clean), batch_size):
        batch     = clean[i: i + batch_size]
        batch_num = i // batch_size + 1

        success = False
        for attempt in range(1, max_retries + 1):
            attempt_str = f' (retry {attempt}/{max_retries})' if attempt > 1 else ''
            print(f'    batch {batch_num}/{total_batches} ({len(batch)} tickers){attempt_str}…',
                  end=' ', flush=True)

            try:
                raw = yf.download(
                    batch, period=period,
                    auto_adjust=True, threads=True, progress=False
                )

                if raw.empty:
                    print('empty')
                    if attempt < max_retries:
                        wait = 5 * (2 ** (attempt - 1))
                        print(f'      ↳ waiting {wait}s before retry…')
                        time.sleep(wait)
                        continue
                    break

                # yfinance returns multi-level columns when batch > 1
                if isinstance(raw.columns, pd.MultiIndex):
                    if 'Close' in raw.columns.get_level_values(0):
                        close_df = raw['Close']
                    else:
                        print('no Close column')
                        break
                else:
                    # Single ticker
                    if 'Close' in raw.columns:
                        close_df = raw[['Close']].rename(columns={'Close': batch[0]})
                    else:
                        print('no Close column')
                        break

                all_closes.append(close_df)
                n_ok = close_df.notna().any().sum()
                print(f'ok ({n_ok} valid)')
                success = True
                break

            except Exception as e:
                print(f'ERROR: {e}')
                if attempt < max_retries:
                    wait = 5 * (2 ** (attempt - 1))
                    print(f'      ↳ waiting {wait}s before retry…')
                    time.sleep(wait)

        if not success:
            failed_batches.append((batch_num, batch))
            print(f'      ✗ batch {batch_num} failed after {max_retries} attempts')

    # Report failed batches
    if failed_batches:
        n_failed_tickers = sum(len(b) for _, b in failed_batches)
        print(f'\n  ⚠ {len(failed_batches)} batch(es) failed ({n_failed_tickers:,} tickers lost)')

    if not all_closes:
        raise RuntimeError('No price data downloaded — check internet connection and yfinance install')

    combined = (
        pd.concat(all_closes, axis=1)
          .loc[:, lambda df: ~df.columns.duplicated()]
    )
    print(f'  Combined: {combined.shape[1]:,} tickers × {combined.shape[0]:,} days')
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# Macro regime ratios  (ARKK/QQQ · IWM/SPY · HYG/IEF)
# ─────────────────────────────────────────────────────────────────────────────

def compute_macro_ratios():
    """
    Download ARKK, QQQ, IWM, SPY, HYG, IEF, XLY, XLP from yfinance (1-year window).

    Per ogni data salva:
      - i 4 ratio grezzi (arkk_qqq, iwm_spy, xly_xlp, hyg_ief)
      - il trend di ogni singolo ETF come flag booleano (True = uptrend)
        usando il criterio EMA21 > SMA30 daily.

    I flag di trend dei singoli ETF sono usati da update_archive.py per
    costruire il composite dual-leg score (-5/+5), che distingue correttamente
    i casi in cui il ratio sale per forza del numeratore vs debolezza del
    denominatore (es. HYG/IEF: se entrambi scendono il segnale è bearish).

    Returns { 'YYYY-MM-DD': {
        'arkk_qqq': float, 'iwm_spy': float, 'xly_xlp': float, 'hyg_ief': float,
        'arkk_close': float, 'arkk_sma20': float|None, 'arkk_atr14': float|None,
        'iwm_close':  float, 'iwm_sma20':  float|None, 'iwm_atr14':  float|None,
        'hyg_close':  float, 'hyg_sma20':  float|None, 'hyg_atr14':  float|None,
        'xly_close':  float, 'xly_sma20':  float|None, 'xly_atr14':  float|None,
        'arkk_up': bool, 'qqq_up': bool, 'iwm_up': bool, 'spy_up': bool,
        'hyg_up': bool, 'ief_up': bool, 'xly_up': bool, 'xlp_up': bool,
    } }
    """
    import pandas as pd
    try:
        import yfinance as yf
    except ImportError:
        print('  ⚠ yfinance not available — skipping macro ratios')
        return {}

    macro_tickers = ['ARKK', 'QQQ', 'IWM', 'SPY', 'HYG', 'IEF', 'XLY', 'XLP']
    print('\nDownloading macro ETF data — 1 year (ARKK, QQQ, IWM, SPY, HYG, IEF, XLY, XLP)…')
    try:
        raw = yf.download(macro_tickers, period='1y', auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex):
            closes = raw['Close']
            highs  = raw['High']
            lows   = raw['Low']
        else:
            closes = raw
            highs  = raw
            lows   = raw
    except Exception as e:
        print(f'  ⚠ Macro download failed: {e}')
        return {}

    # ── Calcola EMA21 e SMA30 rolling per ogni ETF (vettorizzato su tutto l'anno) ──
    # EMA21: span=21, min_periods=21 — serve almeno 21 barre per un valore valido.
    # SMA30: rolling(30) — serve almeno 30 barre.
    # uptrend = EMA21 > SMA30 (criterio daily concordato).
    ema21 = closes.ewm(span=21, min_periods=21, adjust=False).mean()
    sma30 = closes.rolling(30, min_periods=30).mean()
    # True se EMA21 > SMA30, NaN se dati insufficienti
    uptrend_flags = ema21 > sma30   # DataFrame bool (NaN → False dopo fillna)
    uptrend_flags = uptrend_flags.fillna(False)

    # ── SMA20 e ATR14 per i 4 ticker del composite (ARKK, IWM, HYG, XLY) ────
    # SMA20: distanza close - SMA20 come segnale di posizione rispetto al trend.
    # ATR14: media mobile esponenziale (Wilder, alpha=1/14) del True Range.
    #        Normalizza la distanza in "unità di volatilità" → confronto cross-asset
    #        bilanciato tra ticker con volatilità molto diverse (ARKK vs HYG).
    # min_periods=14: ATR NaN se dati insufficienti; close e SMA20 devono coesistere.
    _COMP_TICKERS = ['ARKK', 'IWM', 'HYG', 'XLY']
    sma20_df = closes[_COMP_TICKERS].rolling(20, min_periods=20).mean()
    # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
    # Costruzione colonna per colonna: compatibile Pandas 1.x – 3.x.
    # Evita pd.concat(..., keys=...).max(axis=1, level=0) rimosso in Pandas 2.0.
    prev_close = closes[_COMP_TICKERS].shift(1)
    tr_df = pd.DataFrame({
        c: pd.concat([
            highs[c] - lows[c],
            (highs[c] - prev_close[c]).abs(),
            (lows[c]  - prev_close[c]).abs(),
        ], axis=1).max(axis=1)
        for c in _COMP_TICKERS
    })
    # Wilder smoothing: EWM con alpha=1/14, equivalente a RMA(14)
    atr14_df = tr_df.ewm(alpha=1.0 / 14, min_periods=14, adjust=False).mean()

    # Determine the last fully-completed NYSE session so we never store
    # ratios for a partial (still-open) trading day.
    cutoff = last_completed_session_date()

    result = {}
    sma20_rec  = sma20_df.to_dict(orient='records')
    atr14_rec  = atr14_df.to_dict(orient='records')
    for idx, (date, row, up_row, sma20_row, atr14_row) in enumerate(zip(
        closes.index,
        closes.to_dict(orient='records'),
        uptrend_flags.to_dict(orient='records'),
        sma20_rec,
        atr14_rec,
    )):
        try:
            date_str = date.strftime('%Y-%m-%d')
            if date_str > cutoff:
                continue   # session not yet complete — skip
            arkk = float(row['ARKK'])
            qqq  = float(row['QQQ'])
            iwm  = float(row['IWM'])
            spy  = float(row['SPY'])
            hyg  = float(row['HYG'])
            ief  = float(row['IEF'])
            xly  = float(row['XLY'])
            xlp  = float(row['XLP'])
            # Skip rows with NaN prices
            if any(v != v for v in [arkk, qqq, iwm, spy, hyg, ief, xly, xlp]):
                continue

            def _safe(val):
                """None se NaN o zero (evita divisioni per zero in update_archive)."""
                return None if (val != val or val == 0) else round(float(val), 6)

            result[date_str] = {
                # Ratio grezzi (per i chart individuali e _ratio_regime)
                'arkk_qqq': round(arkk / qqq, 6),
                'iwm_spy':  round(iwm  / spy, 6),
                'xly_xlp':  round(xly  / xlp, 6),
                'hyg_ief':  round(hyg  / ief, 6),
                # Prezzi close + SMA20 + ATR14 per composite ATR-normalised
                'arkk_close': round(arkk, 4),
                'arkk_sma20': _safe(sma20_row['ARKK']),
                'arkk_atr14': _safe(atr14_row['ARKK']),
                'iwm_close':  round(iwm, 4),
                'iwm_sma20':  _safe(sma20_row['IWM']),
                'iwm_atr14':  _safe(atr14_row['IWM']),
                'hyg_close':  round(hyg, 4),
                'hyg_sma20':  _safe(sma20_row['HYG']),
                'hyg_atr14':  _safe(atr14_row['HYG']),
                'xly_close':  round(xly, 4),
                'xly_sma20':  _safe(sma20_row['XLY']),
                'xly_atr14':  _safe(atr14_row['XLY']),
                # Trend dei singoli ETF (EMA21 > SMA30)
                'arkk_up': bool(up_row['ARKK']),
                'qqq_up':  bool(up_row['QQQ']),
                'iwm_up':  bool(up_row['IWM']),
                'spy_up':  bool(up_row['SPY']),
                'hyg_up':  bool(up_row['HYG']),
                'ief_up':  bool(up_row['IEF']),
                'xly_up':  bool(up_row['XLY']),
                'xlp_up':  bool(up_row['XLP']),
            }
        except Exception:
            continue

    print(f'  Macro ratios: {len(result)} dates '
          f'({min(result) if result else "—"} → {max(result) if result else "—"})  '
          f'[cutoff: {cutoff}]')
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Archive CSV loading  (for real 7-factor ST momentum)
# ─────────────────────────────────────────────────────────────────────────────

def load_archive_scores():
    """
    Legge tutti i CSV in Data/archive/ e restituisce un dict
      { 'YYYY-MM-DD': { ticker: {'final': float, 'short': float} } }

    Usato da compute_backfill_entries per calcolare st_long_pct/st_short_pct
    con il 7-factor reale invece del proxy EMA10.
    """
    import pandas as pd

    screener_dir = os.path.dirname(os.path.abspath(__file__))
    archive_dir  = os.path.join(os.path.dirname(screener_dir), 'Data', 'archive')

    if not os.path.isdir(archive_dir):
        return {}

    result = {}
    files  = sorted([
        f for f in os.listdir(archive_dir)
        if f.startswith('screen_results_') and f.endswith('.csv')
    ])

    for fname in files:
        try:
            date_part = fname.replace('screen_results_', '').replace('.csv', '')
            date_str  = f'{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}'
            path      = os.path.join(archive_dir, fname)
            df        = pd.read_csv(path, index_col=0,
                                    usecols=lambda c: c in ('Ticker', 'Final_Score', 'Short_Score')
                                                      or c == df.index.name
                                    if False else True)
            # read_csv ricarica tutte le colonne poi filtriamo
            df = pd.read_csv(path, index_col=0)
            if 'Final_Score' not in df.columns:
                continue
            scores = {}
            for ticker, row in zip(df.index, df.to_dict(orient="records")):
                fs = row.get('Final_Score', float('nan'))
                ss = row.get('Short_Score', float('nan'))
                if not (isinstance(fs, float) and fs != fs):   # not NaN
                    scores[str(ticker)] = {
                        'final': float(fs) if fs == fs else 50.0,
                        'short': float(ss) if ss == ss else 50.0,
                    }
            if scores:
                result[date_str] = scores
        except Exception:
            pass

    if result:
        dates = sorted(result.keys())
        print(f'  Archive CSV trovati: {len(result)}  ({dates[0]} → {dates[-1]})')
    else:
        print('  Nessun CSV in Data/archive/ — ST momentum usa proxy EMA10')

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Backfill computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_backfill_entries(close_df, days=90, ma_long=cfg.sma_slow, ma_short=cfg.sma_medium, archive_scores=None):
    """
    For each of the last `days` trading days, compute dual-timeframe breadth
    and per-basket sector scores using SECTOR_BASKETS from sector_baskets.py.

    Long-Term Breadth (regime detection):
      long_breadth_pct  = % stocks where close > MA50
      short_breadth_pct = % stocks where close < MA20
      Calibrated against real screener data (Final_Score >= 65 + MA50 confirmation).

    Short-Term Momentum (rapid rotation detection):
      st_long_pct  = % stocks where close > EMA10 AND SMA8 > SMA20
      st_short_pct = % stocks where close < EMA10 AND SMA8 < SMA20
      Designed to detect growth stock momentum shifts within 2-5 days.

    Sector scores (estimated proxy):
      For each basket in SECTOR_BASKETS, compute avg of (close/MA50 - 1)*200 + 50
      across basket tickers that have valid price + MA50 data (min 2 tickers).

    Returns a list of entry dicts (oldest first) with 'estimated': True.
    """
    from sector_baskets import SECTOR_BASKETS

    # Forward-fill gaps (weekends/holidays per ticker)
    closes = close_df.ffill()

    n_avail = len(closes)
    if n_avail < ma_long + 5:
        raise ValueError(f'Need at least {ma_long + 5} days of data; only {n_avail} available')

    # Pre-compute rolling MAs and EMA for all tickers at once (vectorised)
    sma8  = closes.rolling(8,  min_periods=8).mean()
    sma20 = closes.rolling(20, min_periods=20).mean()
    ema10 = closes.ewm(span=10, min_periods=10, adjust=False).mean()

    # Pre-compute which basket tickers are actually present in our price data
    basket_map = {}   # basket_name -> list[ticker] present in closes.columns
    for bname, btickers in SECTOR_BASKETS.items():
        present = [t for t in btickers if t in closes.columns]
        if len(present) >= 2:
            basket_map[bname] = present

    print(f'  Baskets with price data: {len(basket_map)}/{len(SECTOR_BASKETS)}')

    # The last `days` rows that have enough history for MA calculation.
    # Also cap to the last fully-completed NYSE session so we never compute
    # breadth from a partial (still-open) trading day.
    session_cutoff = last_completed_session_date()
    target_dates = [
        d for d in closes.index[ma_long:]
        if d.strftime('%Y-%m-%d') <= session_cutoff
    ]
    if len(target_dates) > days:
        target_dates = target_dates[-days:]

    entries = []

    for date in target_dates:
        pos = closes.index.get_loc(date)

        today_close   = closes.iloc[pos]
        ma50_window   = closes.iloc[pos - ma_long  + 1: pos + 1]
        ma20_window   = closes.iloc[pos - ma_short + 1: pos + 1]

        ma50 = ma50_window.mean()
        ma20 = ma20_window.mean()

        # Valid tickers for long-term: need close + MA50 + MA20
        valid_mask = today_close.notna() & ma50.notna() & ma20.notna() & (ma50 > 0)
        n_valid    = int(valid_mask.sum())
        if n_valid < 200:
            continue

        tc  = today_close[valid_mask]
        m50 = ma50[valid_mask]
        m20 = ma20[valid_mask]

        # ── Long-Term breadth: close vs MA50 / close vs MA20 ───────────────
        long_breadth_pct  = round(float((tc > m50).sum()) / n_valid * 100, 1)
        short_breadth_pct = round(float((tc < m20).sum()) / n_valid * 100, 1)
        long_count        = int((tc > m50).sum())
        short_count       = int((tc < m20).sum())

        # ── Short-Term momentum ─────────────────────────────────────────────
        # Strategia:
        #   Se esiste un CSV archiviato per questa data → usa il 7-factor reale
        #     Bullish: Final_Score >= 50 AND SMA8 > SMA20
        #     Bearish: Short_Score >= 50 AND SMA8 < SMA20
        #   Altrimenti → proxy EMA10 + SMA8/SMA20 (come prima)

        date_str    = date.strftime('%Y-%m-%d')
        day_archive = (archive_scores or {}).get(date_str)

        today_sma8  = sma8.iloc[pos]
        today_sma20 = sma20.iloc[pos]

        if day_archive:
            # Via 7-factor reale + SMA alignment
            st_long_n = st_short_n = st_total = 0
            ST_THRESHOLD = 65.0
            for ticker, s in day_archive.items():
                s8  = today_sma8.get(ticker,  float('nan'))
                s20 = today_sma20.get(ticker, float('nan'))
                if s8 != s8 or s20 != s20:   # NaN check
                    continue
                st_total += 1
                if s['final'] >= ST_THRESHOLD and s8 > s20:
                    st_long_n  += 1
                if s['short'] >= ST_THRESHOLD and s8 < s20:
                    st_short_n += 1
            if st_total >= 200:
                st_long_pct  = round(st_long_n  / st_total * 100, 1)
                st_short_pct = round(st_short_n / st_total * 100, 1)
            else:
                st_long_pct = st_short_pct = 0.0
        else:
            # Proxy: EMA10 + SMA8/SMA20
            today_ema10 = ema10.iloc[pos]
            st_valid = (today_close.notna() & today_ema10.notna() &
                        today_sma8.notna() & today_sma20.notna())
            n_st = int(st_valid.sum())
            if n_st >= 200:
                tc_st   = today_close[st_valid]
                em10_st = today_ema10[st_valid]
                s8_st   = today_sma8[st_valid]
                s20_st  = today_sma20[st_valid]
                st_long_pct  = round(float(((tc_st > em10_st) & (s8_st > s20_st)).sum()) / n_st * 100, 1)
                st_short_pct = round(float(((tc_st < em10_st) & (s8_st < s20_st)).sum()) / n_st * 100, 1)
            else:
                st_long_pct = st_short_pct = 0.0

        # ── Sector scores via SECTOR_BASKETS ───────────────────────────────
        sectors = {}
        sectors_short = {}
        for bname, btickers in basket_map.items():
            tc_b  = today_close[btickers]
            m50_b = ma50[btickers]
            v     = tc_b.notna() & m50_b.notna() & (m50_b > 0)
            if v.sum() < 2:
                continue
            # Proxy score: (close/MA50 - 1)*200 + 50  →  centred on 50, like Final_Score
            scores = (tc_b[v] / m50_b[v] - 1) * 200 + 50
            sectors[bname] = round(float(scores.mean()), 1)
            
            # Proxy short score: (1 - close/MA50)*200 + 50 → centred on 50, like Short_Score
            scores_short = (1 - tc_b[v] / m50_b[v]) * 200 + 50
            sectors_short[bname] = round(float(scores_short.mean()), 1)

        sector_avg = round(sum(sectors.values()) / len(sectors), 1) if sectors else 0.0

        entries.append({
            'date':               date.strftime('%Y-%m-%d'),
            'long_breadth_pct':   long_breadth_pct,
            'short_breadth_pct':  short_breadth_pct,
            'st_long_pct':        st_long_pct,
            'st_short_pct':       st_short_pct,
            'sector_avg':         sector_avg,
            'long_count':         long_count,
            'short_count':        short_count,
            'sectors':            sectors,
            'sectors_short':      sectors_short,
            'estimated':          True,
        })

    return entries


# ─────────────────────────────────────────────────────────────────────────────
# Calibration
# ─────────────────────────────────────────────────────────────────────────────

def safe_mean(values):
    return sum(values) / len(values) if values else 1.0


def calibrate_entries(backfill_entries, existing_history):
    """
    Scale/offset backfill estimates to match existing screener data.

    Breadth calibration (multiplicative):
      long_scale  = mean(screener_long_pct  / backfill_long_pct)   on overlap dates
      short_scale = mean(screener_short_pct / backfill_short_pct)  on overlap dates

    Sector calibration (additive offset):
      For each basket present in both real and estimated data on overlap dates,
      offset = mean(real_score - estimated_score).
      Baskets without overlap data use the global mean sector offset as fallback.
      sector_avg is also corrected by the global offset.

    Returns calibrated list ('estimated': True preserved).
    """
    real_entries = {h['date']: h for h in existing_history if not h.get('estimated')}

    overlap = [e for e in backfill_entries if e['date'] in real_entries]

    if not overlap:
        print('  ⚠ No overlapping dates — skipping calibration (raw estimates used)')
        return backfill_entries

    long_scales      = []
    short_scales     = []
    basket_diffs     = {}   # basket_name -> [real_score - estimated_score, ...]
    basket_diffs_short = {} # basket_name -> [real_short_score - estimated_short_score, ...]
    sector_avg_diffs = []

    for e in overlap:
        r = real_entries[e['date']]
        if e['long_breadth_pct']  > 0:
            long_scales.append(r['long_breadth_pct']  / e['long_breadth_pct'])
        if e['short_breadth_pct'] > 0:
            short_scales.append(r['short_breadth_pct'] / e['short_breadth_pct'])

        # Per-basket sector offsets
        r_sectors = r.get('sectors', {})
        e_sectors = e.get('sectors', {})
        for bname, e_score in e_sectors.items():
            if bname in r_sectors:
                basket_diffs.setdefault(bname, []).append(r_sectors[bname] - e_score)
                
        r_sectors_short = r.get('sectors_short', {})
        e_sectors_short = e.get('sectors_short', {})
        for bname, e_score_short in e_sectors_short.items():
            if bname in r_sectors_short:
                basket_diffs_short.setdefault(bname, []).append(r_sectors_short[bname] - e_score_short)

        # sector_avg offset
        r_avg = r.get('sector_avg', 0)
        e_avg = e.get('sector_avg', 0)
        if r_avg != 0 and e_avg != 0:
            sector_avg_diffs.append(r_avg - e_avg)

    long_scale  = round(safe_mean(long_scales),  4)
    short_scale = round(safe_mean(short_scales), 4)

    per_basket_offset = {b: round(sum(d) / len(d), 2) for b, d in basket_diffs.items() if d}
    per_basket_offset_short = {b: round(sum(d) / len(d), 2) for b, d in basket_diffs_short.items() if d}
    global_sec_offset = round(sum(sector_avg_diffs) / len(sector_avg_diffs), 2) \
                        if sector_avg_diffs else 0.0

    print(f'  Calibration ({len(overlap)} overlap dates):')
    print(f'    long_scale={long_scale:.3f}  short_scale={short_scale:.3f}')
    print(f'    sector global_offset={global_sec_offset:+.1f}  '
          f'per-basket offsets computed for {len(per_basket_offset)} baskets (long) and {len(per_basket_offset_short)} (short)')

    calibrated = []
    for e in backfill_entries:
        c = dict(e)
        c['long_breadth_pct']  = round(c['long_breadth_pct']  * long_scale,  1)
        c['short_breadth_pct'] = round(c['short_breadth_pct'] * short_scale, 1)

        # Apply per-basket offsets (global fallback for baskets not in overlap)
        new_sectors = {}
        for bname, score in c.get('sectors', {}).items():
            off = per_basket_offset.get(bname, global_sec_offset)
            new_sectors[bname] = round(score + off, 1)
        c['sectors'] = new_sectors
        
        new_sectors_short = {}
        for bname, score_short in c.get('sectors_short', {}).items():
            off_short = per_basket_offset_short.get(bname, -global_sec_offset) # global offset might be negated due to inverted score, we'll just use -global
            new_sectors_short[bname] = round(score_short + off_short, 1)
        c['sectors_short'] = new_sectors_short

        if new_sectors:
            c['sector_avg'] = round(sum(new_sectors.values()) / len(new_sectors), 1)
        elif c.get('sector_avg', 0) != 0:
            c['sector_avg'] = round(c['sector_avg'] + global_sec_offset, 1)

        calibrated.append(c)

    return calibrated


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Backfill market breadth history')
    parser.add_argument('--days',    type=int,         default=90,
                        help='Number of trading days to backfill (default: 90)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Compute and print stats without writing to disk')
    args = parser.parse_args()

    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    archive_dir  = os.path.join(parent_dir, 'Archive')
    history_path = os.path.join(archive_dir, 'market_score_history.json')

    # ── 1. Load existing history ──────────────────────────────────────────────
    existing_history = []
    if os.path.exists(history_path):
        with open(history_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        existing_history = data.get('history', [])

    n_real      = sum(1 for h in existing_history if not h.get('estimated'))
    n_estimated = len(existing_history) - n_real
    print(f'Existing history: {len(existing_history)} entries '
          f'(screener: {n_real}, estimated: {n_estimated})')

    # ── 2. Find latest CSV ────────────────────────────────────────────────────
    print('\nLocating latest CSV…')
    try:
        csv_path, csv_date = find_latest_csv()
    except FileNotFoundError as e:
        print(f'ERROR: {e}')
        sys.exit(1)
    print(f'  {os.path.basename(csv_path)}  (dated {csv_date})')

    # ── 3. Load tickers ───────────────────────────────────────────────────────
    print('Loading tickers…')
    tickers, _sector_map = get_tickers_and_sectors(csv_path)
    print(f'  {len(tickers):,} tickers from CSV')

    # Also include all basket tickers so every sector has full 3-month history
    # even if some basket tickers are not in the screener universe.
    if screener_dir not in sys.path:
        sys.path.insert(0, screener_dir)
    try:
        from sector_baskets import SECTOR_BASKETS
        basket_tickers = [t for btickers in SECTOR_BASKETS.values() for t in btickers]
        n_before = len(tickers)
        tickers = list(dict.fromkeys(tickers + basket_tickers))   # dedup, order preserved
        print(f'  +{len(tickers) - n_before} basket-only tickers → {len(tickers):,} total')
    except Exception as e:
        print(f'  ⚠ Could not load SECTOR_BASKETS: {e} — using CSV tickers only')

    # ── 4. Download price history ─────────────────────────────────────────────
    print('\nDownloading price history (may take several minutes for ~6000 tickers)…')
    try:
        close_df = download_price_history(tickers, period='6mo')
    except Exception as e:
        print(f'\nERROR downloading prices: {e}')
        sys.exit(1)

    # ── 4b. Load archived CSV scores for real 7-factor ST momentum ───────────
    print('\nLoading archived CSV scores (7-factor reale per ST momentum)…')
    archive_scores = load_archive_scores()

    # ── 4c. Download macro regime ratios (ARKK/QQQ · IWM/SPY · XLY/XLP · HYG/IEF) ─────
    macro_ratios = compute_macro_ratios()

    # ── 5. Compute backfill entries ───────────────────────────────────────────
    print(f'\nComputing {args.days}-day backfill…')
    try:
        backfill_entries = compute_backfill_entries(
            close_df, days=args.days, archive_scores=archive_scores)
    except Exception as e:
        print(f'ERROR computing backfill: {e}')
        sys.exit(1)

    if not backfill_entries:
        print('ERROR: No backfill entries generated — not enough history?')
        sys.exit(1)

    print(f'  Generated: {len(backfill_entries)} entries  '
          f'({backfill_entries[0]["date"]} → {backfill_entries[-1]["date"]})')

    # ── 6. Calibrate ──────────────────────────────────────────────────────────
    print('\nCalibrating against screener data…')
    calibrated_entries = calibrate_entries(backfill_entries, existing_history)

    # ── 7. Print sample ───────────────────────────────────────────────────────
    print('\nSample entries (first 3 / last 3):')
    samples = calibrated_entries[:3] + (['…'] if len(calibrated_entries) > 6 else []) + calibrated_entries[-3:]
    for e in samples:
        if isinstance(e, str):
            print(f'  {e}')
            continue
        print(f'  {e["date"]}  long={e["long_breadth_pct"]:5.1f}%  '
              f'short={e["short_breadth_pct"]:5.1f}%  '
              f'(sectors=[] — from real CSV runs only)')

    if args.dry_run:
        print('\n[dry-run] No changes written.')
        return

    # ── 8. Merge: real entries take priority, but supplement missing sectors ────
    # Real screener entries are canonical for breadth/counts. However, when a
    # new basket is added AFTER a screener run, older real entries have no data
    # for that basket. We fill the gap by copying estimated basket scores for
    # any basket key absent from the real entry's sectors dict.
    merged: dict = {}

    # Index calibrated estimates by date for quick lookup
    est_by_date = {e['date']: e for e in calibrated_entries}

    # Real screener entries always win on breadth; sectors supplemented if needed
    supplemented_entries = 0
    for h in existing_history:
        if h.get('estimated'):
            continue
        est = est_by_date.get(h['date'])
        if est:
            h = dict(h)   # don't mutate original
            # Supplement missing basket sectors
            real_sectors = dict(h.get('sectors', {}))
            missing = {b: s for b, s in est.get('sectors', {}).items()
                       if b not in real_sectors}
            if missing:
                real_sectors.update(missing)
                h['sectors'] = real_sectors
                h['sector_avg'] = round(
                    sum(real_sectors.values()) / len(real_sectors), 1
                ) if real_sectors else h.get('sector_avg', 0.0)
                
                # Supplement missing short basket sectors too!
                real_sectors_short = dict(h.get('sectors_short', {}))
                missing_short = {b: s for b, s in est.get('sectors_short', {}).items()
                           if b not in real_sectors_short}
                if missing_short:
                    real_sectors_short.update(missing_short)
                    h['sectors_short'] = real_sectors_short
                    
                supplemented_entries += 1
            # Supplement short-term momentum if absent
            if 'st_long_pct' not in h and 'st_long_pct' in est:
                h['st_long_pct']  = est['st_long_pct']
                h['st_short_pct'] = est['st_short_pct']
        merged[h['date']] = h

    if supplemented_entries:
        print(f'  Supplemented {supplemented_entries} real entries with missing basket sectors')

    # Add calibrated estimates only for dates without real data
    added = 0
    for e in calibrated_entries:
        if e['date'] not in merged:
            merged[e['date']] = e
            added += 1

    final_history = sorted(merged.values(), key=lambda x: x['date'])

    # ── 8b. Inject macro ratios (always overwrite with fresh data) ────────────
    macro_injected = 0
    for entry in final_history:
        d = entry['date']
        if d in macro_ratios:
            entry['arkk_qqq'] = macro_ratios[d]['arkk_qqq']
            entry['iwm_spy']  = macro_ratios[d]['iwm_spy']
            entry['xly_xlp']  = macro_ratios[d]['xly_xlp']
            entry['hyg_ief']  = macro_ratios[d]['hyg_ief']
            macro_injected += 1
    if macro_injected:
        print(f'  Macro ratios injected into {macro_injected} entries')

    # ── 9. Save ───────────────────────────────────────────────────────────────
    # Build macro_history: full 1-year series sorted by date, independent of
    # screener history — used by update_archive.py for accurate SMA/regression
    macro_history = sorted(
        [{'date': d, **v} for d, v in macro_ratios.items()],
        key=lambda x: x['date']
    ) if macro_ratios else []

    output = {
        'generated':    datetime.now().strftime('%Y-%m-%d %H:%M'),
        'history':      final_history,
        'macro_history': macro_history,
    }
    with open(history_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f'\n✅  market_score_history.json written:')
    print(f'    Total entries : {len(final_history)}')
    print(f'    New estimated : {added}')
    print(f'    Date range    : {final_history[0]["date"]} → {final_history[-1]["date"]}')
    print('\nNext step: run update_archive.py to regenerate Archive/index.html')


if __name__ == '__main__':
    main()
