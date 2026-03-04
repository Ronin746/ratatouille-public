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
from datetime import datetime

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
# Backfill computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_backfill_entries(close_df, days=90, ma_long=50, ma_short=20):
    """
    For each of the last `days` trading days, compute breadth proxies and
    per-basket sector scores using SECTOR_BASKETS from sector_baskets.py.

    Breadth metrics:
      long_breadth_pct  = % stocks with close > MA50  (proxy for screener's % score >= 70)
      short_breadth_pct = % stocks with close < MA20  (proxy for % Short_Score >= 70)

    Sector scores (estimated proxy):
      For each basket in SECTOR_BASKETS, compute avg of (close/MA50 - 1)*200 + 50
      across basket tickers that have valid price + MA50 data (min 2 tickers).
      sector_avg = mean of all basket scores with enough valid tickers.

    Returns a list of entry dicts (oldest first) with 'estimated': True.
    """
    from sector_baskets import SECTOR_BASKETS

    # Forward-fill gaps (weekends/holidays per ticker)
    closes = close_df.ffill()

    n_avail = len(closes)
    if n_avail < ma_long + 5:
        raise ValueError(f'Need at least {ma_long + 5} days of data; only {n_avail} available')

    # Pre-compute which basket tickers are actually present in our price data
    basket_map = {}   # basket_name -> list[ticker] present in closes.columns
    for bname, btickers in SECTOR_BASKETS.items():
        present = [t for t in btickers if t in closes.columns]
        if len(present) >= 2:
            basket_map[bname] = present

    print(f'  Baskets with price data: {len(basket_map)}/{len(SECTOR_BASKETS)}')

    # The last `days` rows that have enough history for MA calculation
    target_dates = closes.index[ma_long:]   # first date we can compute MA50
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

        # Valid tickers: have both today's price and MA values
        valid_mask = today_close.notna() & ma50.notna() & ma20.notna() & (ma50 > 0)
        n_valid    = int(valid_mask.sum())
        if n_valid < 200:
            continue

        tc  = today_close[valid_mask]
        m50 = ma50[valid_mask]
        m20 = ma20[valid_mask]

        long_breadth_pct  = round(float((tc > m50).sum() / n_valid * 100), 1)
        short_breadth_pct = round(float((tc < m20).sum() / n_valid * 100), 1)
        long_count        = int((tc > m50).sum())
        short_count       = int((tc < m20).sum())

        # ── Sector scores via SECTOR_BASKETS ───────────────────────────────────
        sectors = {}
        for bname, btickers in basket_map.items():
            tc_b  = today_close[btickers]
            m50_b = ma50[btickers]
            v     = tc_b.notna() & m50_b.notna() & (m50_b > 0)
            if v.sum() < 2:
                continue
            # Proxy score: (close/MA50 - 1)*200 + 50  →  centred on 50, like Final_Score
            scores = (tc_b[v] / m50_b[v] - 1) * 200 + 50
            sectors[bname] = round(float(scores.mean()), 1)

        sector_avg = round(sum(sectors.values()) / len(sectors), 1) if sectors else 0.0

        entries.append({
            'date':              date.strftime('%Y-%m-%d'),
            'long_breadth_pct':  long_breadth_pct,
            'short_breadth_pct': short_breadth_pct,
            'sector_avg':        sector_avg,
            'long_count':        long_count,
            'short_count':       short_count,
            'sectors':           sectors,
            'estimated':         True,
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

        # sector_avg offset
        r_avg = r.get('sector_avg', 0)
        e_avg = e.get('sector_avg', 0)
        if r_avg != 0 and e_avg != 0:
            sector_avg_diffs.append(r_avg - e_avg)

    long_scale  = round(safe_mean(long_scales),  4)
    short_scale = round(safe_mean(short_scales), 4)

    per_basket_offset = {b: round(sum(d) / len(d), 2) for b, d in basket_diffs.items() if d}
    global_sec_offset = round(sum(sector_avg_diffs) / len(sector_avg_diffs), 2) \
                        if sector_avg_diffs else 0.0

    print(f'  Calibration ({len(overlap)} overlap dates):')
    print(f'    long_scale={long_scale:.3f}  short_scale={short_scale:.3f}')
    print(f'    sector global_offset={global_sec_offset:+.1f}  '
          f'per-basket offsets computed for {len(per_basket_offset)} baskets')

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
    print(f'  {len(tickers):,} tickers  (sector scores come from real CSV runs only)')

    # ── 4. Download price history ─────────────────────────────────────────────
    print('\nDownloading price history (may take several minutes for ~6000 tickers)…')
    try:
        close_df = download_price_history(tickers, period='6mo')
    except Exception as e:
        print(f'\nERROR downloading prices: {e}')
        sys.exit(1)

    # ── 5. Compute backfill entries ───────────────────────────────────────────
    print(f'\nComputing {args.days}-day backfill…')
    try:
        backfill_entries = compute_backfill_entries(
            close_df, days=args.days)
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

    # ── 8. Merge: real entries take priority over estimates ───────────────────
    merged: dict = {}

    # Real screener entries always win
    for h in existing_history:
        if not h.get('estimated'):
            merged[h['date']] = h

    # Add calibrated estimates only for dates without real data
    added = 0
    for e in calibrated_entries:
        if e['date'] not in merged:
            merged[e['date']] = e
            added += 1

    final_history = sorted(merged.values(), key=lambda x: x['date'])

    # ── 9. Save ───────────────────────────────────────────────────────────────
    output = {
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'history':   final_history,
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
