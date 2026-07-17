"""
update_archive.py — BlackRat Archive Builder
Scans BlackRat_YYYY-MM-DD.html reports from Reports/, copies them to Archive/reports/,
reads the corresponding CSV files for stats, and regenerates Archive/index.html.
Only reports on or after CUTOFF_DATE are included.

Layout: Hero → Latest Report Card → Market Breadth → Sector Performance → Footer
"""

import json
import os
import re
import shutil
import sys
from datetime import datetime

# Ensure Unicode output works on Windows consoles (cp1252 can't encode emoji)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')



CUTOFF_DATE = '2025-03-02'

SECTOR_COLORS = [
    '#00d4aa', '#4a9eff', '#f5a623', '#ff4a6a', '#a855f7',
    '#06b6d4', '#f97316', '#84cc16', '#ec4899', '#eab308',
    '#10b981', '#6366f1', '#f43f5e', '#14b8a6', '#c084fc',
]


# ─────────────────────────────────────────────────────────────────────────────
# Report discovery & file helpers
# ─────────────────────────────────────────────────────────────────────────────

def find_reports():
    """Find all BlackRat_YYYY-MM-DD.html files in BlackRat/Reports/ from CUTOFF_DATE onward."""
    parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    reports_dir = os.path.join(parent, 'Reports')
    pattern = re.compile(r'^BlackRat_(\d{4}-\d{2}-\d{2})\.html$')
    reports = []
    if os.path.isdir(reports_dir):
        for f in os.listdir(reports_dir):
            m = pattern.match(f)
            if m:
                date_str = m.group(1)
                if date_str >= CUTOFF_DATE:
                    src = os.path.join(reports_dir, f)
                    reports.append((date_str, src))
    return sorted(reports, key=lambda x: x[0], reverse=True)  # newest first


def get_csv_stats(date_str):
    """Read the CSV for this date and extract key stats."""
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    date_compact = date_str.replace('-', '')
    csv_path = os.path.join(parent_dir, 'Data', f'screen_results_{date_compact}.csv')
    if not os.path.exists(csv_path):
        csv_path = os.path.join(screener_dir, f'screen_results_{date_compact}.csv')

    stats = {'total_stocks': 0, 'top_ticker': '—', 'top_score': 0.0,
             'long_candidates': 0, 'short_candidates': 0}

    if not os.path.exists(csv_path):
        return stats

    try:
        import pandas as pd
        df = pd.read_csv(csv_path, index_col=0)
        stats['total_stocks'] = len(df)
        if 'Final_Score' in df.columns:
            top_idx = df['Final_Score'].idxmax()
            stats['top_ticker']       = str(top_idx)
            stats['top_score']        = round(float(df.loc[top_idx, 'Final_Score']), 1)
            stats['long_candidates']  = int((df['Final_Score'] >= 75).sum())
        if 'Short_Score' in df.columns:
            stats['short_candidates'] = int((df['Short_Score'] >= 70).sum())
    except Exception:
        pass

    return stats


def find_csv_for_session(date_str):
    """
    Locate the screen_results CSV that corresponds to a given report date.

    The report filename uses the run date (day the screener executed), while
    the CSV filename uses the market session date (last completed NY close),
    which is typically 1 trading day earlier.  For Monday-run screeners the
    session is Friday (-3 days due to weekend).

    Strategy (checked in order):
      1. Exact match: screen_results_YYYYMMDD.csv        (new naming convention)
      2. -1 day:      screen_results_<date-1>.csv        (most common: run next day)
      3. -2 day:      screen_results_<date-2>.csv        (run day+2 after session)
      4. -3 day:      screen_results_<date-3>.csv        (Mon report -> Fri session)
      5. +1 day:      screen_results_<date+1>.csv        (legacy edge case)
      6. Fallback:    latest CSV by filename sort        (with a warning)

    Returns the resolved path, or None if no CSV exists at all.
    """
    from datetime import timedelta
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    data_dir     = os.path.join(parent_dir, 'Data')

    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return None

    # Probe in priority order: most likely offsets first
    for days_offset in [0, -1, -2, -3, 1]:
        candidate_dt  = dt + timedelta(days=days_offset)
        candidate_str = candidate_dt.strftime('%Y%m%d')
        path = os.path.join(data_dir, f'screen_results_{candidate_str}.csv')
        if os.path.exists(path):
            if days_offset != 0:
                print(f'  [i] CSV offset {days_offset:+d}d ({candidate_str}) for report {date_str}')
            return path

    # Fallback: latest CSV
    try:
        csv_files = sorted(
            [f for f in os.listdir(data_dir)
             if f.startswith('screen_results_') and f.endswith('.csv')],
            reverse=True
        )
        if csv_files:
            fallback = os.path.join(data_dir, csv_files[0])
            print(f'  [!] No CSV near {date_str} -- using latest: {csv_files[0]}')
            return fallback
    except Exception:
        pass

    return None




def get_latest_candidates(session_date=None):
    """
    Parse the Long Candidates table from the BlackRat report HTML for
    the given session_date (YYYY-MM-DD string).  When session_date is None
    the most recent report by filename is used (legacy behaviour).

    Returns list: [{'t':ticker,'sector':sector,'s':score7f,'r2':score10,
                    'd1':chg1d,'d7':chg1w,'d30':chg1m}, ...]
    preserving the order from the report (best candidates first).

    Report table columns (0-indexed):
      0:Ticker  1:Score/10  2:7-Factor  3:Price  4:Chg1D%  5:Chg1W%  6:Chg1M%
      7:ATR%    8:Dist21EMA 9:DistATR50SMA  10:Setup Notes
    """
    screener_dir  = os.path.dirname(os.path.abspath(__file__))
    parent_dir    = os.path.dirname(screener_dir)
    reports_dir   = os.path.join(parent_dir, 'Reports')

    # Resolve HTML path for the target session
    if session_date is not None:
        html_path = os.path.join(reports_dir, f'BlackRat_{session_date}.html')
        if not os.path.exists(html_path):
            print(f'  ⚠ get_latest_candidates: no report for session {session_date}')
            # Fall back to latest
            html_path = None
    else:
        html_path = None

    if html_path is None:
        try:
            pattern = re.compile(r'^BlackRat_(\d{4}-\d{2}-\d{2})\.html$')
            html_files = sorted(
                [f for f in os.listdir(reports_dir) if pattern.match(f)],
                reverse=True
            )
        except Exception:
            return []
        if not html_files:
            return []
        html_path = os.path.join(reports_dir, html_files[0])

    try:
        # Build reverse ticker→basket map for sector lookup
        try:
            from sector_baskets import SECTOR_BASKETS
            ticker_to_basket = {
                t: basket
                for basket, tickers in SECTOR_BASKETS.items()
                for t in tickers
            }
        except Exception:
            ticker_to_basket = {}

        with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
            html = f.read()

        # Locate the candidateTable tbody
        tbl_start = html.find('id="candidateTable"')
        if tbl_start == -1:
            return []
        tbody_start = html.find('<tbody>', tbl_start)
        tbody_end   = html.find('</tbody>', tbody_start)
        if tbody_start == -1 or tbody_end == -1:
            return []
        tbody = html[tbody_start:tbody_end]

        # Parse rows
        result = []
        for row_m in re.finditer(r'<tr>(.*?)</tr>', tbody, re.DOTALL):
            cells_raw = re.findall(r'<td>(.*?)</td>', row_m.group(1), re.DOTALL)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells_raw]
            if len(cells) < 7:
                continue
            ticker = cells[0].strip()
            if not ticker:
                continue

            # Sector: basket lookup first, then try to extract ALL-CAPS word from Setup Notes
            sector = ticker_to_basket.get(ticker, '')
            if not sector and len(cells) > 10:
                notes = cells[10]
                m = re.search(r'\|\s*([A-Z][A-Z &]{2,})\s*\|', notes)
                if m:
                    sector = m.group(1).strip()

            try:
                result.append({
                    't':      ticker,
                    'sector': sector,
                    's':      round(float(cells[2]), 1),   # 7-Factor score
                    'r2':     round(float(cells[1]), 2),   # Score/10 (setup quality)
                    'd1':     round(float(cells[4]), 1),
                    'd7':     round(float(cells[5]), 1),
                    'd30':    round(float(cells[6]), 1),
                })
            except (ValueError, IndexError):
                continue

        return result

    except Exception as e:
        print(f'  ⚠ get_latest_candidates error: {e}')
        return []


def get_latest_basket_top10(session_date=None, mode='long'):
    """
    Read the screen_results CSV for the given session_date and return
    per-basket top-10 stocks.  When session_date is None, the most recent
    CSV by filename is used (legacy behaviour).

    mode='long'  → sort by Final_Score  desc (best longs first)
    mode='short' → sort by Short_Score  desc (best shorts = weakest stocks first)

    Returns dict: {basket_name: [{'t':ticker,'s':score,'r2':r2,
                                   'd1':chg1d,'d7':chg1w,'d30':chg1m}, ...]}
    """
    # Resolve CSV path for the target session
    if session_date is not None:
        csv_path = find_csv_for_session(session_date)
        if csv_path is None:
            print(f'  ⚠ get_latest_basket_top10: no CSV for session {session_date}')
            return {}
    else:
        screener_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir   = os.path.dirname(screener_dir)
        data_dir     = os.path.join(parent_dir, 'Data')
        try:
            csv_files = sorted(
                [f for f in os.listdir(data_dir)
                 if f.startswith('screen_results_') and f.endswith('.csv')],
                reverse=True
            )
        except Exception:
            return {}
        if not csv_files:
            return {}
        csv_path = os.path.join(data_dir, csv_files[0])

    try:
        import pandas as pd
        from sector_baskets import SECTOR_BASKETS

        df        = pd.read_csv(csv_path, index_col=0)
        score_col = 'Final_Score' if mode == 'long' else 'Short_Score'
        required  = {score_col, 'r_squared', '1d_return', '1w_return', '1m_return'}
        if not required.issubset(df.columns):
            return {}

        # ATR%/ADR% volatility filter threshold (minimum to appear in top-10 sparklines)
        VOLATILITY_MIN = 0.025   # 2.5 %

        result = {}
        for basket_name, tickers in SECTOR_BASKETS.items():
            sub = df[df.index.isin(tickers)].copy()
            if sub.empty:
                continue
            # Apply ATR%/ADR% >= 2.9% filter if columns are present
            if 'atr_pct' in sub.columns or 'adr_pct' in sub.columns:
                atr_ok  = sub.get('atr_pct', pd.Series(0, index=sub.index)) >= VOLATILITY_MIN
                adr_ok  = sub.get('adr_pct', pd.Series(0, index=sub.index)) >= VOLATILITY_MIN
                vol_mask = atr_ok | adr_ok
                filtered = sub[vol_mask]
                # Fall back to unfiltered if no stocks pass the filter
                sub = filtered if not filtered.empty else sub
            sub = sub.sort_values(score_col, ascending=False).head(10)
            stocks = []
            for ticker, row in zip(sub.index, sub.to_dict(orient="records")):
                stocks.append({
                    't':   str(ticker),
                    's':   round(float(row[score_col]), 1),
                    'r2':  round(float(row['r_squared']), 2),
                    'd1':  round(float(row['1d_return']) * 100, 1),
                    'd7':  round(float(row['1w_return']) * 100, 1),
                    'd30': round(float(row['1m_return']) * 100, 1),
                })
            result[basket_name] = stocks
        return result
    except Exception as e:
        print(f'  ⚠ get_latest_basket_top10 error: {e}')
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Market score history
# ─────────────────────────────────────────────────────────────────────────────

def compute_market_scores(date_str):
    """
    Compute market-wide breadth and per-sector average scores.
    Uses find_csv_for_session() to locate the CSV (supports ±1 day offset).

    Long-Term Breadth (7-factor + MA50 structural confirmation):
      long_breadth_pct  = % stocks with Final_Score >= 65 AND close > MA50
      short_breadth_pct = % stocks with Short_Score >= 65 AND close < MA20
      The dual condition integrates the 7-factor model with structural trend
      confirmation, reducing volatility-driven noise.

    Short-Term Momentum (st_long_pct / st_short_pct):
      Computed by backfill from full price history (EMA10 + SMA8/SMA20).
      Supplemented into live entries via merge logic.

    Returns a dict or None if CSV not found / missing required columns.
    """
    csv_path = find_csv_for_session(date_str)
    if not csv_path:
        return None

    LONG_THRESHOLD  = 65   # 7-factor score threshold (integrated with MA50)
    SHORT_THRESHOLD = 65   # Short score threshold (integrated with MA20)

    try:
        import pandas as pd
        df = pd.read_csv(csv_path, index_col=0)
        if 'Final_Score' not in df.columns:
            return None

        n = len(df)
        has_price_cols = all(c in df.columns for c in ['last_price', 'ma20', 'ma50'])

        # ── Long-Term breadth: 7-factor >= 65 + MA confirmation ────────────
        if has_price_cols:
            valid = df['last_price'].notna() & df['ma50'].notna() & df['ma20'].notna() & (df['ma50'] > 0)
            lp  = df.loc[valid, 'last_price']
            m50 = df.loc[valid, 'ma50']
            m20 = df.loc[valid, 'ma20']
            fs  = df.loc[valid, 'Final_Score']

            long_mask  = (fs >= LONG_THRESHOLD) & (lp > m50)
            long_breadth_pct = round(float(long_mask.sum()) / len(valid) * 100, 1) if len(valid) > 0 else 0.0
            long_count       = int(long_mask.sum())

            short_breadth_pct = 0.0
            short_count       = 0
            if 'Short_Score' in df.columns:
                ss = df.loc[valid, 'Short_Score']
                short_mask = (ss >= SHORT_THRESHOLD) & (lp < m20)
                short_breadth_pct = round(float(short_mask.sum()) / len(valid) * 100, 1) if len(valid) > 0 else 0.0
                short_count       = int(short_mask.sum())
        else:
            # Fallback: score-only (no price columns)
            long_breadth_pct  = round(float((df['Final_Score'] >= LONG_THRESHOLD).sum()) / n * 100, 1)
            long_count        = int((df['Final_Score'] >= LONG_THRESHOLD).sum())
            short_breadth_pct = 0.0
            short_count       = 0
            if 'Short_Score' in df.columns:
                short_breadth_pct = round(float((df['Short_Score'] >= SHORT_THRESHOLD).sum()) / n * 100, 1)
                short_count       = int((df['Short_Score'] >= SHORT_THRESHOLD).sum())

        # ── Sector scores ──────────────────────────────────────────────────
        from sector_baskets import SECTOR_BASKETS
        sectors       = {}
        sectors_short = {}
        sector_avg    = 0.0
        for basket_name, basket_tickers in SECTOR_BASKETS.items():
            present = df[df.index.isin(basket_tickers)]
            if len(present) >= 2:
                avg_score = present['Final_Score'].mean()
                sectors[basket_name] = round(float(avg_score), 1)
                if 'Short_Score' in df.columns:
                    avg_short = present['Short_Score'].mean()
                    sectors_short[basket_name] = round(float(avg_short), 1)
        if sectors:
            sector_avg = round(sum(sectors.values()) / len(sectors), 1)

        # ── Short-Term momentum: compute directly from CSV (Final_Score + ma10/ma20)
        st_long_pct  = 0.0
        st_short_pct = 0.0
        ST_THRESHOLD = 65.0
        if has_price_cols and 'Short_Score' in df.columns:
            valid_st = (df['ma10'].notna() & df['ma20'].notna() &
                        df['Final_Score'].notna() & df['Short_Score'].notna() &
                        (df['ma10'] > 0) & (df['ma20'] > 0))
            n_st = int(valid_st.sum())
            if n_st >= 200:
                vdf = df[valid_st]
                st_long_mask  = (vdf['Final_Score'] >= ST_THRESHOLD) & (vdf['ma10'] > vdf['ma20'])
                st_short_mask = (vdf['Short_Score'] >= ST_THRESHOLD) & (vdf['ma10'] < vdf['ma20'])
                st_long_pct  = round(float(st_long_mask.sum())  / n_st * 100, 1)
                st_short_pct = round(float(st_short_mask.sum()) / n_st * 100, 1)

        result = {
            'date':              date_str,
            'long_breadth_pct':  long_breadth_pct,
            'short_breadth_pct': short_breadth_pct,
            'st_long_pct':       st_long_pct,
            'st_short_pct':      st_short_pct,
            'sector_avg':        sector_avg,
            'long_count':        long_count,
            'short_count':       short_count,
            'sectors':           sectors,
            'sectors_short':     sectors_short,
        }
        return result

    except Exception as e:
        print(f'  ⚠ compute_market_scores error for {date_str}: {e}')
        return None


def load_market_history(archive_dir):
    """Load existing market score history; return list."""
    history_path = os.path.join(archive_dir, 'market_score_history.json')
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('history', [])
        except Exception:
            pass
    return []


def load_macro_history(archive_dir):
    """Load full 1-year macro ratio series; return list sorted by date."""
    history_path = os.path.join(archive_dir, 'market_score_history.json')
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return sorted(data.get('macro_history', []), key=lambda x: x['date'])
        except Exception:
            pass
    return []


def save_market_history(archive_dir, history):
    """Persist market score history to Archive/market_score_history.json.
    Preserves existing macro_history key if already present."""
    history_path = os.path.join(archive_dir, 'market_score_history.json')
    # Preserve macro_history from existing file
    existing_macro = []
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                existing = json.load(f)
            existing_macro = existing.get('macro_history', [])
        except Exception:
            pass
    data = {
        'generated':    datetime.now().strftime('%Y-%m-%d %H:%M'),
        'history':      sorted(history, key=lambda x: x['date']),
        'macro_history': existing_macro,
    }
    with open(history_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# HTML helpers
# ─────────────────────────────────────────────────────────────────────────────

def format_date_display(date_str):
    """'2026-02-26' → 'Thursday, Feb 26, 2026'"""
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%A, %b %d, %Y')
    except Exception:
        return date_str


def make_sparkline_svg(values, width=92, height=36):
    """
    Server-side SVG sparkline from a list of floats (may contain None for gaps).
    Points are x-positioned proportionally to their index in the full values list.
    Color: green if last valid > first valid (uptrend), red otherwise.
    """
    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"></svg>'

    lo   = min(valid)
    hi   = max(valid)
    span = hi - lo if hi > lo else 1.0
    n    = len(values)

    pts = []
    for i, v in enumerate(values):
        if v is None:
            continue
        x = round((i / max(n - 1, 1)) * (width - 8) + 4, 1)
        y = round((1.0 - (v - lo) / span) * (height - 10) + 5, 1)
        pts.append((x, y))

    if len(pts) < 2:
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"></svg>'

    uptrend    = valid[-1] >= valid[0]
    color      = '#00d4aa' if uptrend else '#ff4a6a'
    fill_color = 'rgba(0,212,170,0.13)' if uptrend else 'rgba(255,74,106,0.10)'

    polyline_pts = ' '.join(f'{x},{y}' for x, y in pts)
    fill_pts     = f'{pts[0][0]},{height} {polyline_pts} {pts[-1][0]},{height}'

    return (
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {height}">'
        f'<polygon points="{fill_pts}" fill="{fill_color}" stroke="none"/>'
        f'<polyline points="{polyline_pts}" fill="none" stroke="{color}" '
        f'stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>'
        f'<circle cx="{pts[-1][0]}" cy="{pts[-1][1]}" r="2.2" fill="{color}"/>'
        f'</svg>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Enhanced sector sparkline with MA10 + reference lines at 50 / 70
# ─────────────────────────────────────────────────────────────────────────────

def make_sector_spark_svg(values, width=160, height=52):
    """
    Enhanced sector sparkline:
      - Area fill + main score line (green if above MA10, red if below)
      - MA10 dashed overlay
      - Dashed reference lines at score=70 (green) and score=50 (orange)
      - Faint background band for the "strength zone" above 70
    """
    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"></svg>'

    # Fixed scale so 50/70 reference lines are always in a consistent position
    lo    = min(min(valid), 28)
    hi    = max(max(valid), 82)
    span  = hi - lo if hi > lo else 1.0
    n     = len(values)

    pad_l, pad_r, pad_t, pad_b = 3, 3, 4, 4

    def ypx(v):
        return round((1.0 - (v - lo) / span) * (height - pad_t - pad_b) + pad_t, 1)

    def xpx(i):
        return round((i / max(n - 1, 1)) * (width - pad_l - pad_r) + pad_l, 1)

    # Build score point list
    pts = []
    for i, v in enumerate(values):
        if v is None:
            continue
        pts.append((xpx(i), ypx(v)))

    if len(pts) < 2:
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"></svg>'

    # Build MA10 overlay
    ma10_pts = []
    for i, v in enumerate(values):
        if v is None:
            continue
        window = [v2 for v2 in values[max(0, i - 9):i + 1] if v2 is not None]
        ma     = sum(window) / len(window)
        ma10_pts.append((xpx(i), ypx(ma)))

    # Color: green if last value is >= MA10, red otherwise
    last_ma10 = sum(v for v in values[-10:] if v is not None) / max(1, sum(1 for v in values[-10:] if v is not None))
    above_ma  = valid[-1] >= last_ma10
    color      = '#00d4aa' if above_ma else '#ff4a6a'
    fill_color = 'rgba(0,212,170,0.11)' if above_ma else 'rgba(255,74,106,0.09)'

    # Reference Y positions
    y70 = ypx(70)
    y50 = ypx(50)

    polyline_pts = ' '.join(f'{x},{y}' for x, y in pts)
    fill_pts     = f'{pts[0][0]},{height - pad_b} {polyline_pts} {pts[-1][0]},{height - pad_b}'
    ma10_poly    = ' '.join(f'{x},{y}' for x, y in ma10_pts)

    svg  = (f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {width} {height}">')
    # Strength zone background (above 70)
    svg += (f'<rect x="{pad_l}" y="{pad_t}" width="{width-pad_l-pad_r}" '
            f'height="{max(0, y70-pad_t):.1f}" fill="rgba(0,212,170,0.04)"/>')
    # Reference line at 70
    svg += (f'<line x1="{pad_l}" y1="{y70}" x2="{width-pad_r}" y2="{y70}" '
            f'stroke="rgba(0,212,170,0.30)" stroke-width="0.7" stroke-dasharray="3,3"/>')
    # Reference line at 50
    svg += (f'<line x1="{pad_l}" y1="{y50}" x2="{width-pad_r}" y2="{y50}" '
            f'stroke="rgba(245,166,35,0.28)" stroke-width="0.7" stroke-dasharray="3,3"/>')
    # Area fill
    svg += f'<polygon points="{fill_pts}" fill="{fill_color}" stroke="none"/>'
    # MA10 dashed line
    svg += (f'<polyline points="{ma10_poly}" fill="none" stroke="rgba(255,255,255,0.22)" '
            f'stroke-width="1.0" stroke-dasharray="3,2"/>')
    # Main score line
    svg += (f'<polyline points="{polyline_pts}" fill="none" stroke="{color}" '
            f'stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>')
    # End dot
    svg += f'<circle cx="{pts[-1][0]}" cy="{pts[-1][1]}" r="2.4" fill="{color}"/>'
    svg += '</svg>'
    return svg


def _sector_signal(score, d5, above_ma10):
    """Return (label, color) signal classification for a sector."""
    if score >= 70:
        if d5 >= 0:   return 'LEADING',  '#00d4aa'
        else:         return 'FADING',   '#f5a623'
    elif score >= 55:
        if d5 > 2:    return 'BUILDING', '#4a9eff'
        elif d5 < -3: return 'SLIPPING', '#f97316'
        else:         return 'HOLDING',  '#606078'
    elif score >= 40:
        if d5 > 3:    return 'RECOVERY', '#a78bfa'
        else:         return 'WEAK',     '#ff4a6a'
    else:
        if d5 > 3:    return 'RECOVERY', '#a78bfa'
        else:         return 'BEARISH',  '#ff4a6a'


# ─────────────────────────────────────────────────────────────────────────────
# ETF Equal-Weight sparkline SVG + section builder
# ─────────────────────────────────────────────────────────────────────────────

def make_sector_etf_spark_svg(values, width=200, height=100):
    """
    Equal-weight NAV sparkline for the ETF Equal-Weight sub-section.
      - Main NAV line: theme-aware via CSS var(--spark-main)
        → Dark mode: white (#f5f4ef)  |  Light mode: dark gray (#1a1915)
      - 21 EMA: solid blue (#4a9eff)
      - 30 SMA: solid orange (#cc6b45)
      - 50 SMA: solid fuchsia (#ec4899)
      - No fill, no reference lines (clean NAV chart style)
      - Default size 200×100 (2:1 ratio)

    Args:
        values: list of float|None — equal-weight score series (per-basket daily values)
        width: SVG width in px
        height: SVG height in px
    Returns:
        SVG string
    """
    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"></svg>'

    lo    = min(valid) - 1.0
    hi    = max(valid) + 1.0
    span  = hi - lo if hi > lo else 1.0
    n     = len(values)

    pad_l, pad_r, pad_t, pad_b = 4, 4, 6, 6

    def ypx(v):
        return round((1.0 - (v - lo) / span) * (height - pad_t - pad_b) + pad_t, 1)

    def xpx(i):
        return round((i / max(n - 1, 1)) * (width - pad_l - pad_r) + pad_l, 1)

    pts = []
    for i, v in enumerate(values):
        if v is None:
            continue
        pts.append((xpx(i), ypx(v)))

    if len(pts) < 2:
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"></svg>'

    # ── EMA21 — exponential moving average ────────────────────────────────
    k21 = 2.0 / (21 + 1)
    ema21_pts = []
    ema21_val = None
    for i, v in enumerate(values):
        if v is None:
            ema21_pts.append(None)
            continue
        if ema21_val is None:
            ema21_val = v
        else:
            ema21_val = v * k21 + ema21_val * (1 - k21)
        ema21_pts.append((xpx(i), ypx(ema21_val)))

    # ── SMA30 — simple moving average ─────────────────────────────────────
    sma30_pts = []
    for i, v in enumerate(values):
        if v is None:
            sma30_pts.append(None)
            continue
        window = [v2 for v2 in values[max(0, i - 29):i + 1] if v2 is not None]
        if len(window) >= 15:
            sma30_pts.append((xpx(i), ypx(sum(window) / len(window))))
        else:
            sma30_pts.append(None)

    # ── SMA50 — simple moving average ─────────────────────────────────────
    sma50_pts = []
    for i, v in enumerate(values):
        if v is None:
            sma50_pts.append(None)
            continue
        window = [v2 for v2 in values[max(0, i - 49):i + 1] if v2 is not None]
        if len(window) >= 25:
            sma50_pts.append((xpx(i), ypx(sum(window) / len(window))))
        else:
            sma50_pts.append(None)

    # Rebuild clean polylines (skip None segments)
    polyline_pts    = ' '.join(f'{x},{y}' for x, y in pts)
    ema21_poly_pts  = [(x, y) for entry in ema21_pts if entry is not None for x, y in [entry]]
    sma30_poly_pts  = [(x, y) for entry in sma30_pts if entry is not None for x, y in [entry]]
    sma50_poly_pts  = [(x, y) for entry in sma50_pts if entry is not None for x, y in [entry]]
    ema21_poly_str  = ' '.join(f'{x},{y}' for x, y in ema21_poly_pts)
    sma30_poly_str  = ' '.join(f'{x},{y}' for x, y in sma30_poly_pts)
    sma50_poly_str  = ' '.join(f'{x},{y}' for x, y in sma50_poly_pts)

    svg  = (f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {width} {height}">')
    # SMA50 fuchsia (back layer)
    if sma50_poly_str:
        svg += (f'<polyline points="{sma50_poly_str}" fill="none" stroke="#ec4899" '
                f'stroke-opacity="0.80" stroke-width="1.0"/>')
    # SMA30 orange
    if sma30_poly_str:
        svg += (f'<polyline points="{sma30_poly_str}" fill="none" stroke="#cc6b45" '
                f'stroke-opacity="0.80" stroke-width="1.0"/>')
    # EMA21 blue
    if ema21_poly_str:
        svg += (f'<polyline points="{ema21_poly_str}" fill="none" stroke="#4a9eff" '
                f'stroke-opacity="0.85" stroke-width="1.0"/>')
    # Main NAV line — theme-aware via CSS var(--spark-main)
    svg += (f'<polyline points="{polyline_pts}" fill="none" stroke="var(--spark-main)" '
            f'stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>')
    # End dot
    svg += f'<circle cx="{pts[-1][0]}" cy="{pts[-1][1]}" r="2.8" fill="var(--spark-main)"/>'
    svg += '</svg>'
    return svg


def build_sector_etf_html(market_history, session_date=None,
                          top10_data=None, top10_short_data=None):
    """
    ETF Equal-Weight sub-section — one card per sector basket.
    Shows cumulative equal-weight NAV sparkline (using sector score series)
    with EMA21 (dashed blue) + SMA30 (dashed muted) overlays.
    Line color above EMA21 = green, below = red.

    Sorting: primary sort by EMA21 vs SMA30 position (EMA21 > SMA30 first),
    secondary sort by score descending within each group.

    Args:
        market_history: list of market score history dicts with 'sectors' key.
        session_date: optional YYYY-MM-DD cap for history entries.
        top10_data: dict {basket_name: [stock dicts]} for long top-10.
        top10_short_data: dict {basket_name: [stock dicts]} for short top-10.
    Returns:
        HTML string for the ETF Equal-Weight sub-section cards.
    """
    if not market_history:
        return ''

    history = sorted(market_history, key=lambda x: x['date'])
    if session_date:
        history = [h for h in history if h['date'] <= session_date]
    if not history:
        return ''

    N_SPARK = 63
    latest = next(
        (h for h in reversed(history) if h.get('sectors')),
        history[-1]
    )
    latest_sectors = latest.get('sectors', {})
    if not latest_sectors:
        return ''

    # ── Per-sector ETF NAV stats ───────────────────────────────────────────────
    etf_stats = []
    for sec_name, cur_score in latest_sectors.items():
        values_all = [h.get('sectors', {}).get(sec_name) for h in history]
        values = values_all[-N_SPARK:] if len(values_all) > N_SPARK else values_all
        valid  = [v for v in values if v is not None]
        if len(valid) < 5:
            continue

        # EMA21 of the score series (last value)
        k = 2.0 / (21 + 1)
        ema_val = None
        for v in valid:
            if ema_val is None:
                ema_val = v
            else:
                ema_val = v * k + ema_val * (1 - k)
        ema21_last = round(ema_val, 1) if ema_val is not None else cur_score

        # SMA30 (last value)
        sma30_win  = valid[max(0, len(valid) - 30):]
        sma30_last = round(sum(sma30_win) / len(sma30_win), 1) if len(sma30_win) >= 15 else None

        # SMA50 (last value)
        sma50_win  = valid[max(0, len(valid) - 50):]
        sma50_last = round(sum(sma50_win) / len(sma50_win), 1) if len(sma50_win) >= 25 else None

        # 5-day and 20-day momentum
        d5  = round(valid[-1] - (valid[-6]  if len(valid) >= 6  else valid[0]), 1)
        d20 = round(valid[-1] - (valid[-21] if len(valid) >= 21 else valid[0]), 1)

        # Stage vs EMA21
        above_ema21 = cur_score >= ema21_last
        ema_gap     = round(cur_score - ema21_last, 1)

        # EMA21 vs SMA30 cross status (primary sort key)
        if sma30_last is not None:
            ema_above_sma = ema21_last > sma30_last
            ema_sma_gap = round(ema21_last - sma30_last, 1)
        else:
            ema_above_sma = above_ema21  # fallback
            ema_sma_gap = 0.0

        # Score vs SMA30 and SMA50 (for MA filters)
        above_sma30 = cur_score >= sma30_last if sma30_last is not None else None
        above_sma50 = cur_score >= sma50_last if sma50_last is not None else None

        sig_label, sig_color = _sector_signal(cur_score, d5, above_ema21)

        etf_stats.append({
            'name':           sec_name,
            'score':          cur_score,
            'd5':             d5,
            'd20':            d20,
            'ema21':          ema21_last,
            'sma30':          sma30_last,
            'sma50':          sma50_last,
            'above_ema21':    above_ema21,
            'above_sma30':    above_sma30,
            'above_sma50':    above_sma50,
            'ema_gap':        ema_gap,
            'ema_above_sma':  ema_above_sma,
            'ema_sma_gap':    ema_sma_gap,
            'sig_label':      sig_label,
            'sig_color':      sig_color,
            'values':         values,
        })

    # ── SORTING: by EMA21−SMA30 distance, most positive first ────────────
    # Baskets without SMA30 (insufficient data) go to the bottom.
    etf_stats.sort(key=lambda x: -x['ema_sma_gap'] if x['sma30'] is not None else 999)

    grid_html = ''

    for s in etf_stats:
        score         = s['score']
        d5            = s['d5']
        d20           = s['d20']
        ema21         = s['ema21']
        sma30         = s['sma30']
        sma50         = s['sma50']
        ema_gap       = s['ema_gap']
        sig_label     = s['sig_label']
        sig_color     = s['sig_color']
        above_ema21   = s['above_ema21']
        above_sma30   = s['above_sma30']
        above_sma50   = s['above_sma50']
        ema_above_sma = s['ema_above_sma']

        if score >= 70:   score_color = '#00d4aa'
        elif score >= 55: score_color = '#4a9eff'
        elif score >= 40: score_color = '#f5a623'
        else:             score_color = '#ff4a6a'

        d5_str   = f'+{d5}' if d5 > 0 else str(d5)
        d5_col   = '#00d4aa' if d5 > 0.5 else ('#ff4a6a' if d5 < -0.5 else 'var(--text-muted)')
        d20_str  = f'+{d20}' if d20 > 0 else str(d20)
        d20_col  = '#00d4aa' if d20 > 1 else ('#ff4a6a' if d20 < -1 else 'var(--text-muted)')

        ema_tag_label = f'{"▲" if above_ema21 else "▼"} EMA21'
        ema_tag_color = '#00d4aa' if above_ema21 else '#ff4a6a'
        ema_gap_str   = f'+{ema_gap}' if ema_gap >= 0 else str(ema_gap)

        sma30_str = f'{sma30:.1f}' if sma30 is not None else '—'

        sig_r = int(sig_color[1:3], 16)
        sig_g = int(sig_color[3:5], 16)
        sig_b = int(sig_color[5:7], 16)
        sig_bg = f'rgba({sig_r},{sig_g},{sig_b},0.14)'

        disp_name = s['name'] if len(s['name']) <= 24 else s['name'][:22] + '…'
        spark_svg = make_sector_etf_spark_svg(s['values'])

        top10_list       = (top10_data or {}).get(s['name'], [])
        top10_attr       = json.dumps(top10_list).replace('"', '&quot;')
        top10_short_list = (top10_short_data or {}).get(s['name'], [])
        top10_short_attr = json.dumps(top10_short_list).replace('"', '&quot;')

        # EMA−SMA distance indicator (drives sort order)
        ema_sma_gap = s['ema_sma_gap']
        if sma30 is not None:
            gap_str   = f'+{ema_sma_gap}' if ema_sma_gap >= 0 else str(ema_sma_gap)
            gap_color = '#00d4aa' if ema_sma_gap > 0 else ('#ff4a6a' if ema_sma_gap < 0 else 'var(--text-muted)')
        else:
            gap_str   = '—'
            gap_color = 'var(--text-muted)'

        # Data attributes for MA filtering
        da_ema21 = '1' if above_ema21 else '0'
        da_sma30 = '1' if above_sma30 else '0'
        da_sma50 = '1' if above_sma50 else '0'

        grid_html += f"""
    <div class="sec-etf-card" style="border-left-color:{sig_color}"
         data-top10="{top10_attr}" data-top10short="{top10_short_attr}"
         data-above-ema21="{da_ema21}" data-above-sma30="{da_sma30}" data-above-sma50="{da_sma50}"
         onclick="openSectorPanel(this)">
      <div class="sec-etf-card-header">
        <div class="sec-etf-name sec-name" title="{s['name']}">{disp_name}</div>
        <div class="sec-etf-badge" style="background:{sig_bg};color:{sig_color}">{sig_label}</div>
      </div>
      <div class="sec-etf-metrics">
        <div class="sec-score" style="color:{score_color}">{score}</div>
        <div class="sec-etf-ma-tag" style="color:{ema_tag_color}">{ema_tag_label}
          <span class="sec-ma-gap">({ema_gap_str})</span>
        </div>
      </div>
      <div class="sec-etf-spark">{spark_svg}</div>
      <div class="sec-etf-footer">
        <span class="sec-stat" style="color:{d5_col}">{d5_str}</span>
        <span class="sec-stat" style="color:{gap_color};font-size:.55rem">E-S {gap_str}</span>
        <span class="sec-stat sec-stat-muted">SMA30 {sma30_str}</span>
        <span class="sec-stat" style="color:{d20_col}">20d {d20_str}</span>
      </div>
    </div>"""

    return f"""
<!-- ── Sector ETF Equal-Weight Grid ────────────────────────────────── -->
<div class="sec-etf-legend">
  <div class="sec-etf-legend-item">
    <span class="sec-etf-legend-line" style="background:var(--spark-main)"></span> Price
  </div>
  <div class="sec-etf-legend-item">
    <span class="sec-etf-legend-line" style="background:#4a9eff"></span> 21 EMA
  </div>
  <div class="sec-etf-legend-item">
    <span class="sec-etf-legend-line" style="background:#cc6b45"></span> 30 SMA
  </div>
  <div class="sec-etf-legend-item">
    <span class="sec-etf-legend-line" style="background:#ec4899"></span> 50 SMA
  </div>
</div>
<div class="sec-etf-filter-bar">
  <span class="sc-sort-label">Filter MA:</span>
  <button class="sc-sort-btn active" onclick="filterEtfMA('all',this)">All</button>
  <button class="sc-sort-btn" onclick="filterEtfMA('ema21',this)">Above 21 EMA</button>
  <button class="sc-sort-btn" onclick="filterEtfMA('sma30',this)">Above 30 SMA</button>
  <button class="sc-sort-btn" onclick="filterEtfMA('sma50',this)">Above 50 SMA</button>
</div>
<div class="sec-etf-grid" id="secEtfGrid">
{grid_html}
</div>
<!-- ── /Sector ETF Equal-Weight Grid ─────────────────────────────── -->"""


# ─────────────────────────────────────────────────────────────────────────────
# Section builders
# ─────────────────────────────────────────────────────────────────────────────

def build_latest_report_html(reports_with_stats):
    """Prominent 'Latest Report' card positioned at the top of the page."""
    if not reports_with_stats:
        return ''

    date_str, stats = reports_with_stats[0]
    date_display = format_date_display(date_str)
    total_str    = f"{stats['total_stocks']:,}" if stats['total_stocks'] else '—'
    score_str    = str(stats['top_score']) if stats['top_score'] else '—'

    return f"""
<!-- ── Latest Report ─────────────────────────────────────────────────── -->
<div class="lr-wrap">
  <div class="lr-header-row">
    <div class="lr-section-title">Latest Report</div>
  </div>
  <a href="reports/{date_str}.html" class="lr-card">
    <div class="lr-card-top">
      <div class="lr-live-badge">&#9679; LIVE</div>
      <div class="lr-date">{date_display}</div>
      <div class="lr-arrow">&#8594;</div>
    </div>
  </a>
</div>
<!-- ── /Latest Report ────────────────────────────────────────────────── -->
"""


def build_breadth_html(market_history, macro_history=None, session_date=None):
    """
    Market Breadth section: 4 stats (Bias, Long%, Short%, Sector Avg)
    plus a full-width breadth trend chart.
    macro_history: full 1-year ratio series (own date axis for macro charts).
    session_date: when provided, all history and macro_history entries with
                  date > session_date are excluded so charts are always
                  anchored to the same date as the daily report.
    """
    if not market_history:
        return ''

    history = sorted(market_history, key=lambda x: x['date'])
    # Cap history to session_date so backfill estimated forward-day entries
    # do not shift charts beyond the date of the latest daily report.
    if session_date:
        history = [h for h in history if h['date'] <= session_date]
    if not history:
        return ''
    # Breadth trend chart shows the same 63-day window as the macro ratio charts.
    history = history[-63:] if len(history) > 63 else history

    # Use full macro_history if available, else fall back to per-entry values.
    # NOTE: do NOT filter mh by session_date here — the full series (up to 252 days)
    # is needed so SMA10/20/30/50 are fully warmed up across the entire 63-day
    # display window.  The display cap is applied to macro_display_entries below.
    mh = sorted(macro_history, key=lambda x: x['date']) if macro_history else []

    display_dates = []
    for d in [h['date'] for h in history]:
        try:
            display_dates.append(datetime.strptime(d, '%Y-%m-%d').strftime('%b %d'))
        except Exception:
            display_dates.append(d)

    long_breadth  = [h['long_breadth_pct']  for h in history]
    short_breadth = [h['short_breadth_pct'] for h in history]

    # Short-term momentum series
    st_long  = [h.get('st_long_pct', 0)  for h in history]
    st_short = [h.get('st_short_pct', 0) for h in history]

    latest = history[-1]
    l_pct  = latest['long_breadth_pct']
    s_pct  = latest['short_breadth_pct']
    s_avg  = latest.get('sector_avg', 0.0)

    def delta_fmt(cur, prev):
        d = round(cur - prev, 1)
        if d > 0:  return f'+{d}', '#00d4aa'
        if d < 0:  return str(d),  '#ff4a6a'
        return '—', '#505068'

    if len(history) >= 2:
        prev = history[-2]
        l_dstr, l_dcol = delta_fmt(l_pct, prev['long_breadth_pct'])
        s_dstr, s_dcol = delta_fmt(s_pct, prev['short_breadth_pct'])
        a_dstr, a_dcol = delta_fmt(s_avg,  prev.get('sector_avg', s_avg))
    else:
        l_dstr = s_dstr = a_dstr = '—'
        l_dcol = s_dcol = a_dcol = '#505068'

    # ── Spread series ──────────────────────────────────────────────────────
    spreads = [round(h['long_breadth_pct'] - h['short_breadth_pct'], 2) for h in history]
    spread_val = spreads[-1]

    # Spread delta (vs previous day)
    if len(spreads) >= 2:
        sp_dstr, sp_dcol = delta_fmt(spread_val, spreads[-2])
    else:
        sp_dstr, sp_dcol = '—', '#505068'

    # Spread 5-day momentum
    spread_5d_ago = spreads[-6] if len(spreads) >= 6 else spreads[0]
    spread_mom    = round(spread_val - spread_5d_ago, 1)
    sp_mom_str    = f'+{spread_mom}' if spread_mom > 0 else str(spread_mom)

    ratio = l_pct / s_pct if s_pct > 0 else 99

    # Regime classification (composite of spread + ratio)
    if spread_val > 8 and ratio > 2.0:
        regime_label, regime_color, regime_sub = 'RISK-ON',  '#00d4aa', 'Strong long bias'
    elif spread_val > 5 and ratio > 1.5:
        regime_label, regime_color, regime_sub = 'BULLISH',  '#4a9eff', 'Moderate long bias'
    elif spread_val > 2:
        regime_label, regime_color, regime_sub = 'NEUTRAL',  '#f5a623', 'Cautious / mixed'
    elif spread_val >= 0:
        regime_label, regime_color, regime_sub = 'CAUTION',  '#f97316', 'Reduce exposure'
    else:
        regime_label, regime_color, regime_sub = 'RISK-OFF', '#ff4a6a', 'Short bias active'

    latest_date_display = format_date_display(latest['date'])

    # ST Spread series
    st_spreads    = [round(a - b, 2) for a, b in zip(st_long, st_short)]
    st_spread_val = st_spreads[-1] if st_spreads else 0.0

    # ST Regime: SMA(5) dello spread — reattiva al breve, meno lag dell'EMA
    _ST_REG_PERIOD = 5
    st_regime = []
    for i in range(len(st_spreads)):
        window = st_spreads[max(0, i - _ST_REG_PERIOD + 1):i + 1]
        st_regime.append(round(sum(window) / len(window), 2))

    st_regime_val = st_regime[-1] if st_regime else 0.0
    st_regime_prev = st_regime[-2] if len(st_regime) >= 2 else st_regime_val
    st_regime_delta = round(st_regime_val - st_regime_prev, 1)
    st_regime_delta_str = (f'+{st_regime_delta}' if st_regime_delta > 0
                           else str(st_regime_delta))

    # Regime label basato su SMA(5) spread
    if st_regime_val > 10:
        st_regime_label, st_regime_col = 'STRONG BULL', '#00d4aa'
    elif st_regime_val > 4:
        st_regime_label, st_regime_col = 'BULLISH',     '#4a9eff'
    elif st_regime_val > -4:
        st_regime_label, st_regime_col = 'NEUTRAL',     '#f5a623'
    elif st_regime_val > -10:
        st_regime_label, st_regime_col = 'BEARISH',     '#ff7850'
    else:
        st_regime_label, st_regime_col = 'STRONG BEAR', '#ff4a6a'

    # ── Macro regime ratios ────────────────────────────────────────────────────
    def _rolling_sma(series, period):
        result = []
        for i in range(len(series)):
            window = [v for v in series[max(0, i - period + 1):i + 1] if v is not None]
            result.append(round(sum(window) / len(window), 6) if window else None)
        return result

    def _ratio_regime(series):
        """Return (score 0-5, sma10_series, sma20_series, sma30_series, sma50_series) for a ratio series."""
        arr = [v for v in series if v is not None]
        sma10_s = _rolling_sma(series, 10)
        sma20_s = _rolling_sma(series, 20)
        sma30_s = _rolling_sma(series, 30)
        sma50_s = _rolling_sma(series, 50)
        if len(arr) < 50:
            return 0, sma10_s, sma20_s, sma30_s, sma50_s, '#f5a623', 'NO DATA'
        sma10 = sum(arr[-10:]) / 10
        sma20 = sum(arr[-20:]) / 20
        sma50 = sum(arr[-50:]) / 50
        # Linear regression over last 21 days
        y = arr[-21:]
        n = len(y)
        x_mean = (n - 1) / 2.0
        y_mean = sum(y) / n
        ss_xy = sum((i - x_mean) * (y[i] - y_mean) for i in range(n))
        ss_xx = sum((i - x_mean) ** 2 for i in range(n))
        slope = ss_xy / ss_xx if ss_xx > 0 else 0.0
        y_hat = [slope * (i - x_mean) + y_mean for i in range(n)]
        ss_res = sum((y[i] - y_hat[i]) ** 2 for i in range(n))
        ss_tot = sum((v - y_mean) ** 2 for v in y)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        score = sum([
            arr[-1] > sma10,
            sma10   > sma20,
            sma20   > sma50,
            slope   > 0,
            r2      > 0.45,
        ])
        if score >= 4:
            col, lbl = '#00d4aa', 'BULL'
        elif score == 3:
            col, lbl = '#4a9eff', 'MIXED'
        elif score == 2:
            col, lbl = '#f5a623', 'MIXED'
        else:
            col, lbl = '#ff4a6a', 'BEAR'
        return score, sma10_s, sma20_s, sma30_s, sma50_s, col, lbl

    # Full 1-year macro series for SMA/regime warmup (never truncated)
    src_full = mh if mh else history
    macro_arkk_full = [h.get('arkk_qqq') for h in src_full]
    macro_iwm_full  = [h.get('iwm_spy')  for h in src_full]
    macro_xly_full  = [h.get('xly_xlp')  for h in src_full]
    macro_hyg_full  = [h.get('hyg_ief')  for h in src_full]

    arkk_score, arkk_sma10_full, arkk_sma20_full, arkk_sma30_full, arkk_sma50_full, arkk_col, arkk_lbl = _ratio_regime(macro_arkk_full)
    iwm_score,  iwm_sma10_full,  iwm_sma20_full,  iwm_sma30_full,  iwm_sma50_full,  iwm_col,  iwm_lbl  = _ratio_regime(macro_iwm_full)
    xly_score,  xly_sma10_full,  xly_sma20_full,  xly_sma30_full,  xly_sma50_full,  xly_col,  xly_lbl  = _ratio_regime(macro_xly_full)
    hyg_score,  hyg_sma10_full,  hyg_sma20_full,  hyg_sma30_full,  hyg_sma50_full,  hyg_col,  hyg_lbl  = _ratio_regime(macro_hyg_full)

    # ── Macro display window: last 63 market days ending at session_date ────────
    # When mh (252-day yfinance) is available the warmup is complete for all 63
    # visible days. When mh is empty (fallback) we use all history days instead.
    N_MACRO = 63
    src_dates = [h['date'] for h in src_full]

    # IMPORTANT: filter to session_date FIRST, then take last N_MACRO.
    # If we did it the other way (take last 63 then filter), every extra
    # forward-day entry in mh beyond session_date would shrink the visible
    # window by one — causing the chart to lose one day on each run.
    src_for_display = (
        [h for h in src_full if h['date'] <= session_date]
        if session_date else src_full
    )
    macro_display_entries = (
        src_for_display[-N_MACRO:] if len(src_for_display) >= N_MACRO
        else src_for_display
    )

    macro_display_dates = []
    for h in macro_display_entries:
        try:
            macro_display_dates.append(datetime.strptime(h['date'], '%Y-%m-%d').strftime('%b %d'))
        except Exception:
            macro_display_dates.append(h['date'])

    # All macro series aligned to the 63-day display window by date
    def _align_by_date(full_series, display_entries):
        lk = dict(zip(src_dates, full_series))
        return [lk.get(h['date']) for h in display_entries]

    macro_arkk = _align_by_date(macro_arkk_full, macro_display_entries)
    macro_iwm  = _align_by_date(macro_iwm_full,  macro_display_entries)
    macro_xly  = _align_by_date(macro_xly_full,  macro_display_entries)
    macro_hyg  = _align_by_date(macro_hyg_full,  macro_display_entries)

    arkk_sma10 = _align_by_date(arkk_sma10_full, macro_display_entries)
    arkk_sma20 = _align_by_date(arkk_sma20_full, macro_display_entries)
    arkk_sma30 = _align_by_date(arkk_sma30_full, macro_display_entries)
    arkk_sma50 = _align_by_date(arkk_sma50_full, macro_display_entries)
    iwm_sma10  = _align_by_date(iwm_sma10_full,  macro_display_entries)
    iwm_sma20  = _align_by_date(iwm_sma20_full,  macro_display_entries)
    iwm_sma30  = _align_by_date(iwm_sma30_full,  macro_display_entries)
    iwm_sma50  = _align_by_date(iwm_sma50_full,  macro_display_entries)
    xly_sma10  = _align_by_date(xly_sma10_full,  macro_display_entries)
    xly_sma20  = _align_by_date(xly_sma20_full,  macro_display_entries)
    xly_sma30  = _align_by_date(xly_sma30_full,  macro_display_entries)
    xly_sma50  = _align_by_date(xly_sma50_full,  macro_display_entries)
    hyg_sma10  = _align_by_date(hyg_sma10_full,  macro_display_entries)
    hyg_sma20  = _align_by_date(hyg_sma20_full,  macro_display_entries)
    hyg_sma30  = _align_by_date(hyg_sma30_full,  macro_display_entries)
    hyg_sma50  = _align_by_date(hyg_sma50_full,  macro_display_entries)

    # ── Composite ATR-normalised — ARKK · IWM · HYG · XLY (coefficienti grezzi) ─
    # Composite   = media EW di (close_i - SMA20_i) / ATR14_i  per i 4 ticker
    # Linea SMA10 = media EW di (SMA10_close_i - SMA20_i) / ATR14_i  per i 4 ticker
    # Unità: coefficienti ATR14 del singolo ticker → cross-asset bilanciati.
    # +1 = prezzo/SMA10 mediamente 1 ATR sopra la SMA20; 0 = equilibrio (SMA20).
    # Nessuno z-score — i valori grezzi sono già comparabili perché normalizzati
    # per la propria volatilità. Fallback graceful: ticker senza dati escluso dalla media.

    def _simple_sma(arr, w):
        """Rolling SMA con finestra parziale — parte dal primo punto disponibile."""
        out = []
        for i in range(len(arr)):
            win = [v for v in arr[max(0, i - w + 1):i + 1] if v is not None]
            out.append(round(sum(win) / len(win), 2) if win else None)
        return out

    def _atr_dist(close_arr, sma20_arr, atr14_arr):
        """(close - SMA20) / ATR14 — None se uno dei tre è None o ATR14 == 0."""
        out = []
        for c, s, a in zip(close_arr, sma20_arr, atr14_arr):
            if c is None or s is None or a is None or a == 0:
                out.append(None)
            else:
                out.append(round((c - s) / a, 2))
        return out

    def _rolling_z60(arr, W=60):
        """Rolling z-score su finestra W (min 5 punti validi)."""
        out = []
        for i in range(len(arr)):
            win = [x for x in arr[max(0, i - W + 1):i + 1] if x is not None]
            if len(win) < 5 or arr[i] is None:
                out.append(None)
                continue
            mean = sum(win) / len(win)
            std  = (sum((x - mean) ** 2 for x in win) / len(win)) ** 0.5
            out.append(round((arr[i] - mean) / std, 2) if std > 0 else 0.0)
        return out

    # Estrai le serie close/sma20/atr14 per i 4 ticker dal macro_history
    def _mh_series(key):
        return [h.get(key) for h in src_full]

    arkk_close_full = _mh_series('arkk_close')
    arkk_s20_full   = _mh_series('arkk_sma20')
    arkk_atr_full   = _mh_series('arkk_atr14')
    iwm_close_full  = _mh_series('iwm_close')
    iwm_s20_full    = _mh_series('iwm_sma20')
    iwm_atr_full    = _mh_series('iwm_atr14')
    hyg_close_full  = _mh_series('hyg_close')
    hyg_s20_full    = _mh_series('hyg_sma20')
    hyg_atr_full    = _mh_series('hyg_atr14')
    xly_close_full  = _mh_series('xly_close')
    xly_s20_full    = _mh_series('xly_sma20')
    xly_atr_full    = _mh_series('xly_atr14')

    # ── Composite (close) e linea SMA10 — coefficienti ATR grezzi, media EW ──────
    # Composite   = mean( (close_i - SMA20_i) / ATR14_i )  per i 4 ticker
    # Linea SMA10 = mean( (SMA10_close_i - SMA20_i) / ATR14_i )  per i 4 ticker
    # Stessa scala: unità di ATR14 del singolo ticker (+1 = 1 ATR sopra SMA20).
    # Niente z-score — i coefficienti grezzi sono già comparabili cross-asset
    # perché ciascuno è normalizzato per la propria volatilità (ATR14).
    # Crossover close > SMA10 = prezzo accelera oltre la media veloce.

    coeff_arkk_close = _atr_dist(arkk_close_full, arkk_s20_full, arkk_atr_full)
    coeff_iwm_close  = _atr_dist(iwm_close_full,  iwm_s20_full,  iwm_atr_full)
    coeff_hyg_close  = _atr_dist(hyg_close_full,  hyg_s20_full,  hyg_atr_full)
    coeff_xly_close  = _atr_dist(xly_close_full,  xly_s20_full,  xly_atr_full)

    arkk_sma10_p     = _simple_sma(arkk_close_full, 10)
    iwm_sma10_p      = _simple_sma(iwm_close_full,  10)
    hyg_sma10_p      = _simple_sma(hyg_close_full,  10)
    xly_sma10_p      = _simple_sma(xly_close_full,  10)

    coeff_arkk_sma10 = _atr_dist(arkk_sma10_p, arkk_s20_full, arkk_atr_full)
    coeff_iwm_sma10  = _atr_dist(iwm_sma10_p,  iwm_s20_full,  iwm_atr_full)
    coeff_hyg_sma10  = _atr_dist(hyg_sma10_p,  hyg_s20_full,  hyg_atr_full)
    coeff_xly_sma10  = _atr_dist(xly_sma10_p,  xly_s20_full,  xly_atr_full)

    comp_full = []
    comp_sma10_full = []
    for i in range(len(src_dates)):
        vc = [v for v in [coeff_arkk_close[i], coeff_iwm_close[i],
                           coeff_hyg_close[i],  coeff_xly_close[i]] if v is not None]
        vs = [v for v in [coeff_arkk_sma10[i], coeff_iwm_sma10[i],
                           coeff_hyg_sma10[i],  coeff_xly_sma10[i]] if v is not None]
        comp_full.append(round(sum(vc) / len(vc), 2) if vc else None)
        comp_sma10_full.append(round(sum(vs) / len(vs), 2) if vs else None)

    comp_sma20_full = [None] * len(comp_full)   # non usata — mantenuta per compatibilità template

    composite  = _align_by_date(comp_full,       macro_display_entries)
    comp_sma10 = _align_by_date(comp_sma10_full, macro_display_entries)
    comp_sma20 = _align_by_date(comp_sma20_full, macro_display_entries)

    # Macro Regime — composite (price) vs SMA10 / SMA20
    # Composite = media EW di (close - SMA20) / ATR14 dei 4 ticker
    # comp_sma10_full = SMA10 del composite  (già calcolata sopra)
    # SMA20 del composite — calcolata inline per il solo valore terminale
    comp_sma20_val_list = [v for v in comp_full if v is not None]
    comp_last   = comp_full[-1]  if comp_full  else None
    comp_sma10v = comp_sma10_full[-1] if comp_sma10_full else None
    comp_sma20v = (sum(comp_sma20_val_list[-20:]) / min(len(comp_sma20_val_list), 20)
                   if len(comp_sma20_val_list) >= 20 else None)
    xly_has_data = (xly_lbl != 'NO DATA')
    macro_total  = arkk_score + iwm_score + (xly_score if xly_has_data else 0) + hyg_score
    macro_max    = 20 if xly_has_data else 15
    if comp_last is not None and comp_sma10v is not None and comp_sma20v is not None:
        above_sma10 = comp_last > comp_sma10v
        above_sma20 = comp_last > comp_sma20v
        if above_sma10 and above_sma20:
            macro_col, macro_lbl = '#00d4aa', 'BULLISH'   # sopra SMA10 e SMA20
        elif above_sma10:
            macro_col, macro_lbl = '#f5a623', 'CAUTIOUS'  # sopra SMA10, sotto SMA20
        else:
            macro_col, macro_lbl = '#ff4a6a', 'BEARISH'   # sotto SMA10
    else:
        macro_col, macro_lbl = '#6b7280', 'NO DATA'

    # Latest ratio values for display (from full src_full, not the 63-day slice)
    arkk_val = next((v for v in reversed(macro_arkk_full) if v is not None), 0.0)
    iwm_val  = next((v for v in reversed(macro_iwm_full)  if v is not None), 0.0)
    xly_val  = next((v for v in reversed(macro_xly_full)  if v is not None), 0.0)
    hyg_val  = next((v for v in reversed(macro_hyg_full)  if v is not None), 0.0)

    jdates      = json.dumps(display_dates)
    jmacrodates = json.dumps(macro_display_dates)
    jlong       = json.dumps(long_breadth)
    jshort      = json.dumps(short_breadth)
    jspreads    = json.dumps(spreads)
    jst_long    = json.dumps(st_long)
    jst_short   = json.dumps(st_short)
    jst_spreads = json.dumps(st_spreads)
    jst_regime  = json.dumps(st_regime)
    jmacro_arkk      = json.dumps(macro_arkk)
    jmacro_arkk_s10  = json.dumps(arkk_sma10)
    jmacro_arkk_s20  = json.dumps(arkk_sma20)
    jmacro_arkk_s30  = json.dumps(arkk_sma30)
    jmacro_arkk_s50  = json.dumps(arkk_sma50)
    jmacro_iwm       = json.dumps(macro_iwm)
    jmacro_iwm_s10   = json.dumps(iwm_sma10)
    jmacro_iwm_s20   = json.dumps(iwm_sma20)
    jmacro_iwm_s30   = json.dumps(iwm_sma30)
    jmacro_iwm_s50   = json.dumps(iwm_sma50)
    jmacro_xly       = json.dumps(macro_xly)
    jmacro_xly_s10   = json.dumps(xly_sma10)
    jmacro_xly_s20   = json.dumps(xly_sma20)
    jmacro_xly_s30   = json.dumps(xly_sma30)
    jmacro_xly_s50   = json.dumps(xly_sma50)
    jmacro_hyg       = json.dumps(macro_hyg)
    jmacro_hyg_s10   = json.dumps(hyg_sma10)
    jmacro_hyg_s20   = json.dumps(hyg_sma20)
    jmacro_hyg_s30   = json.dumps(hyg_sma30)
    jmacro_hyg_s50   = json.dumps(hyg_sma50)
    # Composite z-score + SMA10/SMA20 — pre-calcolati in Python, date-aligned a history
    jcomposite   = json.dumps(composite)
    jcomp_sma10  = json.dumps(comp_sma10)
    jcomp_sma20  = json.dumps(comp_sma20)

    return f"""
<!-- ── Market Breadth ────────────────────────────────────────────────── -->
<div class="br-wrap">
  <div class="br-header-row">
    <div class="br-section-title">Market Breadth</div>
    <div class="br-as-of">as of {latest_date_display}</div>
  </div>

  <div class="br-stats-row">
    <div class="br-stat">
      <div class="br-stat-label">Regime</div>
      <div class="br-stat-value" style="color:{regime_color}">{regime_label}</div>
      <div class="br-sub">{regime_sub}</div>
    </div>
    <div class="br-stat">
      <div class="br-stat-label">Long Breadth</div>
      <div class="br-stat-value stat-green">{l_pct}%
        <span class="br-delta" style="color:{l_dcol}">{l_dstr}</span>
      </div>
    </div>
    <div class="br-stat">
      <div class="br-stat-label">Short Breadth</div>
      <div class="br-stat-value stat-red">{s_pct}%
        <span class="br-delta" style="color:{s_dcol}">{s_dstr}</span>
      </div>
    </div>
    <div class="br-stat">
      <div class="br-stat-label">L-S Spread</div>
      <div class="br-stat-value stat-blue">{spread_val}
        <span class="br-delta" style="color:{sp_dcol}">{sp_dstr}</span>
      </div>
      <div class="br-sub">5d mom: {sp_mom_str}</div>
    </div>
    <div class="br-stat">
      <div class="br-stat-label">Sector Avg Score</div>
      <div class="br-stat-value stat-blue">{s_avg}
        <span class="br-delta" style="color:{a_dcol}">{a_dstr}</span>
      </div>
    </div>
  </div>

  <div class="br-chart-card">
    <div class="br-chart-row">
      <div class="br-chart-label">
        Macro Regime
        <span class="br-pill" style="background:rgba(255,255,255,.07);color:{macro_col};border:1px solid {macro_col}44">{macro_lbl}</span>
        <span class="br-guide" style="margin-left:6px;font-size:.68rem;color:{macro_col}">{macro_total}/{macro_max}</span>
      </div>
    </div>
    <p class="br-chart-desc">Cross-asset regime filter: ARKK/QQQ (growth risk appetite) &middot; IWM/SPY (small-cap participation) &middot; XLY/XLP (consumer risk on/off) &middot; HYG/IEF (credit leading indicator). Each ratio scored 0&ndash;5 via SMA10/20/50 alignment + linear regression slope &amp; R&sup2;. Total 0&ndash;20: &ge;15 Risk On &middot; &ge;10 Cautious &middot; &ge;5 Neutral &middot; &lt;5 Risk Off. Dashed line = SMA10 (blue) &middot; SMA20 (yellow) &middot; SMA30 (white) &middot; SMA50 (purple).</p>

    <div class="br-chart-row" style="margin-top:14px">
      <div class="br-chart-label">
        Ratio Spread &mdash; Combined (Indexed)
      </div>
    </div>
    <p class="br-chart-desc">Equal-weight composite of the 60-day rolling z-score of each ticker&rsquo;s ATR-normalised distance from SMA20 &mdash; ARKK &middot; IWM &middot; HYG &middot; XLY. Signal per ticker: (Close &minus; SMA20) &divide; ATR14. Normalising by ATR14 equalises contribution across assets with very different volatility (e.g. ARKK vs HYG). Scale: ATR units. +1 = 1 ATR above SMA20 (aggregate). Above 0 = tickers in aggregate above SMA20 <span style="color:#4a9eff">&#9632;</span> &middot; below 0 = below SMA20 <span style="color:#ff4a6a">&#9632;</span>. Dashed line: SMA10 of closes (yellow) &mdash; crossover with composite = momentum signal. Zero = SMA20 equilibrium.</p>
    <div class="br-combined-wrap"><canvas id="macroCombinedChart"></canvas></div>

    <hr class="br-divider">
    <div class="br-chart-row" style="margin-top:8px">
      <div class="br-chart-label">
        ARKK / QQQ
        <span class="br-pill" style="background:rgba(255,255,255,.07);color:{arkk_col};border:1px solid {arkk_col}44">{arkk_lbl} &bull; {arkk_score}/5</span>
        <span class="br-guide" style="margin-left:6px;font-size:.68rem;color:#505068">{arkk_val:.4f}</span>
      </div>
    </div>
    <div class="br-macro-wrap"><canvas id="macroArkkChart"></canvas></div>

    <div class="br-chart-row" style="margin-top:10px">
      <div class="br-chart-label">
        IWM / SPY
        <span class="br-pill" style="background:rgba(255,255,255,.07);color:{iwm_col};border:1px solid {iwm_col}44">{iwm_lbl} &bull; {iwm_score}/5</span>
        <span class="br-guide" style="margin-left:6px;font-size:.68rem;color:#505068">{iwm_val:.4f}</span>
      </div>
    </div>
    <div class="br-macro-wrap"><canvas id="macroIwmChart"></canvas></div>

    <div class="br-chart-row" style="margin-top:10px">
      <div class="br-chart-label">
        XLY / XLP
        <span class="br-pill" style="background:rgba(255,255,255,.07);color:{xly_col};border:1px solid {xly_col}44">{xly_lbl} &bull; {xly_score}/5</span>
        <span class="br-guide" style="margin-left:6px;font-size:.68rem;color:#505068">{xly_val:.4f}</span>
      </div>
    </div>
    <div class="br-macro-wrap"><canvas id="macroXlyChart"></canvas></div>

    <div class="br-chart-row" style="margin-top:10px">
      <div class="br-chart-label">
        HYG / IEF
        <span class="br-pill" style="background:rgba(255,255,255,.07);color:{hyg_col};border:1px solid {hyg_col}44">{hyg_lbl} &bull; {hyg_score}/5</span>
        <span class="br-guide" style="margin-left:6px;font-size:.68rem;color:#505068">{hyg_val:.4f}</span>
      </div>
    </div>
    <div class="br-macro-wrap"><canvas id="macroHygChart"></canvas></div>
  </div>
</div>
<!-- ── /Market Breadth ───────────────────────────────────────────────── -->

<script>
document.addEventListener('DOMContentLoaded', function() {{

  // ── Macro Regime charts ───────────────────────────────────────────────────
  var macroDates = {jmacrodates};  // last 63 market days — x-axis for all ratio + composite charts

  function buildMacroChart(canvasId, ratioData, sma10Data, sma20Data, sma30Data, sma50Data, bullCol) {{
    var ctx = document.getElementById(canvasId).getContext('2d');
    var gradUp = ctx.createLinearGradient(0, 0, 0, 130);
    gradUp.addColorStop(0,   'rgba(' + (bullCol === '#00d4aa' ? '0,212,170' : bullCol === '#4a9eff' ? '74,158,255' : '0,212,170') + ',0.22)');
    gradUp.addColorStop(1,   'rgba(0,0,0,0)');
    var gradDown = ctx.createLinearGradient(0, 0, 0, 130);
    gradDown.addColorStop(0, 'rgba(0,0,0,0)');
    gradDown.addColorStop(1, 'rgba(255,74,106,0.18)');

    new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: macroDates,
        datasets: [
          {{
            label: 'Ratio',
            data: ratioData,
            segment: {{
              borderColor: function(c) {{
                var s10 = sma10Data[c.p1DataIndex];
                var v   = c.p1.parsed.y;
                return (s10 !== null && v > s10) ? 'rgba(0,212,170,0.9)' : 'rgba(255,74,106,0.85)';
              }},
              backgroundColor: function(c) {{
                var s10 = sma10Data[c.p1DataIndex];
                var v   = c.p1.parsed.y;
                return (s10 !== null && v > s10) ? gradUp : gradDown;
              }}
            }},
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            tension: 0.3,
            fill: false,
            spanGaps: true
          }},
          {{
            label: 'SMA10',
            data: sma10Data,
            borderColor: 'rgba(74,158,255,0.75)',
            borderWidth: 1.5,
            borderDash: [4, 3],
            pointRadius: 0,
            fill: false,
            spanGaps: true
          }},
          {{
            label: 'SMA20',
            data: sma20Data,
            borderColor: 'rgba(245,166,35,0.65)',
            borderWidth: 1.5,
            borderDash: [2, 4],
            pointRadius: 0,
            fill: false,
            spanGaps: true
          }},
          {{
            label: 'SMA30',
            data: sma30Data,
            borderColor: 'rgba(255,255,255,0.45)',
            borderWidth: 1.5,
            borderDash: [3, 5],
            pointRadius: 0,
            fill: false,
            spanGaps: true
          }},
          {{
            label: 'SMA50',
            data: sma50Data,
            borderColor: 'rgba(148,0,211,0.80)',
            borderWidth: 1.5,
            borderDash: [6, 3],
            pointRadius: 0,
            fill: false,
            spanGaps: true
          }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            backgroundColor: '#10101a', borderColor: '#2a2a44', borderWidth: 1,
            titleColor: '#e8e8f0', bodyColor: '#8888a0',
            callbacks: {{
              label: function(c) {{
                if (c.datasetIndex === 0) return ' Ratio: '          + (c.parsed.y !== null ? c.parsed.y.toFixed(4) : '—');
                if (c.datasetIndex === 1) return ' SMA10 (blue): '   + (c.parsed.y !== null ? c.parsed.y.toFixed(4) : '—');
                if (c.datasetIndex === 2) return ' SMA20 (yellow): ' + (c.parsed.y !== null ? c.parsed.y.toFixed(4) : '—');
                if (c.datasetIndex === 3) return ' SMA30 (white): '  + (c.parsed.y !== null ? c.parsed.y.toFixed(4) : '—');
                return ' SMA50 (purple): ' + (c.parsed.y !== null ? c.parsed.y.toFixed(4) : '—');
              }}
            }}
          }}
        }},
        scales: {{
          x: {{
            grid: {{ color: 'rgba(255,255,255,0.03)' }},
            ticks: {{ color: '#505068', font: {{ size: 10 }}, maxTicksLimit: 12 }}
          }},
          y: {{
            grid: {{ color: 'rgba(255,255,255,0.04)' }},
            ticks: {{ color: '#505068', font: {{ size: 10 }},
                      callback: function(v) {{ return v.toFixed(3); }} }}
          }}
        }}
      }}
    }});
  }}

  buildMacroChart('macroArkkChart', {jmacro_arkk}, {jmacro_arkk_s10}, {jmacro_arkk_s20}, {jmacro_arkk_s30}, {jmacro_arkk_s50}, '{arkk_col}');
  buildMacroChart('macroIwmChart',  {jmacro_iwm},  {jmacro_iwm_s10},  {jmacro_iwm_s20},  {jmacro_iwm_s30},  {jmacro_iwm_s50},  '{iwm_col}');
  buildMacroChart('macroXlyChart',  {jmacro_xly},  {jmacro_xly_s10},  {jmacro_xly_s20},  {jmacro_xly_s30},  {jmacro_xly_s50},  '{xly_col}');
  buildMacroChart('macroHygChart',  {jmacro_hyg},  {jmacro_hyg_s10},  {jmacro_hyg_s20},  {jmacro_hyg_s30},  {jmacro_hyg_s50},  '{hyg_col}');

  // ── Combined Composite: z-score of % distance from SMA20 (EW) ──────────────
  // Pre-calcolato in Python con allineamento per data (stessi giorni di macroDates)
  (function() {{
    var composite = {jcomposite};
    var compSMA10 = {jcomp_sma10};
    var compSMA20 = {jcomp_sma20};

    var ctx = document.getElementById('macroCombinedChart').getContext('2d');
    var gradUp   = ctx.createLinearGradient(0, 0, 0, 190);
    gradUp.addColorStop(0, 'rgba(74,158,255,0.22)');
    gradUp.addColorStop(1, 'rgba(74,158,255,0.00)');
    var gradDown = ctx.createLinearGradient(0, 0, 0, 190);
    gradDown.addColorStop(0, 'rgba(255,74,106,0.00)');
    gradDown.addColorStop(1, 'rgba(255,74,106,0.18)');

    new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: macroDates,
        datasets: [
          {{
            label: 'Macro Composite (EW)',
            data: composite,
            segment: {{
              borderColor: function(c) {{
                return c.p1.parsed.y >= 0 ? 'rgba(74,158,255,0.95)' : 'rgba(255,74,106,0.90)';
              }},
              backgroundColor: function(c) {{
                return c.p1.parsed.y >= 0 ? gradUp : gradDown;
              }}
            }},
            borderWidth: 2.2,
            pointRadius: 0, pointHoverRadius: 5,
            tension: 0.35,
            fill: {{ target: {{ value: 0 }} }},
            spanGaps: true
          }},
          {{
            label: 'SMA10',
            data: compSMA10,
            borderColor: 'rgba(245,166,35,0.85)',
            borderWidth: 1.5,
            borderDash: [4, 3],
            pointRadius: 0,
            fill: false,
            spanGaps: true
          }},
          {{
            label: '_baseline',
            data: composite.map(function() {{ return 0; }}),
            borderColor: 'rgba(255,255,255,0.18)',
            borderWidth: 1,
            borderDash: [4, 4],
            pointRadius: 0,
            fill: false,
            spanGaps: true
          }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            backgroundColor: '#10101a', borderColor: '#2a2a44', borderWidth: 1,
            titleColor: '#e8e8f0', bodyColor: '#8888a0',
            callbacks: {{
              label: function(c) {{
                if (c.datasetIndex === 3) return null;  // baseline hidden
                if (c.datasetIndex === 1) return ' SMA10 (blue): '   + (c.parsed.y !== null ? c.parsed.y.toFixed(2) : '—');
                if (c.datasetIndex === 2) return ' SMA20 (yellow): ' + (c.parsed.y !== null ? c.parsed.y.toFixed(2) : '—');
                var v   = c.parsed.y;
                var lbl = v >=  1.0 ? 'RISK ON'
                        : v >=  0.0 ? 'POSITIVE'
                        : v >= -1.0 ? 'NEGATIVE'
                        :             'RISK OFF';
                return [' Composite z: ' + (v !== null ? v.toFixed(2) : '—'), ' Signal: ' + lbl];
              }}
            }}
          }}
        }},
        scales: {{
          x: {{
            grid: {{ color: 'rgba(255,255,255,0.03)' }},
            ticks: {{ color: '#505068', font: {{ size: 10 }}, maxTicksLimit: 12 }}
          }},
          y: {{
            grid: {{ color: 'rgba(255,255,255,0.04)' }},
            ticks: {{
              color: '#505068', font: {{ size: 10 }},
              callback: function(v) {{ return v.toFixed(1); }}
            }}
          }}
        }}
      }}
    }});
  }})();

}});
</script>
"""


def build_sector_charts_html(market_history, top10_data=None, top10_short_data=None,
                             session_date=None):
    """
    Sector Performance section — enhanced sparkline grid.
    Each card shows: signal badge, score, Δ5d, Δ20d, percentile, MA10 tag.
    Sparkline has MA10 overlay and reference lines at 50/70.
    JS client-side sort by Score / Δ5d / Δ20d / Percentile / Stage.
    Click card to expand Top-10 stocks panel (Long or Short toggle).
    session_date: when provided, history entries with date > session_date are
                  excluded so sparklines are anchored to the daily report date.
    """
    if not market_history:
        return ''

    history = sorted(market_history, key=lambda x: x['date'])
    # Cap to session_date — exclude backfill estimated forward-day entries.
    if session_date:
        history = [h for h in history if h['date'] <= session_date]
    if not history:
        return ''

    # Use the most recent entry that has sectors_short populated.
    # Backfill estimated entries (tagged 'estimated': True) are created for the
    # current/next trading day from price data but carry NO sectors_short.
    # If such an entry becomes history[-1] it would make all short scores 0.
    latest = next(
        (h for h in reversed(history) if h.get('sectors_short')),
        history[-1]
    )
    latest_sectors       = latest.get('sectors', {})
    latest_sectors_short = latest.get('sectors_short', {})
    if not latest_sectors:
        return ''

    # ── Per-sector stats ───────────────────────────────────────────────────────
    N_SPARK = 63  # sparklines show the same 63-day window as the macro ratio charts
    sector_stats = []
    for sec_name, cur_score in latest_sectors.items():
        values_all = [h.get('sectors', {}).get(sec_name) for h in history]
        # Cap to last 63 entries so the sparkline window matches the ratio charts.
        values = values_all[-N_SPARK:] if len(values_all) > N_SPARK else values_all
        valid  = [v for v in values if v is not None]
        if len(valid) < 2:
            continue

        # 5d and 20d momentum
        d5  = round(valid[-1] - (valid[-6]  if len(valid) >= 6  else valid[0]), 1)
        d20 = round(valid[-1] - (valid[-21] if len(valid) >= 21 else valid[0]), 1)

        # MA10
        ma10_window = valid[max(0, len(valid) - 10):]
        ma10        = round(sum(ma10_window) / len(ma10_window), 1)
        above_ma10  = cur_score >= ma10
        ma_gap      = round(cur_score - ma10, 1)

        # Historical percentile (within own range)
        pct = round(sum(1 for v in valid if v <= cur_score) / len(valid) * 100)

        # Signal classification (long)
        sig_label, sig_color = _sector_signal(cur_score, d5, above_ma10)

        # ── Short score + stage ────────────────────────────────────────────
        short_cur        = latest_sectors_short.get(sec_name)
        short_values_all = [h.get('sectors_short', {}).get(sec_name) for h in history]
        # Cap to last 63 entries to match the long sparkline window.
        short_values = short_values_all[-N_SPARK:] if len(short_values_all) > N_SPARK else short_values_all
        short_valid  = [v for v in short_values if v is not None]
        if short_cur is not None and len(short_valid) >= 2:
            s_d5       = round(short_valid[-1] - (short_valid[-6] if len(short_valid) >= 6 else short_valid[0]), 1)
            s_ma10_w   = short_valid[max(0, len(short_valid) - 10):]
            s_ma10     = round(sum(s_ma10_w) / len(s_ma10_w), 1)
            s_above    = short_cur >= s_ma10
            short_sig_label, short_sig_color = _sector_signal(short_cur, s_d5, s_above)
        else:
            short_cur       = 0.0
            short_sig_label = 'WEAK'
            short_sig_color = '#ff4a6a'

        sector_stats.append({
            'name':            sec_name,
            'score':           cur_score,
            'd5':              d5,
            'd20':             d20,
            'ma10':            ma10,
            'above_ma10':      above_ma10,
            'ma_gap':          ma_gap,
            'pct':             pct,
            'sig_label':       sig_label,
            'sig_color':       sig_color,
            'short_score':     short_cur,
            'short_sig_label': short_sig_label,
            'short_sig_color': short_sig_color,
            'values':          values,
        })

    # Default sort: by stage (same order as sortByStage JS), then score descending within stage
    STAGE_ORDER_PY = ['LEADING','FADING','BUILDING','SLIPPING','HOLDING','RECOVERY','WEAK','BEARISH']
    STAGE_COLORS_PY = {
        'LEADING': '#00d4aa', 'FADING': '#4a9eff', 'BUILDING': '#4a9eff',
        'SLIPPING': '#f5a623', 'HOLDING': '#f5a623', 'RECOVERY': '#f5a623',
        'WEAK': '#ff4a6a', 'BEARISH': '#ff4a6a',
    }
    stage_rank = {s: i for i, s in enumerate(STAGE_ORDER_PY)}
    sector_stats.sort(key=lambda x: (stage_rank.get(x['sig_label'], 99), -x['score']))

    # ── Build card HTML grouped by stage (with stage headers) ─────────────────
    grid_html = ''
    current_stage = None
    # Pre-count cards per stage for the header label
    stage_counts = {}
    for s in sector_stats:
        stage_counts[s['sig_label']] = stage_counts.get(s['sig_label'], 0) + 1

    for s in sector_stats:
        score           = s['score']
        d5              = s['d5']
        d20             = s['d20']
        pct             = s['pct']
        ma10            = s['ma10']
        ma_gap          = s['ma_gap']
        sig_label       = s['sig_label']
        sig_color       = s['sig_color']
        above_ma10      = s['above_ma10']
        short_score     = s['short_score']
        short_sig_label = s['short_sig_label']
        short_sig_color = s['short_sig_color']

        # Stage header when stage group changes
        if sig_label != current_stage:
            current_stage = sig_label
            hdr_color = STAGE_COLORS_PY.get(sig_label, '#888888')
            hdr_count = stage_counts.get(sig_label, 0)
            grid_html += f'\n    <div class="sec-stage-hdr" style="color:{hdr_color}">{sig_label}  ({hdr_count})</div>'

        # Score color by level
        if score >= 70:   score_color = '#00d4aa'
        elif score >= 55: score_color = '#4a9eff'
        elif score >= 40: score_color = '#f5a623'
        else:             score_color = '#ff4a6a'

        # Δ5d display
        d5_str  = f'+{d5}' if d5 > 0 else str(d5)
        d5_col  = '#00d4aa' if d5 > 0.5 else ('#ff4a6a' if d5 < -0.5 else '#505068')
        d5_arrow = '▲' if d5 > 0.5 else ('▼' if d5 < -0.5 else '▸')

        # Δ20d display
        d20_str = f'+{d20}' if d20 > 0 else str(d20)
        d20_col = '#00d4aa' if d20 > 1 else ('#ff4a6a' if d20 < -1 else '#505068')

        # MA10 tag
        ma_tag_label = f'{"▲" if above_ma10 else "▼"} MA10'
        ma_tag_color = '#00d4aa' if above_ma10 else '#ff4a6a'
        ma_gap_str   = f'+{ma_gap}' if ma_gap >= 0 else str(ma_gap)

        # Signal badge background (rgba from hex)
        sig_r, sig_g, sig_b = (
            int(sig_color[1:3], 16),
            int(sig_color[3:5], 16),
            int(sig_color[5:7], 16)
        )
        sig_bg = f'rgba({sig_r},{sig_g},{sig_b},0.14)'

        # Truncate name
        disp_name = s['name'] if len(s['name']) <= 24 else s['name'][:22] + '…'

        # Enhanced sparkline
        spark_svg = make_sector_spark_svg(s['values'])

        # Top-10 data for click panel (long + short)
        top10_list       = (top10_data or {}).get(s['name'], [])
        top10_attr       = json.dumps(top10_list).replace('"', '&quot;')
        top10_short_list = (top10_short_data or {}).get(s['name'], [])
        top10_short_attr = json.dumps(top10_short_list).replace('"', '&quot;')

        grid_html += f"""
    <div class="sec-card" style="border-left-color:{sig_color}"
         data-score="{score}" data-d5="{d5}" data-d20="{d20}" data-pct="{pct}"
         data-stage="{sig_label}" data-stagecolor="{sig_color}"
         data-shortscore="{short_score}" data-shortstage="{short_sig_label}" data-shortstagecolor="{short_sig_color}"
         data-top10="{top10_attr}" data-top10short="{top10_short_attr}"
         onclick="openSectorPanel(this)">
      <div class="sec-card-header">
        <div class="sec-name" title="{s['name']}">{disp_name}</div>
        <div class="sec-sig" style="background:{sig_bg};color:{sig_color}">{sig_label}</div>
      </div>
      <div class="sec-metrics">
        <div class="sec-score" style="color:{score_color}">{score}</div>
        <div class="sec-ma-tag" style="color:{ma_tag_color}">{ma_tag_label}
          <span class="sec-ma-gap">({ma_gap_str})</span>
        </div>
      </div>
      <div class="sec-spark">{spark_svg}</div>
      <div class="sec-footer">
        <span class="sec-stat" style="color:{d5_col}">{d5_arrow} {d5_str}</span>
        <span class="sec-stat sec-stat-muted">{pct}th pct</span>
        <span class="sec-stat" style="color:{d20_col}">20d {d20_str}</span>
      </div>
    </div>"""

    n_sectors = len(sector_stats)

    # Count signals
    signals = {}
    for s in sector_stats:
        signals[s['sig_label']] = signals.get(s['sig_label'], 0) + 1
    sig_summary = ' &nbsp;·&nbsp; '.join(
        f'<span style="color:{_sector_signal(70 if k in ("LEADING","FADING") else 55 if k in ("BUILDING","SLIPPING","HOLDING") else 45, 1 if k in ("LEADING","BUILDING","RECOVERY") else -1, True)[1]}">{v} {k}</span>'
        for k, v in sorted(signals.items(), key=lambda x: -x[1])
    )

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")),
        autoescape=jinja2.select_autoescape(['html', 'xml'])
    )
    template = env.get_template("archive_sector.html")
    return template.render(
        grid_html=grid_html,
        sig_summary=sig_summary
    )


# ─────────────────────────────────────────────────────────────────────────────
# Index HTML builder
# ─────────────────────────────────────────────────────────────────────────────

def build_index_html(reports_with_stats, market_history=None, session_date=None,
                     macro_history=None):
    """Build the complete index.html for the archive site.

    session_date (str YYYY-MM-DD): when provided, basket top-10 and candidate
    data are loaded from that specific session instead of the latest file.
    """

    latest_report_html = build_latest_report_html(reports_with_stats)
    breadth_html       = ''
    sector_html        = ''
    sector_etf_html    = ''
    candidates_data = []
    if market_history:
        breadth_html    = build_breadth_html(market_history, macro_history=macro_history,
                                              session_date=session_date)
        top10_data       = get_latest_basket_top10(session_date=session_date, mode='long')
        top10_short_data = get_latest_basket_top10(session_date=session_date, mode='short')
        candidates_data  = get_latest_candidates(session_date=session_date)
        sector_html      = build_sector_charts_html(market_history,
                               top10_data=top10_data,
                               top10_short_data=top10_short_data,
                               session_date=session_date)
        sector_etf_html  = build_sector_etf_html(market_history,
                               session_date=session_date,
                               top10_data=top10_data,
                               top10_short_data=top10_short_data)

    candidates_json = json.dumps(candidates_data)
    now_str = datetime.now().strftime('%b %d, %Y at %H:%M')

    # Report link for sidebar
    report_url = ''
    report_date_display = ''
    if reports_with_stats:
        rdate = reports_with_stats[0][0]  # YYYY-MM-DD
        report_url = f'reports/{rdate}.html'
        report_date_display = format_date_display(rdate)

    import jinja2
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")),
        autoescape=jinja2.select_autoescape(['html', 'xml'])
    )
    
    template = env.get_template("archive.html")
    return template.render(
        latest_report_html=latest_report_html,
        breadth_html=breadth_html,
        sector_html=sector_html,
        sector_etf_html=sector_etf_html,
        now_str=now_str,
        candidates_json=candidates_json,
        report_url=report_url,
        report_date_display=report_date_display,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    archive_dir  = os.path.join(parent_dir, 'Archive')
    reports_dir  = os.path.join(archive_dir, 'reports')

    os.makedirs(reports_dir, exist_ok=True)

    reports = find_reports()

    # Remove archive reports no longer in the kept set
    keep_names = {f'{date_str}.html' for date_str, _ in reports}
    for fname in os.listdir(reports_dir):
        if fname.endswith('.html') and fname not in keep_names:
            try:
                os.remove(os.path.join(reports_dir, fname))
                print(f'  ✗ removed outdated: {fname}')
            except OSError:
                pass

    # ── Load existing market history ──────────────────────────────────────────
    market_history_list = load_market_history(archive_dir)
    macro_history_list  = load_macro_history(archive_dir)
    history_by_date     = {h['date']: h for h in market_history_list}

    # Inject macro ratios from macro_history (or from other history entries) into
    # any entry that is missing them.  This fixes the timing gap where
    # backfill_market_history.py runs BEFORE run_screener() creates the latest entry.
    macro_by_date = {e['date']: e for e in macro_history_list}
    if not macro_by_date:
        # macro_history is empty — build a fallback lookup from the history itself
        macro_by_date = {
            d: h for d, h in history_by_date.items()
            if h.get('arkk_qqq') is not None
        }
    for d, entry in history_by_date.items():
        if entry.get('arkk_qqq') is None and d in macro_by_date:
            src = macro_by_date[d]
            entry['arkk_qqq'] = src.get('arkk_qqq')
            entry['iwm_spy']  = src.get('iwm_spy')
            entry['xly_xlp']  = src.get('xly_xlp')
            entry['hyg_ief']  = src.get('hyg_ief')

    # ── Copy reports + gather stats ───────────────────────────────────────────
    reports_with_stats = []
    for date_str, src_path in reports:
        dst_path = os.path.join(reports_dir, f'{date_str}.html')
        shutil.copy2(src_path, dst_path)
        stats = get_csv_stats(date_str)
        reports_with_stats.append((date_str, stats))
        print(f'  → {date_str} copied  |  {stats["total_stocks"]} stocks, top: {stats["top_ticker"]}')

        existing = history_by_date.get(date_str)
        if existing is None or existing.get('estimated', False):
            ms = compute_market_scores(date_str)
            if ms:
                # Preserve st_long_pct from backfill if compute_market_scores returned 0
                # (e.g. CSV missing ma10/ma20 columns) but backfill had real values
                if existing and ms.get('st_long_pct', 0.0) == 0.0 and existing.get('st_long_pct'):
                    ms['st_long_pct']  = existing['st_long_pct']
                    ms['st_short_pct'] = existing.get('st_short_pct', 0.0)
                history_by_date[date_str] = ms
                replaced = '(replaced estimate) ' if existing else ''
                print(f'     ↳ {replaced}market scores: long={ms["long_breadth_pct"]}%  '
                      f'short={ms["short_breadth_pct"]}%  '
                      f'st_long={ms.get("st_long_pct",0)}%  sec_avg={ms["sector_avg"]}')
        elif existing and 'st_long_pct' not in existing:
            # Real entry missing st_long_pct — supplement without overwriting other fields
            ms = compute_market_scores(date_str)
            if ms and ms.get('st_long_pct', 0.0) > 0:
                updated = dict(existing)
                updated['st_long_pct']  = ms['st_long_pct']
                updated['st_short_pct'] = ms['st_short_pct']
                history_by_date[date_str] = updated
                print(f'     ↳ supplemented st_long={ms["st_long_pct"]}%  '
                      f'st_short={ms["st_short_pct"]}%  (real entry, was missing)')

    # ── Save updated market history ───────────────────────────────────────────
    updated_history = list(history_by_date.values())
    save_market_history(archive_dir, updated_history)
    print(f'\n✅  market_score_history.json  →  {len(updated_history)} entries')

    # ── Determine session_date for aligned chart loading ──────────────────────
    # Primary source: the latest BlackRat_YYYY-MM-DD.html report filename.
    session_date = reports_with_stats[0][0] if reports_with_stats else None

    # Fallback: if no report was found (e.g. first run, Reports/ empty) derive
    # session_date from the latest REAL history entry (not a backfill estimate).
    if session_date is None and updated_history:
        real_entries = [
            h for h in updated_history
            if not h.get('estimated', False) and h.get('sectors_short')
        ]
        if real_entries:
            session_date = max(h['date'] for h in real_entries)
        elif updated_history:
            session_date = max(h['date'] for h in updated_history
                               if not h.get('estimated', False))

    if session_date:
        print(f'\n📅  Session date for aligned charts: {session_date}')

    # ── Build index.html ──────────────────────────────────────────────────────
    index_html = build_index_html(
        reports_with_stats,
        market_history=updated_history,
        session_date=session_date,
        macro_history=macro_history_list,
    )
    index_path = os.path.join(archive_dir, 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(index_html)

    print(f'✅  Archive updated: {len(reports_with_stats)} report(s)')
    print(f'✅  index.html → {index_path}')
    return True


if __name__ == '__main__':
    main()
