
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def _fmt(val, decimals=2):
    """Format a value: round floats, pass strings/bools through."""
    if isinstance(val, (float, np.floating)):
        return f"{val:.{decimals}f}"
    if isinstance(val, bool) or isinstance(val, (np.bool_,)):
        return "Yes" if val else "No"
    return str(val)


def _pct(val, decimals=2):
    """Format a float as percentage string (0.123 → 12.30%)."""
    if isinstance(val, (float, np.floating)):
        return f"{val * 100:.{decimals}f}%"
    return str(val)


def _color_pct_cell(val):
    """Return a <td> with green/red color for a percentage value (already as float * 100)."""
    try:
        v = float(val)
    except (ValueError, TypeError):
        return f"<td>{val}</td>"
    color = "var(--accent-green)" if v > 0 else "var(--accent-red)" if v < 0 else "var(--text-secondary)"
    sign = "+" if v > 0 else ""
    return f'<td style="color:{color}">{sign}{v:.2f}%</td>'


def _build_table_html(df, table_id, columns=None, formatters=None, index=False,
                      pct_columns=None):
    """
    Build a clean <table> HTML string from a DataFrame.
    No nesting, no pandas artifacts. DataTables-ready.
    pct_columns: list of column names that should be colored green/red.
    """
    if columns is None:
        columns = list(df.columns)
    if formatters is None:
        formatters = {}
    if pct_columns is None:
        pct_columns = []

    rows_data = []
    for idx, row in zip(df.index, df.to_dict(orient="records")):
        cells = []
        for col in columns:
            val = row[col]
            if col in pct_columns:
                cells.append(_color_pct_cell(val))
            elif col in formatters:
                cells.append(f"<td>{formatters[col](val)}</td>")
            else:
                cells.append(f"<td>{_fmt(val)}</td>")
        rows_data.append({"idx": idx, "cells": cells})

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_table.html")
    return template.render(table_id=table_id, columns=columns, rows=rows_data, index=index)


def _parse_market_cap(cap_str):
    """Parse Finviz market cap string (e.g. '1.5B', '300M') to float in USD."""
    if not cap_str or str(cap_str).strip() in ('', '-'):
        return 0.0
    s = str(cap_str).strip()
    mult = {'T': 1e12, 'B': 1e9, 'M': 1e6, 'K': 1e3}
    if s[-1].upper() in mult:
        try:
            return float(s[:-1]) * mult[s[-1].upper()]
        except ValueError:
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _fetch_market_caps(tickers, chunk_size=100):
    """
    Fetch market caps for a list of tickers using data_fetcher (yfinance).
    """
    import data_fetcher
    return data_fetcher.fetch_market_caps(tickers, max_workers=20)


# Industries to exclude from curated sections (TC, TR, Recommended)
EXCLUDED_INDUSTRIES = {"Biotechnology"}


def _exclude_biotech(filtered_df):
    """
    Remove tickers classified as Biotechnology by yfinance,
    UNLESS the ticker belongs to one of our SECTOR_BASKETS.
    """
    import data_fetcher
    import sector_baskets
    tickers = [str(t) for t in filtered_df.index]
    if not tickers:
        return filtered_df
    basket_map = sector_baskets.build_ticker_basket_map()
    # Only check industry for tickers NOT in any basket
    tickers_to_check = [t for t in tickers if t not in basket_map]
    if not tickers_to_check:
        return filtered_df
    industries = data_fetcher.fetch_industries(tickers_to_check, max_workers=20)
    biotech_tickers = {t for t, ind in industries.items() if ind in EXCLUDED_INDUSTRIES}
    if biotech_tickers:
        filtered_df = filtered_df[~filtered_df.index.map(str).isin(biotech_tickers)].copy()
    return filtered_df


def _build_recommended_html(display_df, basket_df, mode="long"):
    """
    Previously built 'Recommended Stocks' section — now hidden.
    The Momentum Pullback Screener serves as 'Recommended Stocks' for both Long and Short.
    """
    return ""


def _build_momentum_pullback_html(display_df):
    """
    Build the Long "Recommended Stocks" (Momentum Pullback) section.
    Criteria:
      - Price > 1
      - ADR% >= 2.5%
      - Price >= 70% above 52w low
      - 3m and 6m performance > 0%
      - Market cap > 500M, exclude biotech
      - Price > 65 EMA daily
      - Price > 21 EMA weekly and 30 SMA weekly
      - 7-Factor (Final_Score) >= 60
    Sorted by 7-Factor descending.
    Includes distance % from 65 SMA 30min.
    """
    df = display_df.copy()

    required = ['last_price', 'adr_pct', 'dist_from_52w_low', '3m_return', '6m_return',
                'ema65', 'ema21w', 'sma30w', 'ema21_dist', 'ema9_dist', 'Final_Score']

    for col in required:
        if col not in df.columns:
            df[col] = 0.0

    mask = (
        (df['last_price'] > 1.0) &
        (df['adr_pct'] >= 0.025) &
        (df['dist_from_52w_low'] >= 0.70) &
        (df['3m_return'] > 0) &
        (df['6m_return'] > 0) &
        (df['last_price'] > df['ema65']) &
        (df['last_price'] > df['ema21w']) &
        (df['last_price'] > df['sma30w']) &
        (df['Final_Score'] >= 60)
    )
    filtered = df[mask].copy()

    if filtered.empty:
        return ""

    # ── Market cap filter: >= $500M ──
    MIN_MARKET_CAP = 500_000_000
    tickers_to_check = [str(t) for t in filtered.index]
    mkt_caps = _fetch_market_caps(tickers_to_check)
    if mkt_caps:
        qualified = {t for t, cap in mkt_caps.items() if cap >= MIN_MARKET_CAP}
        filtered = filtered[filtered.index.map(str).isin(qualified)].copy()

    if filtered.empty:
        return ""

    # ── Exclude biotech by yfinance industry ──
    filtered = _exclude_biotech(filtered)
    if filtered.empty:
        return ""

    # ── Fetch 65 SMA 30-min distance % in parallel ──
    import indicators as ind
    from concurrent.futures import ThreadPoolExecutor, as_completed

    dist_results = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(ind.calc_sma65_30min_dist, t): t for t in filtered.index}
        for future in as_completed(futures):
            t = futures[future]
            try:
                res = future.result()
                dist_results[t] = res.get('sma65_30m_dist', 0.0)
            except Exception:
                dist_results[t] = 0.0

    filtered['sma65_30m_dist'] = filtered.index.map(dist_results)

    # Sort by 7-Factor (Final_Score) descending
    filtered = filtered.sort_values('Final_Score', ascending=False)

    rows = []
    for ticker, row in zip(filtered.index, filtered.to_dict(orient="records")):
        rows.append({
            "Ticker":       str(ticker),
            "Price":        round(row.get('last_price', 0), 2),
            "7-Factor":     round(row.get('Final_Score', 0), 1),
            "ADR%":         round(row.get('adr_pct', 0) * 100, 2),
            "9EMA Dist%":   round(float(row.get('ema9_dist', 0)) * 100, 2),
            "21EMA Dist%":  round(float(row.get('ema21_dist', 0)) * 100, 2),
            "65SMA 30m%":   round(float(row.get('sma65_30m_dist', 0)) * 100, 2),
            "ATR from MA":  round(float(row.get('atr_dist_50sma', 0)), 1),
            "1D %":         round(row.get('1d_return', 0) * 100, 2),
            "1W %":         round(row.get('1w_return', 0) * 100, 2),
            "1M %":         round(row.get('1m_return', 0) * 100, 2),
            "3M %":         round(row.get('3m_return', 0) * 100, 2),
            "6M %":         round(row.get('6m_return', 0) * 100, 2),
        })

    if not rows:
        return ""

    mp_df    = pd.DataFrame(rows)
    n_mp     = len(mp_df)
    table_id = "momentumPullbackTable"

    mp_table = _build_table_html(
        mp_df, table_id,
        columns=["Ticker", "Price", "7-Factor", "ADR%", "9EMA Dist%", "21EMA Dist%",
                 "65SMA 30m%", "ATR from MA", "1D %", "1W %", "1M %", "3M %", "6M %"],
        formatters={
            "7-Factor": lambda v: f'<span class="score-badge">{_fmt(v, 1)}</span>',
        },
        pct_columns=["9EMA Dist%", "21EMA Dist%", "65SMA 30m%", "1D %", "1W %", "1M %", "3M %", "6M %"]
    )

    subtitle = (
        "Price &gt; $1, ADR &ge; 2.5%, Price &ge; 70% above 52w low, "
        "3M &amp; 6M Perf &gt; 0%, Market Cap &ge; $500M (No Biotech), "
        "Price &gt; 65 EMA Daily, Price &gt; 21 EMA &amp; 30 SMA Weekly. "
        "7-Factor &ge; 60. Sorted by 7-Factor descending."
    )

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_card.html")
    return template.render(
        section_id="momentum-pullback-section",
        title="Recommended Stocks",
        badge_class="badge-gold",
        count_label=f"{n_mp} Setups",
        subtitle=subtitle,
        table_html=mp_table
    )


def _build_momentum_pullback_short_html(display_df):
    """
    Build the Short "Recommended Stocks" (Momentum Pullback Short) section.
    Mirror of the Long version with inverted criteria:
      - Price > 1
      - ADR% >= 2.5%
      - 3m and 6m performance < 0%
      - Market cap > 500M, exclude biotech
      - Price < 65 EMA daily
      - Price < 21 EMA weekly and 30 SMA weekly
      - 7-Factor Short_Score >= 60
    Sorted by Short_Score (or inverted Final_Score) descending.
    Includes distance % from 65 SMA 30min.
    """
    df = display_df.copy()

    required = ['last_price', 'adr_pct', '3m_return', '6m_return',
                'ema65', 'ema21w', 'sma30w', 'ema21_dist', 'ema9_dist']

    for col in required:
        if col not in df.columns:
            df[col] = 0.0

    has_short_score = 'Short_Score' in df.columns
    if has_short_score:
        score_col = 'Short_Score'
        score_min_mask = df['Short_Score'] >= 60
    else:
        score_col = 'Final_Score'
        score_min_mask = df['Final_Score'] <= 40  # inverted: low long score = strong short

    mask = (
        (df['last_price'] > 1.0) &
        (df['adr_pct'] >= 0.025) &
        (df['3m_return'] < 0) &
        (df['6m_return'] < 0) &
        (df['last_price'] < df['ema65']) &
        (df['last_price'] < df['ema21w']) &
        (df['last_price'] < df['sma30w']) &
        score_min_mask
    )
    filtered = df[mask].copy()

    if filtered.empty:
        return ""

    # ── Market cap filter: >= $500M ──
    MIN_MARKET_CAP = 500_000_000
    tickers_to_check = [str(t) for t in filtered.index]
    mkt_caps = _fetch_market_caps(tickers_to_check)
    if mkt_caps:
        qualified = {t for t, cap in mkt_caps.items() if cap >= MIN_MARKET_CAP}
        filtered = filtered[filtered.index.map(str).isin(qualified)].copy()

    if filtered.empty:
        return ""

    # ── Exclude biotech by yfinance industry ──
    filtered = _exclude_biotech(filtered)
    if filtered.empty:
        return ""

    # ── Fetch 65 SMA 30-min distance % in parallel ──
    import indicators as ind
    from concurrent.futures import ThreadPoolExecutor, as_completed

    dist_results = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(ind.calc_sma65_30min_dist, t): t for t in filtered.index}
        for future in as_completed(futures):
            t = futures[future]
            try:
                res = future.result()
                dist_results[t] = res.get('sma65_30m_dist', 0.0)
            except Exception:
                dist_results[t] = 0.0

    filtered['sma65_30m_dist'] = filtered.index.map(dist_results)

    # Sort by Short_Score descending (or Final_Score ascending if no short score)
    if has_short_score:
        filtered = filtered.sort_values('Short_Score', ascending=False)
    else:
        filtered = filtered.sort_values('Final_Score', ascending=True)

    rows = []
    for ticker, row in zip(filtered.index, filtered.to_dict(orient="records")):
        score_val = round(row.get('Short_Score', 100 - row.get('Final_Score', 0)), 1)
        rows.append({
            "Ticker":       str(ticker),
            "Price":        round(row.get('last_price', 0), 2),
            "7-Factor":     score_val,
            "ADR%":         round(row.get('adr_pct', 0) * 100, 2),
            "9EMA Dist%":   round(float(row.get('ema9_dist', 0)) * 100, 2),
            "21EMA Dist%":  round(float(row.get('ema21_dist', 0)) * 100, 2),
            "65SMA 30m%":   round(float(row.get('sma65_30m_dist', 0)) * 100, 2),
            "ATR from MA":  round(float(row.get('atr_dist_50sma', 0)), 1),
            "1D %":         round(row.get('1d_return', 0) * 100, 2),
            "1W %":         round(row.get('1w_return', 0) * 100, 2),
            "1M %":         round(row.get('1m_return', 0) * 100, 2),
            "3M %":         round(row.get('3m_return', 0) * 100, 2),
            "6M %":         round(row.get('6m_return', 0) * 100, 2),
        })

    if not rows:
        return ""

    mp_df    = pd.DataFrame(rows)
    n_mp     = len(mp_df)
    table_id = "shortMomentumPullbackTable"

    mp_table = _build_table_html(
        mp_df, table_id,
        columns=["Ticker", "Price", "7-Factor", "ADR%", "9EMA Dist%", "21EMA Dist%",
                 "65SMA 30m%", "ATR from MA", "1D %", "1W %", "1M %", "3M %", "6M %"],
        formatters={
            "7-Factor": lambda v: f'<span class="score-badge-short">{_fmt(v, 1)}</span>',
        },
        pct_columns=["9EMA Dist%", "21EMA Dist%", "65SMA 30m%", "1D %", "1W %", "1M %", "3M %", "6M %"]
    )

    subtitle = (
        "Price &gt; $1, ADR &ge; 2.5%, "
        "3M &amp; 6M Perf &lt; 0%, Market Cap &ge; $500M (No Biotech), "
        "Price &lt; 65 EMA Daily, Price &lt; 21 EMA &amp; 30 SMA Weekly. "
        "7-Factor &ge; 60. Sorted by 7-Factor descending."
    )

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_card.html")
    return template.render(
        section_id="short-momentum-pullback-section",
        title="Recommended Stocks",
        badge_class="badge-short",
        count_label=f"{n_mp} Setups",
        subtitle=subtitle,
        table_html=mp_table
    )



def _build_vol9m_section_html(display_df, daily_data_map=None):
    """
    Build the "Volume Surge > 9M" screener section.
    Uses pre-computed columns from the indicator loop in scheduler_app.py:
      has_9m_vol, max_vol_25d, days_since_9m.

    Filters stocks that had at least one day with volume > 9,000,000
    in the last 25 trading sessions.
    Sorted by max_vol_25d descending.
    """
    df = display_df.copy()

    if 'has_9m_vol' not in df.columns:
        return ""

    # Filter: has 9M volume in last 25 days
    filtered = df[df['has_9m_vol'] == True].copy()

    if filtered.empty:
        return ""

    # Sort by max volume descending
    filtered = filtered.sort_values('max_vol_25d', ascending=False)

    rows = []
    for ticker, row in zip(filtered.index, filtered.to_dict(orient="records")):
        def _fmt_vol(v):
            if v >= 1_000_000:
                return f"{v/1_000_000:.1f}M"
            elif v >= 1_000:
                return f"{v/1_000:.0f}K"
            return str(v)

        rows.append({
            "Ticker":        str(ticker),
            "Price":         round(row.get('last_price', 0), 2),
            "Max Vol (25d)": row.get('max_vol_25d', 0),
            "Days Ago":      int(row.get('days_since_9m', 99)),
            "ADR%":          round(row.get('adr_pct', 0) * 100, 2),
            "1D %":          round(row.get('1d_return', 0) * 100, 2),
            "1W %":          round(row.get('1w_return', 0) * 100, 2),
            "1M %":          round(row.get('1m_return', 0) * 100, 2),
        })

    if not rows:
        return ""

    vol9m_df = pd.DataFrame(rows)
    n_vol    = len(vol9m_df)
    table_id = "vol9mTable"

    def _fmt_vol_cell(v):
        if pd.isna(v):
            return ""
        v = int(v)
        if v >= 1_000_000:
            return f"{v/1_000_000:.1f}M"
        elif v >= 1_000:
            return f"{v/1_000:.0f}K"
        return str(v)

    vol9m_table = _build_table_html(
        vol9m_df, table_id,
        columns=["Ticker", "Price", "Max Vol (25d)", "Days Ago",
                 "ADR%", "1D %", "1W %", "1M %"],
        formatters={
            "Max Vol (25d)": _fmt_vol_cell,
        },
        pct_columns=["1D %", "1W %", "1M %"]
    )

    subtitle = (
        "Stocks with at least one day where volume exceeded 9,000,000 shares "
        "in the last 25 trading sessions. "
        "Sorted by Max Volume descending."
    )

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_card.html")
    return template.render(
        section_id="vol9m-section",
        title="Volume Surge &gt; 9M",
        badge_class="badge-blue",
        count_label=f"{n_vol} Stocks",
        subtitle=subtitle,
        table_html=vol9m_table
    )


def generate_html_report(display_df, filename="dashboard.html", **kwargs):
    """
    Generates a premium dark-theme HTML dashboard with LONG/SHORT dual-tab system.
    Tab switching via vanilla JS. Long = Green/Blue theme, Short = Red/Orange theme.
    All tables built manually (no pandas to_html). DataTables.js for sorting.
    """
    basket_df = kwargs.get('basket_df', None)
    candidates_df = kwargs.get('candidates_df', None)
    short_basket_df = kwargs.get('short_basket_df', None)
    short_candidates_df = kwargs.get('short_candidates_df', None)

    # ── Sort everything ──
    display_df = display_df.sort_values(by='Final_Score', ascending=False)
    if basket_df is not None and not basket_df.empty:
        basket_df = basket_df.sort_values(by='Avg Score', ascending=False)

    gen_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    session_date = kwargs.get('session_date', None)
    if session_date is not None:
        gen_date = session_date.strftime('%B %d, %Y')
    else:
        gen_date = datetime.now().strftime('%B %d, %Y')
    n_stocks = len(display_df)
    n_cands = len(candidates_df) if candidates_df is not None and not candidates_df.empty else 0
    n_baskets = len(basket_df) if basket_df is not None and not basket_df.empty else 0
    n_short_cands = len(short_candidates_df) if short_candidates_df is not None and not short_candidates_df.empty else 0
    n_short_baskets = len(short_basket_df) if short_basket_df is not None and not short_basket_df.empty else 0

    # Strong basket counts for stats bar
    n_strong_baskets = 0
    if basket_df is not None and not basket_df.empty and 'Avg Score' in basket_df.columns:
        n_strong_baskets = int((basket_df['Avg Score'] >= 60).sum())
    n_strong_short_baskets = 0
    if short_basket_df is not None and not short_basket_df.empty and 'Avg Score' in short_basket_df.columns:
        n_strong_short_baskets = int((short_basket_df['Avg Score'] >= 60).sum())

    import sector_baskets
    display_df = display_df.copy()
    # Build basket map once — used for sector attribution in Full Screener tables
    _ticker_basket_map = sector_baskets.build_ticker_basket_map()

    # ══════════════════════════════════════════════════════
    # ██  LONG SECTIONS
    # ══════════════════════════════════════════════════════

    # ── Long Candidates removed ──
    long_candidate_section = ""

    # ── Long Momentum Pullback Screener ("Recommended Stocks" Long) ──
    long_momentum_pullback_section = _build_momentum_pullback_html(display_df)

    # ── Long High Tight Flag section ──
    daily_data_map = kwargs.get('daily_data_map', None)
    long_vol9m_section = _build_vol9m_section_html(display_df, daily_data_map=daily_data_map)

    # ── Long Recommended — hidden (replaced by Momentum Pullback above) ──
    long_recommended_section = ""

    # ── Long Basket Momentum ──
    long_basket_section = ""

    # ── Long Full Screener ──
    screener_rows = []
    long_sorted = display_df.sort_values(by='Final_Score', ascending=False)
    for rank_i, (ticker, row) in enumerate(zip(long_sorted.index, long_sorted.to_dict(orient="records")), start=1):
        ticker_str = str(ticker)
        screener_rows.append({
            "Rank":   rank_i,
            "Ticker": ticker_str,
            "Price":  round(row.get('last_price', 0), 2),
            "Score":  round(row.get('Final_Score', 0), 1),
            "R²":     round(row.get('r_squared', 0), 2),
            "ATR%":   round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":   round(row.get('adr_pct', 0) * 100, 2),
            "21EMA%": round(float(row.get('ema21_dist', 0)) * 100, 2),
            "30W%":   round(float(row.get('sma30w_dist', 0)) * 100, 2),
            "VWAP%":  round(float(row.get('ipo_vwap_dist', 0)) * 100, 2),
            "ATR×50": round(float(row.get('atr_dist_50sma', 0)), 1),
            "1D %":   round(row.get('1d_return', 0) * 100, 2),
            "1W %":   round(row.get('1w_return', 0) * 100, 2),
            "1M %":   round(row.get('1m_return', 0) * 100, 2),
            "3M %":   round(row.get('3m_return', 0) * 100, 2),
        })

    screener_df = pd.DataFrame(screener_rows)
    screener_cols = ["Rank", "Ticker", "Price", "Score", "R²", "ATR%", "ADR%",
                     "21EMA%", "30W%", "VWAP%", "ATR×50", "1D %", "1W %", "1M %", "3M %"]

    long_screener_table = _build_table_html(
        screener_df, "screenerTable",
        columns=screener_cols,
        formatters={
            "Score": lambda v: f'<span class="score-badge">{_fmt(v, 1)}</span>',
        },
        pct_columns=["21EMA%", "30W%", "VWAP%", "1D %", "1W %", "1M %", "3M %"]
    )

    # ══════════════════════════════════════════════════════
    # ██  SHORT SECTIONS
    # ══════════════════════════════════════════════════════

    # ── Short Candidates removed ──
    short_candidate_section = ""

    # ── Short Momentum Pullback Screener ("Recommended Stocks" Short) ──
    short_momentum_pullback_section = _build_momentum_pullback_short_html(display_df)

    # ── Short Recommended — hidden (replaced by Momentum Pullback Short above) ──
    short_recommended_section = ""

    # ── Short Basket Momentum ──
    short_basket_section = ""

    # ── Short Full Screener ──
    short_screener_rows = []
    has_short_scores = 'Short_Score' in display_df.columns
    short_sorted = display_df.sort_values(by='Short_Score', ascending=False) if has_short_scores else display_df.sort_values(by='Final_Score', ascending=True)

    for rank_i, (ticker, row) in enumerate(zip(short_sorted.index, short_sorted.to_dict(orient="records")), start=1):
        ticker_str = str(ticker)
        short_screener_rows.append({
            "Rank":   rank_i,
            "Ticker": ticker_str,
            "Price":  round(row.get('last_price', 0), 2),
            "Score":  round(row.get('Short_Score', 100 - row.get('Final_Score', 0)), 1),
            "R²":     round(row.get('r_squared', 0), 2),
            "ATR%":   round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":   round(row.get('adr_pct', 0) * 100, 2),
            "21EMA%": round(float(row.get('ema21_dist', 0)) * 100, 2),
            "30W%":   round(float(row.get('sma30w_dist', 0)) * 100, 2),
            "VWAP%":  round(float(row.get('ipo_vwap_dist', 0)) * 100, 2),
            "ATR×50": round(float(row.get('atr_dist_50sma', 0)), 1),
            "1D %":   round(row.get('1d_return', 0) * 100, 2),
            "1W %":   round(row.get('1w_return', 0) * 100, 2),
            "1M %":   round(row.get('1m_return', 0) * 100, 2),
            "3M %":   round(row.get('3m_return', 0) * 100, 2),
        })

    short_screener_df = pd.DataFrame(short_screener_rows)
    short_screener_cols = ["Rank", "Ticker", "Price", "Score", "R²", "ATR%", "ADR%",
                           "21EMA%", "30W%", "VWAP%", "ATR×50", "1D %", "1W %", "1M %", "3M %"]

    short_screener_table = _build_table_html(
        short_screener_df, "short_screenerTable",
        columns=short_screener_cols,
        formatters={
            "Score": lambda v: f'<span class="score-badge-short">{_fmt(v, 1)}</span>',
        },
        pct_columns=["21EMA%", "30W%", "VWAP%", "1D %", "1W %", "1M %", "3M %"]
    )

    # ══════════════════════════════════════════════════════
    # ██  ASSEMBLE FULL HTML VIA JINJA2
    # ══════════════════════════════════════════════════════

    import jinja2
    
    # Calculate pre-computed template variables that shouldn't live in Jinja
    top_score = f"{display_df['Final_Score'].max():.1f}"
    median_score = f"{display_df['Final_Score'].median():.1f}"
    
    if len(short_sorted) > 0:
        top_short_score = f"{short_sorted.iloc[0].get('Short_Score', short_sorted.iloc[0].get('Final_Score', 0)):.1f}"
    else:
        top_short_score = "0.0"
        
    if has_short_scores:
        median_short_score = f"{display_df['Short_Score'].median():.1f}"
    else:
        median_short_score = f"{(100 - display_df['Final_Score'].median()):.1f}"

    # Prepare Jinja environment
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")),
        autoescape=jinja2.select_autoescape(['html', 'xml'])
    )
    
    # Render layout
    template = env.get_template("layout.html")
    html_content = template.render(
        gen_date=gen_date,
        gen_time=gen_time,
        n_stocks=n_stocks,
        n_cands=n_cands,
        n_strong_baskets=n_strong_baskets,
        n_short_cands=n_short_cands,
        n_strong_short_baskets=n_strong_short_baskets,
        top_score=top_score,
        median_score=median_score,
        top_short_score=top_short_score,
        median_short_score=median_short_score,
        long_candidate_section=long_candidate_section,
        long_momentum_pullback_section=long_momentum_pullback_section,
        long_htf_section=long_vol9m_section,
        long_recommended_section=long_recommended_section,
        long_basket_section=long_basket_section,
        long_screener_table=long_screener_table,
        short_candidate_section=short_candidate_section,
        short_momentum_pullback_section=short_momentum_pullback_section,
        short_recommended_section=short_recommended_section,
        short_basket_section=short_basket_section,
        short_screener_table=short_screener_table
    )

    with open(filename, "w", encoding='utf-8') as f:
        f.write(html_content)

    return os.path.abspath(filename)


if __name__ == "__main__":
    dummy_df = pd.DataFrame({
        'Ticker': ['AAPL', 'MSFT'],
        'Final_Score': [99.5, 98.2],
        'Short_Score': [10.5, 12.2],
        '3m_return': [0.123, 0.234],
        '1m_return': [0.05, 0.08],
        '1w_return': [0.01, 0.02],
        '3d_return': [0.005, 0.01],
        'Score_Price': [85.0, 90.0],
        'Score_RS': [80.0, 75.0],
        'Score_Candles': [70.0, 65.0],
        'Score_MA': [60.0, 55.0],
        'Score_Trend': [50.0, 45.0],
        'Score_Vol': [40.0, 35.0],
        'Score_Volume': [30.0, 25.0],
        'Short_Price': [15.0, 10.0],
        'Short_RS': [20.0, 25.0],
        'Short_Candles': [30.0, 35.0],
        'Short_MA': [40.0, 45.0],
        'Short_Trend': [50.0, 55.0],
        'Short_Vol': [60.0, 65.0],
        'Short_Volume': [70.0, 75.0],
    }).set_index('Ticker')
    path = generate_html_report(dummy_df)
    print(f"Report generated at: {path}")
