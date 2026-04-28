
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
    # Use max_workers=20 to speed up since yfinance is fast
    return data_fetcher.fetch_market_caps(tickers, max_workers=20)


def _build_recommended_html(display_df, basket_df, mode="long"):
    """
    Build "Recommended Stocks" section with DEEP sector verification.
    mode='long': top stocks (Score >= 65, positive returns)
    mode='short': worst stocks (Short_Score >= 65 or Final_Score <= 35, negative returns)
    """
    import sector_baskets
    ticker_map = sector_baskets.build_ticker_basket_map()

    if basket_df is None or basket_df.empty:
        return ""

    df = display_df.copy()
    prefix = "short_" if mode == "short" else ""
    table_id = f"{prefix}recommendedTable"

    if mode == "short":
        score_col = 'Short_Score' if 'Short_Score' in df.columns else 'Final_Score'
        if score_col == 'Short_Score':
            mask_score = df['Short_Score'] >= 65
        else:
            mask_score = df['Final_Score'] <= 35

        mask_returns = (
            (df['3m_return'] < 0).astype(int) +
            (df['1m_return'] < 0).astype(int) +
            (df['1w_return'] < 0).astype(int) +
            (df['3d_return'] < 0).astype(int)
        ) >= 1
    else:
        score_col = 'Final_Score'
        mask_score = df['Final_Score'] >= 65
        mask_returns = (
            (df['3m_return'] > 0).astype(int) +
            (df['1m_return'] > 0).astype(int) +
            (df['1w_return'] > 0).astype(int) +
            (df['3d_return'] > 0).astype(int)
        ) >= 1

    pre_filtered = df[mask_score & mask_returns].copy()

    if pre_filtered.empty:
        return ""

    # PRICE FILTER: removed — no price constraint

    if pre_filtered.empty:
        return ""

    # ATR% FILTER: exclude stocks with ATR% < 2% (too illiquid/tight)
    MIN_ATR_PCT = 0.02
    if 'atr_pct' in pre_filtered.columns:
        pre_filtered = pre_filtered[pre_filtered['atr_pct'] >= MIN_ATR_PCT].copy()
    if pre_filtered.empty:
        return ""

    # MARKET CAP FILTER: >= $500M
    MIN_MARKET_CAP = 500_000_000  # $500M
    sort_col = 'Short_Score' if (mode == "short" and score_col == 'Short_Score') else 'Final_Score'
    ascending = (mode == "short" and score_col != 'Short_Score')
    top_df = pre_filtered.sort_values(sort_col, ascending=ascending).head(300)
    tickers_to_check = [str(t) for t in top_df.index]
    mkt_caps = _fetch_market_caps(tickers_to_check)
    if mkt_caps:
        qualified = {t for t, cap in mkt_caps.items() if cap >= MIN_MARKET_CAP}
        pre_filtered = top_df[top_df.index.map(str).isin(qualified)].copy()
    else:
        pre_filtered = top_df.copy()

    if pre_filtered.empty:
        return ""

    if mode == "short":
        if score_col == 'Short_Score':
            pre_filtered = pre_filtered.sort_values('Short_Score', ascending=False)
        else:
            pre_filtered = pre_filtered.sort_values('Final_Score', ascending=True)
    else:
        pre_filtered = pre_filtered.sort_values('Final_Score', ascending=False)

    rows = []
    for ticker, row in zip(pre_filtered.index, pre_filtered.to_dict(orient="records")):
        ticker_str = str(ticker)
        sector_label, source = sector_baskets.get_deep_sector(ticker_str, ticker_map)

        if sector_label == "Unclassified":
            continue

        if mode == "short":
            score_val = round(row.get('Short_Score', row.get('Final_Score', 0)), 1)
        else:
            score_val = round(row['Final_Score'], 1)

        rows.append({
            "Ticker": ticker_str,
            "Price":  round(row.get('last_price', 0), 2),
            "Score":  score_val,
            "R²":     round(row.get('r_squared', 0) * 100, 1),
            "ATR%":   round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":   round(row.get('adr_pct', 0) * 100, 2),
            "1D %":   round(row.get('1d_return', 0) * 100, 2),
            "1W %":   round(row.get('1w_return', 0) * 100, 2),
            "1M %":   round(row.get('1m_return', 0) * 100, 2),
            "3M %":   round(row.get('3m_return', 0) * 100, 2),
        })

    if not rows:
        return ""

    rec_df = pd.DataFrame(rows)
    n_rec = len(rec_df)

    score_badge_class = "score-badge-short" if mode == "short" else "score-badge"
    rec_df = rec_df.sort_values("Score", ascending=False).reset_index(drop=True)
    rec_df.insert(0, "Rank", rec_df.index + 1)
    rec_table = _build_table_html(
        rec_df, table_id,
        columns=["Rank", "Ticker", "Price", "Score", "R²", "ATR%", "ADR%", "1D %", "1W %", "1M %", "3M %"],
        formatters={
            "Score": lambda v: f'<span class="{score_badge_class}">{_fmt(v, 1)}</span>',
        },
        pct_columns=["1D %", "1W %", "1M %", "3M %"]
    )

    if mode == "short":
        icon = ""
        title = "Short Recommended"
        subtitle = ("Stocks with Short Score &ge; 65, market cap &ge; $500M, "
                    "at least 1 negative timeframe, and a verified sector (basket map lookup). "
                    "Best candidates for short selling.")
        badge_class = "badge-short"
    else:
        icon = ""
        title = "Recommended Stocks"
        subtitle = ("Stocks with Score &ge; 65, market cap &ge; $500M, "
                    "at least 1 positive timeframe, and a verified sector (basket map lookup). "
                    "All columns are sortable.")
        badge_class = "badge-gold"

    section_id = f"{prefix}recommended-section"

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_card.html")
    return template.render(
        section_id=section_id,
        title=title,
        badge_class=badge_class,
        count_label=f"{n_rec} Stocks",
        subtitle=subtitle,
        table_html=rec_table
    )


def _build_trend_continuation_html(display_df, mode="long"):
    """
    Build the "Trend Continuation Setups" section.
    Source: top 300 stocks by score.
    Criteria: long  — Final_Score >= 70 AND R² >= 0.80
              short — Short_Score >= 60 AND R² >= 0.80
    Sort: ascending by |Distance from 21 EMA| — stocks nearest to their EMA first.
    mode='long'  → green theme, long candidates
    mode='short' → red theme,   short candidates
    """
    df = display_df.copy()

    if mode == "short":
        score_col = 'Short_Score' if 'Short_Score' in df.columns else 'Final_Score'
    else:
        score_col = 'Final_Score'

    # Top 300 by score
    top300 = df.sort_values(score_col, ascending=False).head(300)

    # Filter: long >= 70, short >= 60; both require R² >= 0.80
    score_threshold = 60 if mode == "short" else 70
    mask = (top300[score_col] >= score_threshold) & (top300['r_squared'] >= 0.80)
    filtered = top300[mask].copy()

    if filtered.empty:
        return ""

    # ema21_dist is stored as fraction (e.g. 0.03 = 3%)
    if 'ema21_dist' not in filtered.columns:
        return ""

    # ── Market cap filter: >= $500M ─────────────────────────────────────
    MIN_MARKET_CAP = 500_000_000
    tickers_to_check = [str(t) for t in filtered.index]
    mkt_caps = _fetch_market_caps(tickers_to_check)
    if mkt_caps:
        qualified = {t for t, cap in mkt_caps.items() if cap >= MIN_MARKET_CAP}
        filtered = filtered[filtered.index.map(str).isin(qualified)].copy()
    if filtered.empty:
        return ""

    filtered['_abs_dist'] = filtered['ema21_dist'].abs()
    filtered = filtered.sort_values('_abs_dist', ascending=True)

    rows = []
    for ticker, row in zip(filtered.index, filtered.to_dict(orient="records")):
        score_val = round(row.get(score_col, 0), 1)
        dist_pct  = round(float(row.get('ema21_dist', 0)) * 100, 2)
        rows.append({
            "Ticker":          str(ticker),
            "Price":           round(row.get('last_price', 0), 2),
            "7-Factor":        score_val,
            "R²":              round(row.get('r_squared', 0) * 100, 1),
            "ATR%":            round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":            round(row.get('adr_pct', 0) * 100, 2),
            "21EMA Dist%":     dist_pct,
            "1D %":            round(row.get('1d_return', 0) * 100, 2),
            "1W %":            round(row.get('1w_return', 0) * 100, 2),
            "1M %":            round(row.get('1m_return', 0) * 100, 2),
        })

    if not rows:
        return ""

    tc_df = pd.DataFrame(rows)
    n_tc  = len(tc_df)

    if mode == "short":
        table_id    = "short_trendContTable"
        badge_class = "badge-short"
        score_badge = "score-badge-short"
        title       = "Short Trend Continuation Setups"
        subtitle    = ("Stocks from top 300 Short Score with 7-Factor &ge; 60, R&sup2; &ge; 80, "
                       "Market Cap &ge; $500M. "
                       "Sorted by proximity to 21 EMA &mdash; closest first. "
                       "Ideal candidates showing orderly reversion toward key moving average.")
        section_id  = "short_trend-continuation-section"
    else:
        table_id    = "trendContTable"
        badge_class = "badge-gold"
        score_badge = "score-badge"
        title       = "Trend Continuation Setups"
        subtitle    = ("Stocks from top 300 with 7-Factor &ge; 70, R&sup2; &ge; 80, "
                       "Market Cap &ge; $500M. "
                       "Sorted by proximity to 21 EMA &mdash; closest first. "
                       "Orderly pullback or squeeze near key moving average.")
        section_id  = "trend-continuation-section"

    tc_table = _build_table_html(
        tc_df, table_id,
        columns=["Ticker", "Price", "7-Factor", "R²", "ATR%", "ADR%",
                 "21EMA Dist%", "1D %", "1W %", "1M %"],
        formatters={
            "7-Factor": lambda v: f'<span class="{score_badge}">{_fmt(v, 1)}</span>',
        },
        pct_columns=["21EMA Dist%", "1D %", "1W %", "1M %"]
    )

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_card.html")
    return template.render(
        section_id=section_id,
        title=title,
        badge_class=badge_class,
        count_label=f"{n_tc} Setups",
        subtitle=subtitle,
        table_html=tc_table
    )


def _build_trend_reversals_html(display_df, mode="long"):
    """
    Build the "Trend Reversal" section replacing the old basket breakdown.
    Criteria (long only):
      - 7-Factor >= 40
      - ADR >= 3%
      - Market Cap >= 700M
      - Close > 21 EMA daily
    Sorted by R2 (21 days) descending to find tight new trends.
    """
    if mode == "short":
        return ""  # Only implementing long trend reversals for now

    df = display_df.copy()
    
    # Base filters
    if 'ema21_dist' not in df.columns or 'r_squared_21d' not in df.columns:
        return ""
        
    mask = (df['Final_Score'] >= 40) & (df['adr_pct'] >= 0.03) & (df['ema21_dist'] > 0)
    filtered = df[mask].copy()

    if filtered.empty:
        return ""

    # ── Market cap filter: >= $700M ─────────────────────────────────────
    MIN_MARKET_CAP = 700_000_000
    tickers_to_check = [str(t) for t in filtered.index]
    mkt_caps = _fetch_market_caps(tickers_to_check)
    if mkt_caps:
        qualified = {t for t, cap in mkt_caps.items() if cap >= MIN_MARKET_CAP}
        filtered = filtered[filtered.index.map(str).isin(qualified)].copy()
        
    if filtered.empty:
        return ""

    # Sort by 21-day R-squared descending
    filtered = filtered.sort_values('r_squared_21d', ascending=False).head(30)

    rows = []
    for ticker, row in zip(filtered.index, filtered.to_dict(orient="records")):
        rows.append({
            "Ticker":          str(ticker),
            "Price":           round(row.get('last_price', 0), 2),
            "7-Factor":        round(row.get('Final_Score', 0), 1),
            "R² (21d)":        round(row.get('r_squared_21d', 0) * 100, 1),
            "ATR%":            round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":            round(row.get('adr_pct', 0) * 100, 2),
            "21EMA Dist%":     round(float(row.get('ema21_dist', 0)) * 100, 2),
            "1D %":            round(row.get('1d_return', 0) * 100, 2),
            "1W %":            round(row.get('1w_return', 0) * 100, 2),
            "1M %":            round(row.get('1m_return', 0) * 100, 2),
        })

    if not rows:
        return ""

    tr_df = pd.DataFrame(rows)
    n_tr  = len(tr_df)

    table_id    = "trendReversalsTable"
    badge_class = "badge-basket"
    score_badge = "score-badge"
    title       = "Trend Reversals"
    subtitle    = ("Stocks with 7-Factor &ge; 40, ADR &ge; 3%, Market Cap &ge; $700M, "
                   "and Price closing above the 21 EMA. "
                   "Sorted by highest 21-day R&sup2; to highlight tight emerging uptrends.")
    section_id  = "trend-reversals-section"

    tr_table = _build_table_html(
        tr_df, table_id,
        columns=["Ticker", "Price", "7-Factor", "R² (21d)", "ATR%", "ADR%",
                 "21EMA Dist%", "1D %", "1W %", "1M %"],
        formatters={
            "7-Factor": lambda v: f'<span class="{score_badge}">{_fmt(v, 1)}</span>',
        },
        pct_columns=["21EMA Dist%", "1D %", "1W %", "1M %"]
    )

    import jinja2
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(script_dir, "templates")))
    template = env.get_template("report_card.html")
    return template.render(
        section_id=section_id,
        title=title,
        badge_class=badge_class,
        count_label=f"{n_tr} Setups",
        subtitle=subtitle,
        table_html=tr_table
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

    # ── Long Trend Continuation Setups ──
    long_trend_continuation_section = _build_trend_continuation_html(display_df, mode="long")

    # ── Long Recommended ──
    long_recommended_section = _build_recommended_html(display_df, basket_df, mode="long")

    # ── Long Basket Momentum ──
    long_basket_section = ""

    # ── Long Trend Reversals ──
    long_basket_detail_section = _build_trend_reversals_html(display_df, mode="long")

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
            "R²":     round(row.get('r_squared', 0) * 100, 1),
            "ATR%":   round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":   round(row.get('adr_pct', 0) * 100, 2),
            "1D %":   round(row.get('1d_return', 0) * 100, 2),
            "1W %":   round(row.get('1w_return', 0) * 100, 2),
            "1M %":   round(row.get('1m_return', 0) * 100, 2),
            "3M %":   round(row.get('3m_return', 0) * 100, 2),
        })

    screener_df = pd.DataFrame(screener_rows)
    screener_cols = ["Rank", "Ticker", "Price", "Score", "R²", "ATR%", "ADR%", "1D %", "1W %", "1M %", "3M %"]

    long_screener_table = _build_table_html(
        screener_df, "screenerTable",
        columns=screener_cols,
        formatters={
            "Score": lambda v: f'<span class="score-badge">{_fmt(v, 1)}</span>',
        },
        pct_columns=["1D %", "1W %", "1M %", "3M %"]
    )

    # ══════════════════════════════════════════════════════
    # ██  SHORT SECTIONS
    # ══════════════════════════════════════════════════════

    # ── Short Candidates removed ──
    short_candidate_section = ""

    # ── Short Trend Continuation Setups ──
    short_trend_continuation_section = _build_trend_continuation_html(display_df, mode="short")

    # ── Short Recommended ──
    effective_short_basket_for_rec = short_basket_df if (short_basket_df is not None and not short_basket_df.empty) else basket_df
    short_recommended_section = _build_recommended_html(display_df, effective_short_basket_for_rec, mode="short")

    # ── Short Basket Momentum ──
    short_basket_section = ""

    # ── Short Trend Reversals (Not implemented yet) ──
    short_basket_detail_section = ""

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
            "R²":     round(row.get('r_squared', 0) * 100, 1),
            "ATR%":   round(row.get('atr_pct', 0) * 100, 2),
            "ADR%":   round(row.get('adr_pct', 0) * 100, 2),
            "1D %":   round(row.get('1d_return', 0) * 100, 2),
            "1W %":   round(row.get('1w_return', 0) * 100, 2),
            "1M %":   round(row.get('1m_return', 0) * 100, 2),
            "3M %":   round(row.get('3m_return', 0) * 100, 2),
        })

    short_screener_df = pd.DataFrame(short_screener_rows)
    short_screener_cols = ["Rank", "Ticker", "Price", "Score", "R²", "ATR%", "ADR%", "1D %", "1W %", "1M %", "3M %"]

    short_screener_table = _build_table_html(
        short_screener_df, "short_screenerTable",
        columns=short_screener_cols,
        formatters={
            "Score": lambda v: f'<span class="score-badge-short">{_fmt(v, 1)}</span>',
        },
        pct_columns=["1D %", "1W %", "1M %", "3M %"]
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
        long_trend_continuation_section=long_trend_continuation_section,
        long_recommended_section=long_recommended_section,
        long_basket_section=long_basket_section,
        long_basket_detail_section=long_basket_detail_section,
        long_screener_table=long_screener_table,
        short_candidate_section=short_candidate_section,
        short_trend_continuation_section=short_trend_continuation_section,
        short_recommended_section=short_recommended_section,
        short_basket_section=short_basket_section,
        short_basket_detail_section=short_basket_detail_section,
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
