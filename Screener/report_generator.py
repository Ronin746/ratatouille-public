
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

    # Header
    header_cells = ""
    if index:
        header_cells += "<th>Ticker</th>"
    for col in columns:
        header_cells += f"<th>{col}</th>"

    # Rows
    rows_html = ""
    for idx, row in df.iterrows():
        cells = ""
        if index:
            cells += f"<td><strong>{idx}</strong></td>"
        for col in columns:
            val = row[col]
            if col in pct_columns:
                cells += _color_pct_cell(val)
            elif col in formatters:
                cells += f"<td>{formatters[col](val)}</td>"
            else:
                cells += f"<td>{_fmt(val)}</td>"
        rows_html += f"<tr>{cells}</tr>\n"

    return f"""<table id="{table_id}" class="display compact" style="width:100%">
<thead><tr>{header_cells}</tr></thead>
<tbody>
{rows_html}
</tbody>
</table>"""


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


def _fetch_market_caps_finviz(tickers, chunk_size=100):
    """
    Fetch market caps from Finviz screener for a list of tickers.
    Returns dict {ticker: market_cap_float}. Falls back to empty dict on error.
    Requires: pip install finvizfinance
    """
    result = {}
    if not tickers:
        return result
    try:
        from finvizfinance.screener.overview import Overview
    except ImportError:
        return result

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            fov = Overview()
            fov.set_filter(ticker=','.join(chunk))
            df_fv = fov.screener_view()
            if df_fv is not None and not df_fv.empty:
                for _, r in df_fv.iterrows():
                    t = str(r.get('Ticker', '')).strip()
                    if t:
                        result[t] = _parse_market_cap(r.get('Market Cap', ''))
        except Exception:
            pass

    return result


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

    # PRICE FILTER: min $10 for long, $30–$200 for short
    if mode == "short":
        pre_filtered = pre_filtered[
            pre_filtered['last_price'].between(30.0, 200.0)
        ].copy()
    else:
        pre_filtered = pre_filtered[
            pre_filtered['last_price'] >= 10.0
        ].copy()

    if pre_filtered.empty:
        return ""

    # MARKET CAP FILTER via Finviz: > $1B — applied to top 300 by score
    MIN_MARKET_CAP = 1_000_000_000  # $1B
    sort_col = 'Short_Score' if (mode == "short" and score_col == 'Short_Score') else 'Final_Score'
    ascending = (mode == "short" and score_col != 'Short_Score')
    top_df = pre_filtered.sort_values(sort_col, ascending=ascending).head(300)
    tickers_to_check = [str(t) for t in top_df.index]
    mkt_caps = _fetch_market_caps_finviz(tickers_to_check)
    if mkt_caps:
        qualified = {t for t, cap in mkt_caps.items() if cap >= MIN_MARKET_CAP}
        pre_filtered = top_df[top_df.index.map(str).isin(qualified)].copy()
    else:
        # Finviz non disponibile — procedi senza filtro market cap
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
    for ticker, row in pre_filtered.iterrows():
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
            "R²":     round(row.get('r_squared', 0), 2),
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
        pct_columns=["ATR%", "ADR%", "1D %", "1W %", "1M %", "3M %"]
    )

    if mode == "short":
        icon = ""
        title = "Short Recommended"
        subtitle = ("Stocks with Short Score &ge; 65, market cap &ge; $1B, price $30&ndash;$200, "
                    "at least 1 negative timeframe, and a verified sector (basket map lookup). "
                    "Best candidates for short selling.")
        badge_class = "badge-short"
    else:
        icon = ""
        title = "Recommended Stocks"
        subtitle = ("Stocks with Score &ge; 65, market cap &ge; $1B, price &ge; $10, "
                    "at least 1 positive timeframe, and a verified sector (basket map lookup). "
                    "All columns are sortable.")
        badge_class = "badge-gold"

    section_id = f"{prefix}recommended-section"

    return f"""
    <section class="card" id="{section_id}">
        <div class="card-header">
            <div class="card-title">
                <h2>{title}</h2>
            </div>
            <span class="card-badge {badge_class}">{n_rec} Stocks</span>
        </div>
        <p class="card-subtitle">{subtitle}</p>
        <div class="card-body">{rec_table}</div>
    </section>"""


def _build_basket_detail_sections(display_df, basket_df, mode="long"):
    """
    Build per-basket TOP 5 tables (long) or BOTTOM 5 tables (short).
    Each basket gets its own card with a table.
    """
    import sector_baskets

    prefix = "short_" if mode == "short" else ""

    if mode == "short":
        basket_top = sector_baskets.get_basket_bottom_stocks(display_df, top_n=5)
        score_col = 'Short_Score' if 'Short_Score' in display_df.columns else 'Final_Score'
    else:
        basket_top = sector_baskets.get_basket_top_stocks(display_df, top_n=5)
        score_col = 'Final_Score'

    if not basket_top:
        return ""

    basket_order = []
    if basket_df is not None and not basket_df.empty:
        basket_order = basket_df['Basket'].tolist()

    for b in basket_top:
        if b not in basket_order:
            basket_order.append(b)

    sections = []
    table_counter = 0

    for bname in basket_order:
        if bname not in basket_top:
            continue
        top_stocks = basket_top[bname]
        if top_stocks.empty:
            continue

        table_counter += 1
        tid = f"{prefix}basketDetail{table_counter}"

        rows = []
        for ticker, row in top_stocks.iterrows():
            rows.append({
                "Ticker": str(ticker),
                "Score": round(row.get(score_col, 0), 1),
                "R²":    round(row.get('r_squared', 0), 2),
                "1D %": round(row.get('1d_return', 0) * 100, 2),
                "3D %": round(row.get('3d_return', 0) * 100, 2),
                "1W %": round(row.get('1w_return', 0) * 100, 2),
                "1M %": round(row.get('1m_return', 0) * 100, 2),
            })

        bdf = pd.DataFrame(rows)
        n_stocks = len(bdf)

        score_badge_class = "score-badge-short" if mode == "short" else "score-badge"
        table_html = _build_table_html(
            bdf, tid,
            columns=["Ticker", "Score", "R²", "1D %", "3D %", "1W %", "1M %"],
            formatters={
                "Score": lambda v: f'<span class="{score_badge_class}">{_fmt(v, 1)}</span>',
            },
            pct_columns=["1D %", "3D %", "1W %", "1M %"]
        )

        label = f"Top {n_stocks}" if mode == "long" else f"Worst {n_stocks}"
        toggle_fn = f"toggleBasket{'Short' if mode == 'short' else ''}(this)"

        sections.append(f"""
        <div class="basket-detail-card">
            <div class="basket-detail-header" onclick="{toggle_fn}">
                <div class="basket-detail-name">{bname}</div>
                <div class="basket-detail-meta">
                    <span class="tag tag-dim">{label}</span>
                    <span class="basket-toggle">&#x25BC;</span>
                </div>
            </div>
            <div class="basket-detail-body">
                {table_html}
            </div>
        </div>""")

    if not sections:
        return ""

    all_sections = "\n".join(sections)

    if mode == "short":
        title = "Basket Breakdown &mdash; Worst 5 per Sector"
        subtitle = "Worst 5 stocks per basket, sorted by Short Score (descending). Best short candidates by sector."
        badge_class = "badge-short"
        section_id = "short_basket-details-section"
    else:
        title = "Basket Breakdown &mdash; Top 5 per Sector"
        subtitle = "Top 5 stocks per basket, sorted by Score (descending). Only baskets with stocks found in the current scan are shown."
        badge_class = "badge-basket"
        section_id = "basket-details-section"

    return f"""
    <section class="card" id="{section_id}">
        <div class="card-header">
            <div class="card-title">
                <h2>{title}</h2>
            </div>
            <span class="card-badge {badge_class}">{table_counter} Active Baskets</span>
        </div>
        <p class="card-subtitle">{subtitle}</p>
        <div class="card-body basket-detail-grid">
            {all_sections}
        </div>
    </section>"""


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

    # ── Long Candidate Table ──
    long_candidate_section = ""
    if candidates_df is not None and not candidates_df.empty:
        all_cols = list(candidates_df.columns)
        cand_cols = ["Ticker"] + [c for c in all_cols if c not in ("Ticker", "Sector")]
        sort_col = "Overall Score" if "Overall Score" in candidates_df.columns else cand_cols[1] if len(cand_cols) > 1 else None
        if sort_col:
            candidates_df = candidates_df.sort_values(sort_col, ascending=False)
        cand_table = _build_table_html(
            candidates_df, "candidateTable",
            columns=cand_cols,
            formatters={
                "Overall Score": lambda v: f'<span class="score-badge">{_fmt(v, 1)}</span>',
            },
            pct_columns=["1D %", "3D %", "1W %", "1M %", "21EMA%"]
        )
        long_candidate_section = f"""
    <section class="card card-highlight-long" id="candidates-section">
        <div class="card-header">
            <div class="card-title">
                <h2>Long Candidates</h2>
            </div>
            <span class="card-badge badge-main">{n_cands} Candidate{'s' if n_cands != 1 else ''}</span>
        </div>
        <p class="card-subtitle">
            Top-ranked stocks from the Long pipeline (Score &ge; 6.0, Price &gt; $10).
            SCORE10 = (align&times;0.20 + cross&times;0.30 + vcp&times;0.30 + r&sup2;&times;0.20) &divide; 3 &times; 10 &mdash;
            filtered by Market Cap &ge; $1B, Price &gt; 4&times;ATR from 50 SMA, weekly uptrend confirmed.
        </p>
        <div class="card-body">{cand_table}</div>
    </section>"""

    # ── Long Recommended ──
    long_recommended_section = _build_recommended_html(display_df, basket_df, mode="long")

    # ── Long Basket Momentum ──
    long_basket_section = ""
    if basket_df is not None and not basket_df.empty:
        basket_table = _build_table_html(
            basket_df, "basketTable",
            formatters={
                'Avg Score': lambda v: f'<span class="score-badge">{_fmt(v, 2)}</span>',
            },
            pct_columns=["3M %", "1M %", "1W %", "3D %"]
        )
        long_basket_section = f"""
    <section class="card" id="baskets-section">
        <div class="card-header">
            <div class="card-title">
                <h2>Sector &amp; Theme Momentum</h2>
            </div>
            <span class="card-badge badge-basket">{n_baskets} Baskets</span>
        </div>
        <p class="card-subtitle">
            Aggregate performance by sector. Only baskets with stocks found in the current scan are shown.
        </p>
        <div class="card-body">{basket_table}</div>
    </section>"""

    # ── Long Basket Details (Top 5) ──
    long_basket_detail_section = _build_basket_detail_sections(display_df, basket_df, mode="long")

    # ── Long Full Screener ──
    screener_rows = []
    long_sorted = display_df.sort_values(by='Final_Score', ascending=False)
    for rank_i, (ticker, row) in enumerate(long_sorted.iterrows(), start=1):
        ticker_str = str(ticker)
        screener_rows.append({
            "Rank":   rank_i,
            "Ticker": ticker_str,
            "Price":  round(row.get('last_price', 0), 2),
            "Score":  round(row.get('Final_Score', 0), 1),
            "R²":     round(row.get('r_squared', 0), 2),
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
        pct_columns=["ATR%", "ADR%", "1D %", "1W %", "1M %", "3M %"]
    )

    # ══════════════════════════════════════════════════════
    # ██  SHORT SECTIONS
    # ══════════════════════════════════════════════════════

    # ── Short Candidate Table ──
    short_candidate_section = ""
    if short_candidates_df is not None and not short_candidates_df.empty:
        all_cols_s = list(short_candidates_df.columns)
        scand_cols = ["Ticker"] + [c for c in all_cols_s if c not in ("Ticker", "Sector")]
        sort_col_s = "Overall Score" if "Overall Score" in short_candidates_df.columns else scand_cols[1] if len(scand_cols) > 1 else None
        if sort_col_s:
            short_candidates_df = short_candidates_df.sort_values(sort_col_s, ascending=False)
        scand_table = _build_table_html(
            short_candidates_df, "short_candidateTable",
            columns=scand_cols,
            formatters={
                "Overall Score": lambda v: f'<span class="score-badge-short">{_fmt(v, 1)}</span>',
            },
            pct_columns=["1D %", "3D %", "1W %", "1M %", "21EMA%"]
        )
        short_candidate_section = f"""
    <section class="card card-highlight-short" id="short_candidates-section">
        <div class="card-header">
            <div class="card-title">
                <h2>Short Candidates</h2>
            </div>
            <span class="card-badge badge-short">{n_short_cands} Candidate{'s' if n_short_cands != 1 else ''}</span>
        </div>
        <p class="card-subtitle">
            Top-ranked stocks from the Short pipeline (Score &ge; 6.0, Price &gt; $30).
            Inverted SCORE10 &mdash; highest scores indicate the weakest technicals.
            Filtered by Market Cap &ge; $1B; weekly downtrend and distribution confirmed.
        </p>
        <div class="card-body">{scand_table}</div>
    </section>"""

    # ── Short Recommended ──
    effective_short_basket_for_rec = short_basket_df if (short_basket_df is not None and not short_basket_df.empty) else basket_df
    short_recommended_section = _build_recommended_html(display_df, effective_short_basket_for_rec, mode="short")

    # ── Short Basket Momentum ──
    short_basket_section = ""
    effective_short_basket = short_basket_df if short_basket_df is not None and not short_basket_df.empty else None
    if effective_short_basket is not None:
        sbasket_table = _build_table_html(
            effective_short_basket, "short_basketTable",
            formatters={
                'Avg Score': lambda v: f'<span class="score-badge-short">{_fmt(v, 2)}</span>',
            },
            pct_columns=["3M %", "1M %", "1W %", "3D %"]
        )
        short_basket_section = f"""
    <section class="card" id="short_baskets-section">
        <div class="card-header">
            <div class="card-title">
                <h2>Weakest Sectors &amp; Themes</h2>
            </div>
            <span class="card-badge badge-short">{n_short_baskets} Baskets</span>
        </div>
        <p class="card-subtitle">
            Sector baskets sorted by weakness. Worst performing sectors first — ideal for identifying short opportunities.
        </p>
        <div class="card-body">{sbasket_table}</div>
    </section>"""

    # ── Short Basket Details (Worst 5) ──
    short_basket_detail_section = _build_basket_detail_sections(display_df, effective_short_basket, mode="short")

    # ── Short Full Screener ──
    short_screener_rows = []
    has_short_scores = 'Short_Score' in display_df.columns
    short_sorted = display_df.sort_values(by='Short_Score', ascending=False) if has_short_scores else display_df.sort_values(by='Final_Score', ascending=True)

    for rank_i, (ticker, row) in enumerate(short_sorted.iterrows(), start=1):
        ticker_str = str(ticker)
        short_screener_rows.append({
            "Rank":   rank_i,
            "Ticker": ticker_str,
            "Price":  round(row.get('last_price', 0), 2),
            "Score":  round(row.get('Short_Score', 100 - row.get('Final_Score', 0)), 1),
            "R²":     round(row.get('r_squared', 0), 2),
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
        pct_columns=["ATR%", "ADR%", "1D %", "1W %", "1M %", "3M %"]
    )

    # ══════════════════════════════════════════════════════
    # ██  ASSEMBLE FULL HTML
    # ══════════════════════════════════════════════════════

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ratatouille Screener &mdash; {gen_date}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.3.6/css/buttons.dataTables.min.css">
<style>
/* ══════════════════════════════════════════════════════════
   MASTER TEMPLATE — Unified visual style
   ══════════════════════════════════════════════════════════ */
:root {{
    --bg-primary: #0a0a0f;
    --bg-card: #111118;
    --bg-card-hover: #1a1a26;
    --bg-input: #1a1a28;
    --border-subtle: #1c1c2c;
    --border-accent: #2a2a3c;
    --text-primary: #e8e8f0;
    --text-secondary: #8888a0;
    --text-muted: #555570;
    --accent-green: #00d4aa;
    --accent-green-dim: rgba(0,212,170,0.10);
    --accent-gold: #f5a623;
    --accent-gold-dim: rgba(245,166,35,0.10);
    --accent-blue: #4a9eff;
    --accent-red: #ff4a6a;
    --accent-red-dim: rgba(255,74,106,0.10);
    --accent-orange: #ff8c42;
    --accent-orange-dim: rgba(255,140,66,0.10);
    --accent-amaranth: #e23d6d;
    --accent-purple: #8b5cf6;
    --radius: 14px;
    --radius-sm: 8px;
    --shadow: 0 2px 16px rgba(0,0,0,0.35);
    --shadow-lg: 0 8px 40px rgba(0,0,0,0.5);
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.6;
}}

/* ══════════════════════════════════════════
   TAB SYSTEM — Long / Short Toggle
   ══════════════════════════════════════════ */
.tab-bar {{
    display:flex; justify-content:center; gap:12px;
    padding:20px 40px 0;
    max-width:1600px; margin:0 auto;
}}
.tab-btn {{
    padding:14px 40px; border:2px solid transparent; border-radius:var(--radius);
    font-size:1rem; font-weight:700; cursor:pointer;
    transition:all 0.25s ease; letter-spacing:0.5px;
    font-family:'Inter', sans-serif;
}}
.tab-btn-long {{
    background: linear-gradient(135deg, rgba(0,212,170,0.12), rgba(74,158,255,0.08));
    color: var(--accent-green);
    border-color: rgba(0,212,170,0.3);
}}
.tab-btn-long:hover, .tab-btn-long.active {{
    background: linear-gradient(135deg, rgba(0,212,170,0.25), rgba(74,158,255,0.15));
    border-color: var(--accent-green);
    box-shadow: 0 0 24px rgba(0,212,170,0.15);
}}
.tab-btn-short {{
    background: linear-gradient(135deg, rgba(255,74,106,0.12), rgba(255,140,66,0.08));
    color: var(--accent-red);
    border-color: rgba(255,74,106,0.3);
}}
.tab-btn-short:hover, .tab-btn-short.active {{
    background: linear-gradient(135deg, rgba(255,74,106,0.25), rgba(255,140,66,0.15));
    border-color: var(--accent-red);
    box-shadow: 0 0 24px rgba(255,74,106,0.15);
}}
.tab-content {{ display:none; }}
.tab-content.active {{ display:block; }}

/* ── Topbar ── */
.topbar {{
    background: linear-gradient(135deg, #0e0e18 0%, #161628 100%);
    border-bottom: 1px solid var(--border-subtle);
    padding: 18px 40px;
    display: flex; align-items: center; justify-content: space-between;
    position: sticky; top: 0; z-index: 100;
    backdrop-filter: blur(24px);
}}
.topbar-left {{ display:flex; align-items:center; gap:16px; }}
.home-btn {{
    display:flex; align-items:center; justify-content:center;
    width:34px; height:34px; border-radius:50%;
    background:var(--bg-input); border:1px solid var(--border-accent);
    color:var(--text-secondary); font-size:1rem; text-decoration:none;
    transition:all 0.2s; flex-shrink:0;
}}
.home-btn:hover {{ color:var(--accent-green); border-color:var(--accent-green); background:var(--accent-green-dim); }}
.topbar-logo {{
    font-size: 1.5rem; font-weight: 700;
    background: linear-gradient(135deg, var(--accent-green), var(--accent-blue));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}}
.topbar-divider {{ width:1px; height:28px; background:var(--border-accent); }}
.topbar-date {{ color:var(--text-secondary); font-size:0.85rem; }}
.topbar-right {{ display:flex; gap:10px; flex-wrap:wrap; }}
.nav-pill {{
    padding:6px 16px; border-radius:20px; font-size:0.78rem; font-weight:500;
    color:var(--text-secondary); background:var(--bg-input); border:1px solid var(--border-subtle);
    cursor:pointer; text-decoration:none; transition:all 0.2s;
}}
.nav-pill:hover, .nav-pill.active {{
    color:var(--accent-green); border-color:var(--accent-green); background:var(--accent-green-dim);
}}
/* Short theme nav pills */
.nav-pill-short {{
    padding:6px 16px; border-radius:20px; font-size:0.78rem; font-weight:500;
    color:var(--text-secondary); background:var(--bg-input); border:1px solid var(--border-subtle);
    cursor:pointer; text-decoration:none; transition:all 0.2s;
}}
.nav-pill-short:hover, .nav-pill-short.active {{
    color:var(--accent-red); border-color:var(--accent-red); background:var(--accent-red-dim);
}}

/* ── Layout ── */
.dashboard {{ max-width:1600px; margin:0 auto; padding:28px 40px 60px; display:flex; flex-direction:column; gap:24px; }}

/* ── Stats Bar ── */
.stats-bar {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(160px,1fr)); gap:14px; }}
.stat-card {{
    background:var(--bg-card); border:1px solid var(--border-subtle); border-radius:var(--radius-sm);
    padding:16px 18px; transition:all 0.2s;
}}
.stat-card:hover {{ border-color:var(--border-accent); transform:translateY(-1px); }}
.stat-label {{
    font-size:0.68rem; font-weight:600; text-transform:uppercase; letter-spacing:0.8px;
    color:var(--text-muted); margin-bottom:4px;
}}
.stat-value {{ font-size:1.5rem; font-weight:700; font-family:'JetBrains Mono', monospace; }}
.stat-green {{ color:var(--accent-green); }}
.stat-gold {{ color:var(--accent-gold); }}
.stat-blue {{ color:var(--accent-blue); }}
.stat-purple {{ color:var(--accent-purple); }}
.stat-red {{ color:var(--accent-red); }}
.stat-orange {{ color:var(--accent-orange); }}

/* ══════════════════════════════════════════
   CARD — Master template
   ══════════════════════════════════════════ */
.card {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius); overflow:hidden; box-shadow:var(--shadow);
}}
.card-highlight {{
    border-color:rgba(245,166,35,0.3);
    box-shadow:var(--shadow-lg), 0 0 80px rgba(245,166,35,0.04);
}}
.card-highlight-long {{
    border-color:rgba(0,212,170,0.35);
    box-shadow:var(--shadow-lg), 0 0 80px rgba(0,212,170,0.06);
}}
.card-highlight-short {{
    border-color:rgba(255,74,106,0.3);
    box-shadow:var(--shadow-lg), 0 0 80px rgba(255,74,106,0.04);
}}
.card-header {{ display:flex; align-items:center; justify-content:space-between; padding:20px 24px 0; }}
.card-title {{ display:flex; align-items:center; gap:10px; }}
.card-icon {{ font-size:1.2rem; }}
.card-title h2 {{ font-size:1.1rem; font-weight:600; letter-spacing:-0.3px; }}
.card-badge {{
    font-size:0.7rem; font-weight:600; padding:4px 12px; border-radius:20px; letter-spacing:0.4px;
}}
.badge-gold {{ color:var(--accent-gold); background:var(--accent-gold-dim); border:1px solid rgba(245,166,35,0.2); }}
.badge-basket {{ color:var(--accent-blue); background:rgba(74,158,255,0.08); border:1px solid rgba(74,158,255,0.2); }}
.badge-main {{ color:var(--accent-green); background:var(--accent-green-dim); border:1px solid rgba(0,212,170,0.2); }}
.badge-short {{ color:var(--accent-red); background:var(--accent-red-dim); border:1px solid rgba(255,74,106,0.2); }}
.badge-short-orange {{ color:var(--accent-orange); background:var(--accent-orange-dim); border:1px solid rgba(255,140,66,0.2); }}
.card-subtitle {{ padding:6px 24px 0; font-size:0.78rem; color:var(--text-muted); }}
.card-body {{ padding:18px 24px 24px; overflow-x:auto; }}

/* ── Score Badge ── */
.score-badge {{
    display:inline-block; font-family:'JetBrains Mono', monospace; font-weight:700;
    font-size:0.82rem; color:var(--accent-green);
}}
.score-badge-short {{
    display:inline-block; font-family:'JetBrains Mono', monospace; font-weight:700;
    font-size:0.82rem; color:var(--accent-red);
}}

/* ── Tags ── */
.tag {{
    display:inline-block; font-size:0.62rem; font-weight:500; padding:2px 7px;
    border-radius:4px; letter-spacing:0.3px;
}}
.tag-green {{ color:#00d4aa; background:rgba(0,212,170,0.12); }}
.tag-red {{ color:#ff4a6a; background:rgba(255,74,106,0.12); }}
.tag-neutral {{ color:var(--accent-gold); background:var(--accent-gold-dim); }}
.tag-dim {{ color:var(--text-muted); background:rgba(255,255,255,0.04); }}

/* ══════════════════════════════════════════
   DATATABLES — Unified styling
   ══════════════════════════════════════════ */
table.dataTable {{ border-collapse:collapse !important; width:100% !important; font-size:0.8rem; }}
table.dataTable thead th {{
    background:var(--bg-input) !important; color:var(--text-secondary) !important;
    font-weight:600 !important; font-size:0.7rem !important; text-transform:uppercase !important;
    letter-spacing:0.5px !important; padding:11px 12px !important;
    border-bottom:1px solid var(--border-accent) !important; white-space:nowrap;
    cursor:pointer;
}}
table.dataTable thead th:first-child {{ border-radius:var(--radius-sm) 0 0 0; }}
table.dataTable thead th:last-child {{ border-radius:0 var(--radius-sm) 0 0; }}
table.dataTable tbody td {{
    padding:9px 12px !important; border-bottom:1px solid var(--border-subtle) !important;
    font-family:'JetBrains Mono', monospace; font-size:0.76rem; color:var(--text-primary);
    vertical-align:middle;
}}
table.dataTable tbody tr {{ background:transparent !important; transition:background 0.15s; }}
table.dataTable tbody tr:hover {{ background:var(--bg-card-hover) !important; }}
table.dataTable tbody tr:nth-child(even) {{ background:rgba(255,255,255,0.012) !important; }}
table.dataTable tbody tr:nth-child(even):hover {{ background:var(--bg-card-hover) !important; }}

/* Sort arrows */
table.dataTable thead .sorting::after,
table.dataTable thead .sorting_asc::after,
table.dataTable thead .sorting_desc::after {{ opacity:0.3 !important; }}
table.dataTable thead .sorting_asc::after,
table.dataTable thead .sorting_desc::after {{ opacity:0.9 !important; color:var(--accent-green) !important; }}

/* Controls */
.dataTables_wrapper {{ color:var(--text-secondary) !important; }}
.dataTables_wrapper .dataTables_filter input {{
    background:var(--bg-input) !important; color:var(--text-primary) !important;
    border:1px solid var(--border-accent) !important; border-radius:var(--radius-sm) !important;
    padding:7px 14px !important; font-family:'Inter',sans-serif !important;
    font-size:0.8rem !important; outline:none; transition:border-color 0.2s;
}}
.dataTables_wrapper .dataTables_filter input:focus {{ border-color:var(--accent-green) !important; }}
.dataTables_wrapper .dataTables_filter label,
.dataTables_wrapper .dataTables_length label {{ color:var(--text-muted) !important; font-size:0.78rem !important; }}
.dataTables_wrapper .dataTables_length select {{
    background:var(--bg-input) !important; color:var(--text-primary) !important;
    border:1px solid var(--border-accent) !important; border-radius:6px !important; padding:3px 8px !important;
}}
.dataTables_wrapper .dataTables_info {{ color:var(--text-muted) !important; font-size:0.76rem !important; padding-top:14px !important; }}
.dataTables_wrapper .dataTables_paginate {{ padding-top:14px !important; }}
.dataTables_wrapper .dataTables_paginate .paginate_button {{
    color:var(--text-secondary) !important; background:transparent !important;
    border:1px solid var(--border-subtle) !important; border-radius:6px !important;
    margin:0 2px !important; padding:3px 10px !important; font-size:0.76rem !important; transition:all 0.15s;
}}
.dataTables_wrapper .dataTables_paginate .paginate_button:hover {{
    color:var(--accent-green) !important; border-color:var(--accent-green) !important; background:var(--accent-green-dim) !important;
}}
.dataTables_wrapper .dataTables_paginate .paginate_button.current {{
    color:var(--bg-primary) !important; background:var(--accent-green) !important;
    border-color:var(--accent-green) !important; font-weight:600 !important;
}}
.dataTables_wrapper .dataTables_paginate .paginate_button.disabled {{ color:var(--text-muted) !important; opacity:0.4; }}

/* Buttons */
.dt-buttons {{ margin-bottom:14px !important; }}
.dt-button {{
    background:var(--bg-input) !important; color:var(--text-secondary) !important;
    border:1px solid var(--border-accent) !important; border-radius:6px !important;
    padding:5px 14px !important; font-size:0.72rem !important; font-family:'Inter',sans-serif !important;
    font-weight:500 !important; cursor:pointer !important; transition:all 0.15s !important;
}}
.dt-button:hover {{
    color:var(--accent-green) !important; border-color:var(--accent-green) !important;
    background:var(--accent-green-dim) !important;
}}

/* ── Basket Detail Cards ── */
.basket-detail-grid {{
    display:grid; grid-template-columns:repeat(auto-fill, minmax(420px, 1fr)); gap:16px;
}}
.basket-detail-card {{
    background:var(--bg-primary); border:1px solid var(--border-subtle);
    border-radius:var(--radius-sm); overflow:hidden; transition:border-color 0.2s;
}}
.basket-detail-card:hover {{ border-color:var(--border-accent); }}
.basket-detail-header {{
    padding:14px 16px; display:flex; align-items:center; justify-content:space-between;
    border-bottom:1px solid var(--border-subtle); cursor:pointer; user-select:none;
}}
.basket-detail-header:hover {{ background:rgba(255,255,255,0.02); }}
.basket-detail-name {{ font-size:0.85rem; font-weight:600; color:var(--text-primary); }}
.basket-detail-meta {{ display:flex; gap:8px; align-items:center; }}
.basket-toggle {{
    font-size:0.7rem; color:var(--text-muted); transition:transform 0.2s;
}}
.basket-detail-body {{
    padding:8px 12px 14px; overflow-x:auto;
}}
.basket-detail-body.collapsed {{
    display:none;
}}

/* ── Footer ── */
.footer {{
    text-align:center; padding:20px; color:var(--text-muted); font-size:0.72rem;
    border-top:1px solid var(--border-subtle); margin-top:8px;
}}

/* ── Responsive ── */
@media (max-width:768px) {{
    .topbar {{ padding:14px 16px; flex-direction:column; gap:10px; }}
    .dashboard {{ padding:16px 12px 40px; }}
    .card-body {{ padding:12px 14px 18px; }}
    .card-header {{ padding:14px 14px 0; }}
    .stats-bar {{ grid-template-columns:repeat(2,1fr); }}
    .basket-detail-grid {{ grid-template-columns:1fr; }}
    .tab-bar {{ flex-direction:column; padding:12px 16px 0; }}
    .tab-btn {{ padding:12px 24px; font-size:0.9rem; }}
}}
</style>
</head>
<body>

<div class="topbar">
    <div class="topbar-left">
        <a href="../index.html" class="home-btn" title="Back to Homepage">&#8592;</a>
        <span class="topbar-logo">Ratatouille</span>
        <div class="topbar-divider"></div>
        <span class="topbar-date">{gen_date} &middot; {gen_time}</span>
    </div>
    <div class="topbar-right" id="navPills">
        <!-- Populated dynamically by JS based on active tab -->
    </div>
</div>

<!-- ══ TAB BUTTONS ══ -->
<div class="tab-bar">
    <button class="tab-btn tab-btn-long active" onclick="switchTab('long')">Long Analysis</button>
    <button class="tab-btn tab-btn-short" onclick="switchTab('short')">Short Analysis</button>
</div>

<!-- ══════════════════════════════════════════════════════
     LONG TAB CONTENT
     ══════════════════════════════════════════════════════ -->
<div id="tab-long" class="tab-content active">
<div class="dashboard">

    <div class="stats-bar">
        <div class="stat-card">
            <div class="stat-label">Universe</div>
            <div class="stat-value stat-blue">{n_stocks}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Candidates</div>
            <div class="stat-value stat-gold">{n_cands}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Strong Baskets</div>
            <div class="stat-value stat-green">{n_strong_baskets}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Top Score</div>
            <div class="stat-value stat-green">{display_df['Final_Score'].max():.1f}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Median</div>
            <div class="stat-value" style="color:var(--text-secondary)">{display_df['Final_Score'].median():.1f}</div>
        </div>
    </div>

    {long_candidate_section}

    {long_recommended_section}

    <section class="card" id="screener-section">
        <div class="card-header">
            <div class="card-title">
                <h2>Full Screener</h2>
            </div>
            <span class="card-badge badge-main">{n_stocks} Stocks</span>
        </div>
        <div class="card-body">{long_screener_table}</div>
    </section>

</div>
</div>

<!-- ══════════════════════════════════════════════════════
     SHORT TAB CONTENT
     ══════════════════════════════════════════════════════ -->
<div id="tab-short" class="tab-content">
<div class="dashboard">

    <div class="stats-bar">
        <div class="stat-card">
            <div class="stat-label">Universe</div>
            <div class="stat-value stat-blue">{n_stocks}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Short Candidates</div>
            <div class="stat-value stat-gold">{n_short_cands}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Weak Baskets</div>
            <div class="stat-value stat-red">{n_strong_short_baskets}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Top Short Score</div>
            <div class="stat-value stat-red">{short_sorted.iloc[0].get('Short_Score', short_sorted.iloc[0].get('Final_Score', 0)) if len(short_sorted) > 0 else 0:.1f}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Median</div>
            <div class="stat-value" style="color:var(--text-secondary)">{display_df['Short_Score'].median() if has_short_scores else (100 - display_df['Final_Score'].median()):.1f}</div>
        </div>
    </div>

    {short_candidate_section}

    {short_recommended_section}

    <section class="card" id="short_screener-section">
        <div class="card-header">
            <div class="card-title">
                <h2>Full Short Screener</h2>
            </div>
            <span class="card-badge badge-short">{n_stocks} Stocks</span>
        </div>
        <div class="card-body">{short_screener_table}</div>
    </section>

</div>
</div>

<div class="footer">
    Ratatouille Screener &middot; {gen_time} &middot; Long/Short Dual Analysis &middot; 14-Point Framework &middot; Zero Paid APIs
</div>

<script src="https://code.jquery.com/jquery-3.5.1.js"></script>
<script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.3.6/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.3.6/js/buttons.html5.min.js"></script>
<script>
// ══════════════════════════════════════════════════════
// TAB SWITCHING — Vanilla JS
// ══════════════════════════════════════════════════════
var currentTab = 'long';
var tablesInitialized = {{ long: false, short: false }};

var longNavItems = [
    {{ href: '#candidates-section', label: 'Candidates' }},
    {{ href: '#recommended-section', label: 'Recommended' }},
    {{ href: '#screener-section', label: 'Full Screen' }}
];

var shortNavItems = [
    {{ href: '#short_candidates-section', label: 'Short Candidates' }},
    {{ href: '#short_recommended-section', label: 'Short Recommended' }},
    {{ href: '#short_screener-section', label: 'Full Short Screen' }}
];

function buildNavPills(items, pillClass) {{
    var navEl = document.getElementById('navPills');
    navEl.innerHTML = '';
    items.forEach(function(item, i) {{
        var a = document.createElement('a');
        a.href = item.href;
        a.className = pillClass + (i === 0 ? ' active' : '');
        a.textContent = item.label;
        a.addEventListener('click', function(e) {{
            e.preventDefault();
            var target = document.querySelector(item.href);
            if (target) {{
                window.scrollTo({{ top: target.offsetTop - 70, behavior: 'smooth' }});
            }}
            navEl.querySelectorAll('.' + pillClass).forEach(function(p) {{ p.classList.remove('active'); }});
            a.classList.add('active');
        }});
        navEl.appendChild(a);
    }});
}}

function switchTab(tab) {{
    currentTab = tab;

    // Toggle tab buttons
    document.querySelectorAll('.tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
    if (tab === 'long') {{
        document.querySelector('.tab-btn-long').classList.add('active');
    }} else {{
        document.querySelector('.tab-btn-short').classList.add('active');
    }}

    // Toggle content
    document.querySelectorAll('.tab-content').forEach(function(c) {{ c.classList.remove('active'); }});
    document.getElementById('tab-' + tab).classList.add('active');

    // Update nav pills
    if (tab === 'long') {{
        buildNavPills(longNavItems, 'nav-pill');
    }} else {{
        buildNavPills(shortNavItems, 'nav-pill-short');
    }}

    // Initialize DataTables for this tab if not done yet
    if (!tablesInitialized[tab]) {{
        initTabTables(tab);
        tablesInitialized[tab] = true;
    }}
}}

function initTabTables(tab) {{
    if (tab === 'long') {{
        // Long Screener
        if ($('#screenerTable').length && !$.fn.DataTable.isDataTable('#screenerTable')) {{
            $('#screenerTable').DataTable({{
                order: [[3, 'desc']],  // col 3 = Score (Rank=0, Ticker=1, Price=2, Score=3)
                pageLength: 50,
                dom: '<"dt-top"Bf>rt<"dt-bottom"lip>',
                buttons: ['copy','csv','excel'],
                language: {{
                    search: 'Filter:', lengthMenu: 'Show _MENU_',
                    info: '_START_&ndash;_END_ of _TOTAL_',
                    paginate: {{ previous: '&larr;', next: '&rarr;' }}
                }}
            }});
        }}
        // Long Candidates
        if ($('#candidateTable').length && !$.fn.DataTable.isDataTable('#candidateTable')) {{
            $('#candidateTable').DataTable({{
                order: [[1, 'desc']], pageLength: 25, searching: true,
                dom: '<"dt-top"f>rt<"dt-bottom"lip>',
                language: {{ search: 'Filter:', info: '_START_&ndash;_END_ of _TOTAL_', paginate: {{ previous: '&larr;', next: '&rarr;' }} }}
            }});
        }}
        // Long Recommended
        if ($('#recommendedTable').length && !$.fn.DataTable.isDataTable('#recommendedTable')) {{
            $('#recommendedTable').DataTable({{
                order: [[3, 'desc']], pageLength: 25, searching: true,  // col 3 = Score
                dom: '<"dt-top"f>rt<"dt-bottom"lip>',
                language: {{ search: 'Filter:', info: '_START_&ndash;_END_ of _TOTAL_', paginate: {{ previous: '&larr;', next: '&rarr;' }} }}
            }});
        }}
    }} else {{
        // Short Screener
        if ($('#short_screenerTable').length && !$.fn.DataTable.isDataTable('#short_screenerTable')) {{
            $('#short_screenerTable').DataTable({{
                order: [[3, 'desc']],  // col 3 = Score (Rank=0, Ticker=1, Price=2, Score=3)
                pageLength: 50,
                dom: '<"dt-top"Bf>rt<"dt-bottom"lip>',
                buttons: ['copy','csv','excel'],
                language: {{
                    search: 'Filter:', lengthMenu: 'Show _MENU_',
                    info: '_START_&ndash;_END_ of _TOTAL_',
                    paginate: {{ previous: '&larr;', next: '&rarr;' }}
                }}
            }});
        }}
        // Short Candidates
        if ($('#short_candidateTable').length && !$.fn.DataTable.isDataTable('#short_candidateTable')) {{
            $('#short_candidateTable').DataTable({{
                order: [[1, 'desc']], pageLength: 25, searching: true,
                dom: '<"dt-top"f>rt<"dt-bottom"lip>',
                language: {{ search: 'Filter:', info: '_START_&ndash;_END_ of _TOTAL_', paginate: {{ previous: '&larr;', next: '&rarr;' }} }}
            }});
        }}
        // Short Recommended
        if ($('#short_recommendedTable').length && !$.fn.DataTable.isDataTable('#short_recommendedTable')) {{
            $('#short_recommendedTable').DataTable({{
                order: [[3, 'desc']], pageLength: 25, searching: true,  // col 3 = Score
                dom: '<"dt-top"f>rt<"dt-bottom"lip>',
                language: {{ search: 'Filter:', info: '_START_&ndash;_END_ of _TOTAL_', paginate: {{ previous: '&larr;', next: '&rarr;' }} }}
            }});
        }}
    }}
}}

// ══ BASKET TOGGLE (Long) ══
function toggleBasket(header) {{
    var body = header.nextElementSibling;
    var toggle = header.querySelector('.basket-toggle');
    if (body.classList.contains('collapsed')) {{
        body.classList.remove('collapsed');
        toggle.innerHTML = '&#x25BC;';
    }} else {{
        body.classList.add('collapsed');
        toggle.innerHTML = '&#x25B6;';
    }}
}}

// ══ BASKET TOGGLE (Short) ══
function toggleBasketShort(header) {{
    var body = header.nextElementSibling;
    var toggle = header.querySelector('.basket-toggle');
    if (body.classList.contains('collapsed')) {{
        body.classList.remove('collapsed');
        toggle.innerHTML = '&#x25BC;';
    }} else {{
        body.classList.add('collapsed');
        toggle.innerHTML = '&#x25B6;';
    }}
}}

// ══ INIT ON LOAD ══
$(document).ready(function() {{
    // Default: Long tab active
    switchTab('long');
}});
</script>
</body>
</html>"""

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
