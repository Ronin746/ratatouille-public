"""
update_archive.py — Ratatouille Archive Builder
Scans Ratatouille_YYYY-MM-DD.html reports from Reports/, copies them to Archive/reports/,
reads the corresponding CSV files for stats, and regenerates Archive/index.html.
Only reports on or after CUTOFF_DATE are included.

Layout: Hero → Latest Report Card → Market Breadth → Sector Performance → Footer
"""

import json
import os
import re
import shutil
from datetime import datetime


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
    """Find all Ratatouille_YYYY-MM-DD.html files in Ratatouille/Reports/ from CUTOFF_DATE onward."""
    parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    reports_dir = os.path.join(parent, 'Reports')
    pattern = re.compile(r'^Ratatouille_(\d{4}-\d{2}-\d{2})\.html$')
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


def get_latest_basket_top10():
    """
    Read the latest screen_results CSV and return per-basket top-10 stocks.
    Returns dict: {basket_name: [{'t':ticker,'s':score10,'r2':r2,
                                   'd1':chg1d,'d7':chg1w,'d30':chg1m}, ...]}
    """
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    data_dir     = os.path.join(parent_dir, 'Data')

    # Find latest CSV
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

        df = pd.read_csv(csv_path, index_col=0)
        required = {'Final_Score', 'r_squared', '1d_return', '1w_return', '1m_return'}
        if not required.issubset(df.columns):
            return {}

        result = {}
        for basket_name, tickers in SECTOR_BASKETS.items():
            sub = df[df.index.isin(tickers)].copy()
            if sub.empty:
                continue
            sub = sub.sort_values('Final_Score', ascending=False).head(10)
            stocks = []
            for ticker, row in sub.iterrows():
                stocks.append({
                    't':   str(ticker),
                    's':   round(float(row['Final_Score']), 1),
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
    Compute market-wide breadth and per-sector average scores for a given date.
    Returns a dict or None if CSV not found / missing required columns.
    """
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    date_compact = date_str.replace('-', '')
    csv_path = os.path.join(parent_dir, 'Data', f'screen_results_{date_compact}.csv')
    if not os.path.exists(csv_path):
        csv_path = os.path.join(screener_dir, f'screen_results_{date_compact}.csv')
    if not os.path.exists(csv_path):
        return None

    try:
        import pandas as pd
        df = pd.read_csv(csv_path, index_col=0)
        if 'Final_Score' not in df.columns:
            return None

        n                 = len(df)
        long_breadth_pct  = round(float((df['Final_Score'] >= 70).sum()) / n * 100, 1)
        long_count        = int((df['Final_Score'] >= 70).sum())
        short_breadth_pct = 0.0
        short_count       = 0
        if 'Short_Score' in df.columns:
            short_breadth_pct = round(float((df['Short_Score'] >= 70).sum()) / n * 100, 1)
            short_count       = int((df['Short_Score'] >= 70).sum())

        from sector_baskets import SECTOR_BASKETS
        sectors    = {}
        sector_avg = 0.0
        for basket_name, basket_tickers in SECTOR_BASKETS.items():
            present = df[df.index.isin(basket_tickers)]
            if len(present) >= 2:
                avg_score = present['Final_Score'].mean()
                sectors[basket_name] = round(float(avg_score), 1)
        if sectors:
            sector_avg = round(sum(sectors.values()) / len(sectors), 1)

        return {
            'date':              date_str,
            'long_breadth_pct':  long_breadth_pct,
            'short_breadth_pct': short_breadth_pct,
            'sector_avg':        sector_avg,
            'long_count':        long_count,
            'short_count':       short_count,
            'sectors':           sectors,
        }

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


def save_market_history(archive_dir, history):
    """Persist market score history to Archive/market_score_history.json."""
    history_path = os.path.join(archive_dir, 'market_score_history.json')
    data = {
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'history':   sorted(history, key=lambda x: x['date']),
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
    <div class="lr-stats">
      <div class="lr-stat">
        <div class="lr-stat-label">Scanned</div>
        <div class="lr-stat-value stat-blue">{total_str}</div>
      </div>
      <div class="lr-stat">
        <div class="lr-stat-label">Top Pick</div>
        <div class="lr-stat-value stat-gold">{stats['top_ticker']}</div>
      </div>
      <div class="lr-stat">
        <div class="lr-stat-label">Best Score</div>
        <div class="lr-stat-value stat-green">{score_str}</div>
      </div>
    </div>
  </a>
</div>
<!-- ── /Latest Report ────────────────────────────────────────────────── -->
"""


def build_breadth_html(market_history):
    """
    Market Breadth section: 4 stats (Bias, Long%, Short%, Sector Avg)
    plus a full-width breadth trend chart.
    """
    if not market_history:
        return ''

    history = sorted(market_history, key=lambda x: x['date'])

    display_dates = []
    for d in [h['date'] for h in history]:
        try:
            display_dates.append(datetime.strptime(d, '%Y-%m-%d').strftime('%b %d'))
        except Exception:
            display_dates.append(d)

    long_breadth  = [h['long_breadth_pct']  for h in history]
    short_breadth = [h['short_breadth_pct'] for h in history]

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

    jdates   = json.dumps(display_dates)
    jlong    = json.dumps(long_breadth)
    jshort   = json.dumps(short_breadth)
    jspreads = json.dumps(spreads)

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
        Breadth Trend &mdash; % stocks scoring 70+
        <span class="br-pill br-pill-green">LONG</span>
        <span class="br-pill br-pill-red">SHORT</span>
      </div>
    </div>
    <div class="br-chart-wrap"><canvas id="breadthChart"></canvas></div>
    <hr class="br-divider">
    <div class="br-chart-row" style="margin-top:14px">
      <div class="br-chart-label">
        L-S Spread
        <span class="br-guide">(Long% &minus; Short%)</span>
      </div>
    </div>
    <div class="br-spread-wrap"><canvas id="spreadChart"></canvas></div>
  </div>
</div>
<!-- ── /Market Breadth ───────────────────────────────────────────────── -->

<script>
document.addEventListener('DOMContentLoaded', function() {{
  var ctx = document.getElementById('breadthChart').getContext('2d');
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: {jdates},
      datasets: [
        {{
          label: 'Long 70+%',
          data: {jlong},
          borderColor: '#00d4aa',
          backgroundColor: 'rgba(0,212,170,0.12)',
          fill: true, tension: 0.35,
          pointRadius: 5, pointBackgroundColor: '#00d4aa', borderWidth: 2.5
        }},
        {{
          label: 'Short 70+%',
          data: {jshort},
          borderColor: '#ff4a6a',
          backgroundColor: 'rgba(255,74,106,0.08)',
          fill: true, tension: 0.35,
          pointRadius: 5, pointBackgroundColor: '#ff4a6a', borderWidth: 2.5
        }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      layout: {{ padding: {{ right: 32 }} }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#10101a', borderColor: '#2a2a44', borderWidth: 1,
          titleColor: '#e8e8f0', bodyColor: '#8888a0',
          callbacks: {{
            label: function(c) {{
              if (c.dataset.label === 'MA10')
                return ' MA10: ' + c.parsed.y.toFixed(1) + '%';
              return ' ' + c.dataset.label + ': ' + c.parsed.y + '%';
            }},
            afterBody: function(items) {{
              var lo = items.find(function(i) {{ return i.dataset.label === 'Long 70+%'; }});
              var sh = items.find(function(i) {{ return i.dataset.label === 'Short 70+%'; }});
              if (lo && sh) {{
                var sp  = (lo.parsed.y - sh.parsed.y).toFixed(1);
                var rat = sh.parsed.y > 0
                          ? (lo.parsed.y / sh.parsed.y).toFixed(2) + 'x'
                          : '∞';
                var reg = sp > 8 ? 'RISK-ON'
                        : sp > 5 ? 'BULLISH'
                        : sp > 2 ? 'NEUTRAL'
                        : sp > 0 ? 'CAUTION'
                        :          'RISK-OFF';
                return ['', ' Spread : ' + sp, ' Ratio  : ' + rat, ' Regime : ' + reg];
              }}
              return [];
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
            callback: function(v) {{ return v + '%'; }}
          }},
          suggestedMin: 4, suggestedMax: 23
        }}
      }}
    }}
  }});

  // ── Spread bar chart ─────────────────────────────────────────────────────
  var spreads      = {jspreads};
  var spreadColors = spreads.map(function(s) {{
    if (s > 8) return 'rgba(0,212,170,0.80)';
    if (s > 5) return 'rgba(74,158,255,0.70)';
    if (s > 2) return 'rgba(245,166,35,0.65)';
    if (s > 0) return 'rgba(245,166,35,0.35)';
    return 'rgba(255,74,106,0.80)';
  }});

  var ctx2 = document.getElementById('spreadChart').getContext('2d');
  new Chart(ctx2, {{
    type: 'bar',
    data: {{
      labels: {jdates},
      datasets: [{{
        label: 'Spread',
        data: spreads,
        backgroundColor: spreadColors,
        borderRadius: 2,
        borderSkipped: false,
        borderWidth: 0,
        barPercentage: 0.9,
        categoryPercentage: 1.0
      }}]
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
              var s   = c.parsed.y;
              var reg = s > 8 ? 'RISK-ON'
                      : s > 5 ? 'BULLISH'
                      : s > 2 ? 'NEUTRAL'
                      : s > 0 ? 'CAUTION'
                      :         'RISK-OFF';
              return [' Spread: ' + s.toFixed(1), ' Regime: ' + reg];
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: 'rgba(255,255,255,0.02)' }},
          ticks: {{ color: '#505068', font: {{ size: 10 }}, maxTicksLimit: 12 }}
        }},
        y: {{
          grid: {{ color: 'rgba(255,255,255,0.04)' }},
          ticks: {{ color: '#505068', font: {{ size: 10 }} }}
        }}
      }}
    }}
  }});

}});
</script>
"""


def build_sector_charts_html(market_history, top10_data=None):
    """
    Sector Performance section — enhanced sparkline grid.
    Each card shows: signal badge, score, Δ5d, Δ20d, percentile, MA10 tag.
    Sparkline has MA10 overlay and reference lines at 50/70.
    JS client-side sort by Score / Δ5d / Δ20d / Percentile / Stage.
    Click card to expand Top-10 stocks panel.
    """
    if not market_history:
        return ''

    history = sorted(market_history, key=lambda x: x['date'])

    latest         = history[-1]
    latest_sectors = latest.get('sectors', {})
    if not latest_sectors:
        return ''

    # ── Per-sector stats ───────────────────────────────────────────────────────
    sector_stats = []
    for sec_name, cur_score in latest_sectors.items():
        values = [h.get('sectors', {}).get(sec_name) for h in history]
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

        # Signal classification
        sig_label, sig_color = _sector_signal(cur_score, d5, above_ma10)

        sector_stats.append({
            'name':       sec_name,
            'score':      cur_score,
            'd5':         d5,
            'd20':        d20,
            'ma10':       ma10,
            'above_ma10': above_ma10,
            'ma_gap':     ma_gap,
            'pct':        pct,
            'sig_label':  sig_label,
            'sig_color':  sig_color,
            'values':     values,
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
        score      = s['score']
        d5         = s['d5']
        d20        = s['d20']
        pct        = s['pct']
        ma10       = s['ma10']
        ma_gap     = s['ma_gap']
        sig_label  = s['sig_label']
        sig_color  = s['sig_color']
        above_ma10 = s['above_ma10']

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

        # Top-10 data for click panel
        top10_list = (top10_data or {}).get(s['name'], [])
        top10_attr = json.dumps(top10_list).replace('"', '&quot;')

        grid_html += f"""
    <div class="sec-card" style="border-left-color:{sig_color}"
         data-score="{score}" data-d5="{d5}" data-d20="{d20}" data-pct="{pct}"
         data-stage="{sig_label}" data-top10="{top10_attr}"
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

    return f"""
<!-- ── Sector Performance ───────────────────────────────────────────── -->
<div class="sc-wrap">
  <div class="sc-header-row">
    <div class="sc-section-title">Sector Performance</div>
    <div class="sc-header-right">
      <div class="sc-sig-summary">{sig_summary}</div>
      <div class="sc-sort-bar">
        <span class="sc-sort-label">Sort:</span>
        <button class="sc-sort-btn" onclick="sortSectors('score',this)">Score</button>
        <button class="sc-sort-btn" onclick="sortSectors('d5',this)">&#916; 5d</button>
        <button class="sc-sort-btn" onclick="sortSectors('d20',this)">&#916; 20d</button>
        <button class="sc-sort-btn" onclick="sortSectors('pct',this)">Pct</button>
        <button class="sc-sort-btn active" onclick="sortByStage(this)">Stage</button>
      </div>
    </div>
  </div>

  <div class="sec-grid" id="secGrid">
{grid_html}
  </div>

  <div class="sec-panel-area" id="secPanelArea" style="display:none">
    <div class="sec-panel-header">
      <span class="sec-panel-title" id="secPanelTitle"></span>
      <button class="sec-panel-close" onclick="closeSectorPanel()">&#x2715;</button>
    </div>
    <div id="secPanelTable"></div>
  </div>
</div>

<script>
// ── Sort helpers ──────────────────────────────────────────────────────────
function sortSectors(key, btn) {{
  var grid = document.getElementById('secGrid');
  grid.querySelectorAll('.sec-stage-hdr').forEach(function(h) {{ h.remove(); }});
  var cards = Array.from(grid.querySelectorAll('.sec-card'));
  cards.sort(function(a, b) {{
    return parseFloat(b.dataset[key]) - parseFloat(a.dataset[key]);
  }});
  cards.forEach(function(c) {{ grid.appendChild(c); }});
  document.querySelectorAll('.sc-sort-btn').forEach(function(b) {{ b.classList.remove('active'); }});
  btn.classList.add('active');
}}

function sortByStage(btn) {{
  var STAGE_ORDER = ['LEADING','FADING','BUILDING','SLIPPING','HOLDING','RECOVERY','WEAK','BEARISH'];
  var STAGE_COLORS = {{
    'LEADING':'#00d4aa','FADING':'#4a9eff','BUILDING':'#4a9eff',
    'SLIPPING':'#f5a623','HOLDING':'#f5a623','RECOVERY':'#f5a623',
    'WEAK':'#ff4a6a','BEARISH':'#ff4a6a'
  }};
  var grid = document.getElementById('secGrid');
  grid.querySelectorAll('.sec-stage-hdr').forEach(function(h) {{ h.remove(); }});
  var cards = Array.from(grid.querySelectorAll('.sec-card'));
  var groups = {{}};
  STAGE_ORDER.forEach(function(s) {{ groups[s] = []; }});
  cards.forEach(function(c) {{
    var stage = c.dataset.stage || 'WEAK';
    if (!groups[stage]) groups[stage] = [];
    groups[stage].push(c);
  }});
  STAGE_ORDER.forEach(function(s) {{
    groups[s].sort(function(a,b) {{ return parseFloat(b.dataset.score) - parseFloat(a.dataset.score); }});
  }});
  STAGE_ORDER.forEach(function(s) {{
    if (groups[s].length === 0) return;
    var hdr = document.createElement('div');
    hdr.className = 'sec-stage-hdr';
    hdr.style.color = STAGE_COLORS[s] || '#888';
    hdr.textContent = s + '  (' + groups[s].length + ')';
    grid.appendChild(hdr);
    groups[s].forEach(function(c) {{ grid.appendChild(c); }});
  }});
  document.querySelectorAll('.sc-sort-btn').forEach(function(b) {{ b.classList.remove('active'); }});
  btn.classList.add('active');
}}

// ── Top-10 panel ─────────────────────────────────────────────────────────
var _selCard = null;

function openSectorPanel(card) {{
  if (_selCard === card) {{ closeSectorPanel(); return; }}
  if (_selCard) _selCard.classList.remove('sec-selected');
  _selCard = card;
  card.classList.add('sec-selected');

  var stocks = [];
  try {{ stocks = JSON.parse(card.dataset.top10 || '[]'); }} catch(e) {{}}

  var name = (card.querySelector('.sec-name').title || card.querySelector('.sec-name').textContent).trim();
  document.getElementById('secPanelTitle').textContent = name + ' \u2014 Top 10';

  if (stocks.length === 0) {{
    document.getElementById('secPanelTable').innerHTML = '<p style="color:var(--text-muted);font-size:.65rem;padding:6px 0">No data available.</p>';
  }} else {{
    var rows = stocks.map(function(s) {{
      var d1s   = (s.d1  > 0 ? '+' : '') + s.d1  + '%';
      var d7s   = (s.d7  > 0 ? '+' : '') + s.d7  + '%';
      var d30s  = (s.d30 > 0 ? '+' : '') + s.d30 + '%';
      var c1    = s.d1  > 0 ? '#00d4aa' : (s.d1  < 0 ? '#ff4a6a' : '#808090');
      var c7    = s.d7  > 0 ? '#00d4aa' : (s.d7  < 0 ? '#ff4a6a' : '#808090');
      var c30   = s.d30 > 0 ? '#00d4aa' : (s.d30 < 0 ? '#ff4a6a' : '#808090');
      return '<tr>'
        + '<td>' + s.t + '</td>'
        + '<td>' + s.s + '</td>'
        + '<td>' + s.r2 + '</td>'
        + '<td style="color:' + c1  + '">' + d1s  + '</td>'
        + '<td style="color:' + c7  + '">' + d7s  + '</td>'
        + '<td style="color:' + c30 + '">' + d30s + '</td>'
        + '</tr>';
    }}).join('');
    document.getElementById('secPanelTable').innerHTML =
      '<table class="sec-panel-table"><thead><tr>'
      + '<th>Ticker</th><th>Score</th><th>R\u00B2</th>'
      + '<th>Chg 1D%</th><th>Chg 1W%</th><th>Chg 1M%</th>'
      + '</tr></thead><tbody>' + rows + '</tbody></table>';
  }}
  document.getElementById('secPanelArea').style.display = 'block';
  document.getElementById('secPanelArea').scrollIntoView({{behavior:'smooth', block:'nearest'}});
}}

function closeSectorPanel() {{
  if (_selCard) _selCard.classList.remove('sec-selected');
  _selCard = null;
  document.getElementById('secPanelArea').style.display = 'none';
}}

</script>
<!-- ── /Sector Performance ───────────────────────────────────────────── -->
"""


# ─────────────────────────────────────────────────────────────────────────────
# Index HTML builder
# ─────────────────────────────────────────────────────────────────────────────

def build_index_html(reports_with_stats, market_history=None):
    """Build the complete index.html for the archive site."""

    latest_report_html = build_latest_report_html(reports_with_stats)
    breadth_html       = ''
    sector_html        = ''
    if market_history:
        breadth_html  = build_breadth_html(market_history)
        top10_data    = get_latest_basket_top10()
        sector_html   = build_sector_charts_html(market_history, top10_data=top10_data)

    now_str = datetime.now().strftime('%b %d, %Y at %H:%M')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ratatouille &#8212; Market Intelligence Archive</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js" defer></script>
<style>
:root {{
    --bg-primary:    #080810;
    --bg-card:       #10101a;
    --bg-card-hover: #18182a;
    --border-subtle: #1c1c30;
    --border-accent: #2a2a44;
    --text-primary:  #e8e8f0;
    --text-secondary:#8888a0;
    --text-muted:    #505068;
    --accent-green:  #00d4aa;
    --accent-gold:   #f5a623;
    --accent-blue:   #4a9eff;
    --accent-red:    #ff4a6a;
    --radius:        16px;
    --radius-sm:     10px;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
    background:var(--bg-primary); color:var(--text-primary);
    min-height:100vh; line-height:1.6;
}}

/* ══ Hero ══════════════════════════════════════════════════════════════ */
.hero {{
    background:linear-gradient(135deg,#0a0a18 0%,#100e22 50%,#0a1018 100%);
    border-bottom:1px solid var(--border-subtle);
    padding:60px 40px 52px; text-align:center; position:relative; overflow:hidden;
}}
.hero::before {{
    content:''; position:absolute; inset:0;
    background:
        radial-gradient(ellipse 80% 60% at 50%  0%, rgba(74,158,255,.07),  transparent),
        radial-gradient(ellipse 60% 40% at 15% 100%,rgba(0,212,170,.05),   transparent),
        radial-gradient(ellipse 50% 40% at 85%  80%,rgba(139,92,246,.04),  transparent);
    pointer-events:none;
}}
.hero-eyebrow {{
    font-size:.7rem; font-weight:600; letter-spacing:3px;
    text-transform:uppercase; color:var(--accent-blue); margin-bottom:14px; opacity:.8;
}}
.hero-title {{
    font-size:4.5rem; font-weight:800; letter-spacing:-3px; line-height:1;
    background:linear-gradient(135deg,#e8e8f0 0%,#aaaacc 60%,#8888aa 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:12px;
}}
.hero-subtitle {{
    font-size:1rem; color:var(--text-secondary);
}}

/* ══ Latest Report ═════════════════════════════════════════════════════ */
.lr-wrap {{
    max-width:1000px; margin:0 auto; padding:48px 24px 0;
}}
.lr-header-row {{
    display:flex; align-items:center; justify-content:space-between; margin-bottom:14px;
}}
.lr-section-title {{
    font-size:.78rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:1.2px;
}}
.lr-card {{
    display:block; text-decoration:none; color:inherit;
    background:linear-gradient(135deg,#0d1a18,#0e1520);
    border:1px solid rgba(0,212,170,.3);
    border-radius:var(--radius); padding:28px 32px;
    box-shadow:0 0 60px rgba(0,212,170,.06);
    transition:all .2s ease;
}}
.lr-card:hover {{
    border-color:rgba(0,212,170,.55);
    box-shadow:0 14px 60px rgba(0,212,170,.13);
    transform:translateY(-2px);
}}
.lr-card-top {{
    display:flex; align-items:center; gap:18px; margin-bottom:24px;
}}
.lr-live-badge {{
    font-size:.63rem; font-weight:700; letter-spacing:1px;
    color:var(--accent-green); background:rgba(0,212,170,.1);
    border:1px solid rgba(0,212,170,.2); border-radius:20px;
    padding:4px 12px; text-transform:uppercase; flex-shrink:0;
    animation:pulse-badge 2.2s ease-in-out infinite;
}}
@keyframes pulse-badge {{
    0%,100% {{ opacity:1; }} 50% {{ opacity:.55; }}
}}
.lr-date {{
    font-size:1.4rem; font-weight:700; color:var(--text-primary); flex:1;
}}
.lr-arrow {{
    color:var(--accent-green); font-size:1.4rem;
    font-family:'JetBrains Mono',monospace; transition:transform .2s;
}}
.lr-card:hover .lr-arrow {{ transform:translateX(7px); }}
.lr-stats {{
    display:grid; grid-template-columns:repeat(3,1fr); gap:12px;
}}
.lr-stat {{
    background:rgba(255,255,255,.03); border:1px solid var(--border-subtle);
    border-radius:var(--radius-sm); padding:14px 16px;
}}
.lr-stat-label {{
    font-size:.63rem; font-weight:600; text-transform:uppercase;
    letter-spacing:.8px; color:var(--text-muted); margin-bottom:6px;
}}
.lr-stat-value {{
    font-family:'JetBrains Mono',monospace; font-size:1.35rem; font-weight:700;
}}

/* ══ Market Breadth ════════════════════════════════════════════════════ */
.br-wrap {{
    max-width:1000px; margin:0 auto; padding:44px 24px 0;
}}
.br-header-row {{
    display:flex; align-items:center; justify-content:space-between; margin-bottom:14px;
}}
.br-section-title {{
    font-size:.78rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:1.2px;
}}
.br-as-of {{
    font-size:.7rem; color:var(--text-muted); font-family:'JetBrains Mono',monospace;
}}
.br-stats-row {{
    display:grid; grid-template-columns:repeat(5,1fr); gap:9px; margin-bottom:12px;
}}
.br-stat {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius-sm); padding:12px 14px;
}}
.br-stat-label {{
    font-size:.59rem; font-weight:600; text-transform:uppercase;
    letter-spacing:.8px; color:var(--text-muted); margin-bottom:5px;
}}
.br-stat-value {{
    font-family:'JetBrains Mono',monospace; font-size:1.1rem; font-weight:700;
}}
.br-delta {{ font-size:.68rem; font-weight:500; margin-left:4px; }}
.br-sub   {{ font-size:.62rem; color:var(--text-muted); margin-top:4px; }}

/* Chart card */
.br-chart-card {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius); padding:20px 22px 18px;
}}
.br-chart-row {{
    display:flex; align-items:center; gap:10px; margin-bottom:12px;
}}
.br-chart-label {{
    font-size:.67rem; font-weight:600; text-transform:uppercase;
    letter-spacing:1px; color:var(--text-secondary);
    display:flex; align-items:center; gap:8px; flex-wrap:wrap;
}}
.br-guide {{
    font-size:.6rem; font-weight:400; letter-spacing:.3px;
    color:var(--text-muted); text-transform:none; margin-left:4px;
}}
.br-pill {{
    display:inline-block; font-size:.58rem; font-weight:600; letter-spacing:.5px;
    text-transform:uppercase; border-radius:4px; padding:2px 7px;
}}
.br-pill-green {{ background:rgba(0,212,170,.15); color:#00d4aa; }}
.br-pill-red   {{ background:rgba(255,74,106,.13); color:#ff4a6a; }}
.br-pill-white {{ background:rgba(255,255,255,.08); color:rgba(255,255,255,.55); }}

.br-chart-wrap  {{ position:relative; height:240px; }}
.br-spread-wrap {{ position:relative; height:150px; }}
.br-divider     {{
    border:none; border-top:1px solid var(--border-subtle);
    margin:16px 0 0;
}}

/* ══ Sector Performance ════════════════════════════════════════════════ */
.sc-wrap {{
    max-width:1060px; margin:0 auto; padding:44px 24px 0;
}}
.sc-header-row {{
    display:flex; align-items:flex-start; justify-content:space-between;
    margin-bottom:16px; gap:12px; flex-wrap:wrap;
}}
.sc-section-title {{
    font-size:.78rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:1.2px;
}}
.sc-header-right {{
    display:flex; flex-direction:column; align-items:flex-end; gap:8px;
}}
.sc-sig-summary {{
    font-size:.62rem; color:var(--text-muted); letter-spacing:.3px;
    display:flex; flex-wrap:wrap; gap:4px; justify-content:flex-end;
}}
.sc-sort-bar {{
    display:flex; align-items:center; gap:6px;
}}
.sc-sort-label {{
    font-size:.62rem; color:var(--text-muted); text-transform:uppercase;
    letter-spacing:.8px;
}}
.sc-sort-btn {{
    font-family:'JetBrains Mono',monospace; font-size:.62rem; font-weight:600;
    background:var(--bg-card); border:1px solid var(--border-subtle);
    color:var(--text-secondary); padding:3px 10px; border-radius:20px;
    cursor:pointer; transition:all .15s;
}}
.sc-sort-btn:hover {{ border-color:var(--border-accent); color:var(--text-primary); }}
.sc-sort-btn.active {{
    background:rgba(74,158,255,.12); border-color:rgba(74,158,255,.35);
    color:var(--accent-blue);
}}
/* ══ Sector Sparkline Grid ═════════════════════════════════════════════ */
.sec-grid {{
    display:grid;
    grid-template-columns:repeat(auto-fill,minmax(185px,1fr));
    gap:9px;
}}
.sec-card {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-left:3px solid transparent;
    border-radius:var(--radius-sm); padding:11px 13px 9px;
    transition:border-color .15s, background .15s;
}}
.sec-card:hover {{ background:var(--bg-card-hover); }}
.sec-card-header {{
    display:flex; align-items:flex-start; justify-content:space-between;
    gap:5px; margin-bottom:5px;
}}
.sec-name {{
    font-size:.57rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:.5px; line-height:1.35;
    flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
}}
.sec-sig {{
    font-size:.50rem; font-weight:700; letter-spacing:.6px;
    padding:2px 6px; border-radius:20px; flex-shrink:0;
    text-transform:uppercase; white-space:nowrap;
}}
.sec-metrics {{
    display:flex; align-items:baseline; justify-content:space-between;
    margin-bottom:4px; gap:4px;
}}
.sec-score {{
    font-family:'JetBrains Mono',monospace; font-size:.92rem;
    font-weight:700; flex-shrink:0;
}}
.sec-ma-tag {{
    font-size:.58rem; font-weight:600; flex-shrink:0; text-align:right;
}}
.sec-ma-gap {{
    font-size:.52rem; opacity:.7; font-family:'JetBrains Mono',monospace;
}}
.sec-spark {{ line-height:0; margin:3px 0 6px; width:100%; }}
.sec-spark svg {{ width:100%; height:52px; }}
.sec-footer {{
    display:flex; align-items:center; justify-content:space-between;
}}
.sec-stat {{
    font-family:'JetBrains Mono',monospace; font-size:.58rem; font-weight:600;
}}
.sec-stat-muted {{ color:var(--text-muted); }}
.sec-card {{ cursor:pointer; }}
.sec-card.sec-selected {{ box-shadow:0 0 0 1.5px var(--border-accent); }}

/* ── Stage header (Per Stage sort) ──────────────────────────────── */
.sec-stage-hdr {{
    grid-column:1/-1; padding:10px 0 3px;
    font-size:.57rem; font-weight:700; text-transform:uppercase; letter-spacing:.8px;
    border-bottom:1px solid var(--border-subtle); margin-bottom:2px;
}}

/* ── Top-10 panel ────────────────────────────────────────────────── */
.sec-panel-area {{
    margin-top:12px; background:var(--bg-card);
    border:1px solid var(--border-accent); border-radius:var(--radius-sm);
    padding:13px 16px; animation:spFadeIn .15s ease;
}}
@keyframes spFadeIn {{ from {{ opacity:0; transform:translateY(-5px); }} to {{ opacity:1; transform:translateY(0); }} }}
.sec-panel-header {{
    display:flex; align-items:center; justify-content:space-between; margin-bottom:10px;
}}
.sec-panel-title {{
    font-size:.62rem; font-weight:700; text-transform:uppercase;
    letter-spacing:.6px; color:var(--text-secondary);
}}
.sec-panel-close {{
    background:none; border:none; color:var(--text-muted); cursor:pointer;
    font-size:.75rem; padding:2px 7px; border-radius:4px; transition:color .15s;
}}
.sec-panel-close:hover {{ color:var(--text-primary); }}
.sec-panel-table {{ width:100%; border-collapse:collapse; }}
.sec-panel-table th {{
    font-family:'JetBrains Mono',monospace; font-size:.54rem; font-weight:600;
    color:var(--text-muted); text-transform:uppercase; letter-spacing:.4px;
    padding:3px 8px 5px; border-bottom:1px solid var(--border-subtle); text-align:right;
}}
.sec-panel-table th:first-child {{ text-align:left; }}
.sec-panel-table td {{
    font-family:'JetBrains Mono',monospace; font-size:.62rem;
    padding:4px 8px; border-bottom:1px solid var(--border-subtle);
    text-align:right; color:var(--text-secondary);
}}
.sec-panel-table td:first-child {{ text-align:left; font-weight:700; color:var(--text-primary); }}
.sec-panel-table tr:last-child td {{ border-bottom:none; }}

/* ══ Shared stat colors ════════════════════════════════════════════════ */
.stat-green {{ color:var(--accent-green); }}
.stat-gold  {{ color:var(--accent-gold);  }}
.stat-blue  {{ color:var(--accent-blue);  }}
.stat-red   {{ color:var(--accent-red);   }}

/* ══ Footer ════════════════════════════════════════════════════════════ */
.footer {{
    border-top:1px solid var(--border-subtle);
    padding:32px 40px; text-align:center;
    font-size:.7rem; color:var(--text-muted); letter-spacing:.3px;
    margin-top:64px;
}}

/* ══ Responsive ════════════════════════════════════════════════════════ */
@media (max-width:700px) {{
    .hero-title    {{ font-size:3rem; letter-spacing:-1px; }}
    .hero          {{ padding:44px 20px 38px; }}
    .lr-stats      {{ grid-template-columns:repeat(3,1fr); }}
    .lr-date       {{ font-size:1.1rem; }}
    .br-stats-row  {{ grid-template-columns:repeat(2,1fr); }}
    .br-chart-wrap {{ height:180px; }}
    .br-spread-wrap{{ height:110px; }}
    .sec-grid      {{ grid-template-columns:repeat(auto-fill,minmax(155px,1fr)); }}
    .sc-header-right {{ align-items:flex-start; }}
    .sc-sig-summary {{ justify-content:flex-start; }}
}}
</style>
</head>
<body>

<!-- ── Hero ──────────────────────────────────────────────────────────── -->
<div class="hero">
    <div class="hero-eyebrow">Market Intelligence System</div>
    <div class="hero-title">Ratatouille</div>
    <div class="hero-subtitle">Momentum Screener &mdash; Full US Market Universe</div>
</div>

{latest_report_html}

{breadth_html}

{sector_html}

<!-- ── Footer ────────────────────────────────────────────────────────── -->
<div class="footer">
    Ratatouille Screener &nbsp;&middot;&nbsp; Updated {now_str}
</div>

</body>
</html>"""


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
    history_by_date     = {h['date']: h for h in market_history_list}

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
                history_by_date[date_str] = ms
                replaced = '(replaced estimate) ' if existing else ''
                print(f'     ↳ {replaced}market scores: long={ms["long_breadth_pct"]}%  '
                      f'short={ms["short_breadth_pct"]}%  sec_avg={ms["sector_avg"]}')

    # ── Save updated market history ───────────────────────────────────────────
    updated_history = list(history_by_date.values())
    save_market_history(archive_dir, updated_history)
    print(f'\n✅  market_score_history.json  →  {len(updated_history)} entries')

    # ── Build index.html ──────────────────────────────────────────────────────
    index_html = build_index_html(reports_with_stats, market_history=updated_history)
    index_path = os.path.join(archive_dir, 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(index_html)

    print(f'✅  Archive updated: {len(reports_with_stats)} report(s)')
    print(f'✅  index.html → {index_path}')
    return True


if __name__ == '__main__':
    main()
