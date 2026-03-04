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

        sectors    = {}
        sector_avg = 0.0
        if 'Sector' in df.columns:
            named         = df[df['Sector'] != 'Other']
            sector_counts = named.groupby('Sector').size()
            sector_means  = named.groupby('Sector')['Final_Score'].mean()
            for sec, avg in sector_means.items():
                if sector_counts.get(sec, 0) >= 3:
                    sectors[sec] = round(float(avg), 1)
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

    ratio = l_pct / s_pct if s_pct > 0 else 99
    if ratio > 2.0:
        bias_label, bias_color = 'BULLISH', '#00d4aa'
    elif ratio > 1.2:
        bias_label, bias_color = 'NEUTRAL', '#f5a623'
    else:
        bias_label, bias_color = 'BEARISH', '#ff4a6a'

    latest_date_display = format_date_display(latest['date'])

    jdates = json.dumps(display_dates)
    jlong  = json.dumps(long_breadth)
    jshort = json.dumps(short_breadth)

    return f"""
<!-- ── Market Breadth ────────────────────────────────────────────────── -->
<div class="br-wrap">
  <div class="br-header-row">
    <div class="br-section-title">Market Breadth</div>
    <div class="br-as-of">as of {latest_date_display}</div>
  </div>

  <div class="br-stats-row">
    <div class="br-stat">
      <div class="br-stat-label">Market Bias</div>
      <div class="br-stat-value" style="color:{bias_color}">{bias_label}</div>
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
      <div class="br-stat-label">Sector Avg Score</div>
      <div class="br-stat-value stat-blue">{s_avg}
        <span class="br-delta" style="color:{a_dcol}">{a_dstr}</span>
      </div>
    </div>
  </div>

  <div class="br-chart-card">
    <div class="br-chart-title">Breadth Trend &mdash; % stocks scoring 70+</div>
    <div class="br-chart-wrap"><canvas id="breadthChart"></canvas></div>
  </div>
</div>
<!-- ── /Market Breadth ───────────────────────────────────────────────── -->

<script>
(function() {{
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
      plugins: {{
        legend: {{
          display: true, position: 'top',
          labels: {{ color: '#8888a0', font: {{ size: 11 }}, boxWidth: 12, padding: 16 }}
        }},
        tooltip: {{
          backgroundColor: '#10101a', borderColor: '#2a2a44', borderWidth: 1,
          titleColor: '#e8e8f0', bodyColor: '#8888a0',
          callbacks: {{ label: function(c) {{ return ' ' + c.dataset.label + ': ' + c.parsed.y + '%'; }} }}
        }}
      }},
      scales: {{
        x: {{ grid: {{ color: 'rgba(255,255,255,0.04)' }}, ticks: {{ color: '#505068', font: {{ size: 11 }} }} }},
        y: {{
          grid: {{ color: 'rgba(255,255,255,0.04)' }},
          ticks: {{ color: '#505068', font: {{ size: 11 }}, callback: function(v) {{ return v + '%'; }} }},
          suggestedMin: 0
        }}
      }}
    }}
  }});
}})();
</script>
"""


def build_sector_charts_html(market_history):
    """
    Sector Performance section:
      1. Top-10 multi-line trend Chart.js
      2. Full sparkline grid for all named sectors (sorted by latest score)
    """
    if not market_history:
        return ''

    history = sorted(market_history, key=lambda x: x['date'])

    # Display dates for chart labels
    display_dates = []
    for d in [h['date'] for h in history]:
        try:
            display_dates.append(datetime.strptime(d, '%Y-%m-%d').strftime('%b %d'))
        except Exception:
            display_dates.append(d)

    # Latest snapshot
    latest         = history[-1]
    latest_sectors = latest.get('sectors', {})
    if not latest_sectors:
        return ''

    # All sectors that appear in the latest snapshot, sorted by score
    sectors_sorted = sorted(latest_sectors.items(), key=lambda x: x[1], reverse=True)

    # ── Top-10 multi-line datasets ────────────────────────────────────────────
    top10 = sectors_sorted[:10]
    datasets = []
    for i, (sec_name, _) in enumerate(top10):
        color = SECTOR_COLORS[i % len(SECTOR_COLORS)]
        data  = [h.get('sectors', {}).get(sec_name) for h in history]  # None = gap
        datasets.append({
            'label':              sec_name,
            'data':               data,
            'borderColor':        color,
            'backgroundColor':    'transparent',
            'tension':            0.3,
            'pointRadius':        3,
            'pointBackgroundColor': color,
            'borderWidth':        2,
            'spanGaps':           True,
        })

    # ── Sparkline grid — all sectors ──────────────────────────────────────────
    grid_html = ''
    for sec_name, latest_score in sectors_sorted:
        values    = [h.get('sectors', {}).get(sec_name) for h in history]
        spark_svg = make_sparkline_svg(values)

        if latest_score >= 70:   score_color = '#00d4aa'
        elif latest_score >= 60: score_color = '#4a9eff'
        elif latest_score >= 50: score_color = '#f5a623'
        else:                    score_color = '#ff4a6a'

        valid_vals = [v for v in values if v is not None]
        if len(valid_vals) >= 2:
            delta = round(valid_vals[-1] - valid_vals[0], 1)
            if   delta >  0.5: trend_str, trend_col = f'+{delta}', '#00d4aa'
            elif delta < -0.5: trend_str, trend_col = str(delta),  '#ff4a6a'
            else:              trend_str, trend_col = '—',         '#505068'
        else:
            trend_str, trend_col = '—', '#505068'

        # Truncate long sector names for display
        disp_name = sec_name if len(sec_name) <= 22 else sec_name[:20] + '…'

        grid_html += f"""
    <div class="sec-card">
      <div class="sec-card-top">
        <div class="sec-name" title="{sec_name}">{disp_name}</div>
        <div class="sec-score" style="color:{score_color}">{latest_score}</div>
      </div>
      <div class="sec-spark">{spark_svg}</div>
      <div class="sec-trend" style="color:{trend_col}">{trend_str}</div>
    </div>"""

    jdates    = json.dumps(display_dates)
    jdatasets = json.dumps(datasets)
    n_sectors = len(sectors_sorted)

    return f"""
<!-- ── Sector Performance ───────────────────────────────────────────── -->
<div class="sc-wrap">
  <div class="sc-header-row">
    <div class="sc-section-title">Sector Performance</div>
    <div class="sc-count">{n_sectors} sectors tracked</div>
  </div>

  <!-- Top-10 trend chart -->
  <div class="sc-chart-card">
    <div class="sc-chart-title">Top 10 Sectors &mdash; Score Trend Over Time</div>
    <div class="sc-chart-wrap"><canvas id="sectorTrendChart"></canvas></div>
  </div>

  <!-- All-sector sparkline grid -->
  <div class="sec-grid">
    {grid_html}
  </div>
</div>
<!-- ── /Sector Performance ───────────────────────────────────────────── -->

<script>
(function() {{
  var ctx = document.getElementById('sectorTrendChart').getContext('2d');
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: {jdates},
      datasets: {jdatasets}
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{
          display: true, position: 'right',
          labels: {{ color: '#8888a0', font: {{ size: 10 }}, boxWidth: 10,
                    padding: 8, usePointStyle: true }}
        }},
        tooltip: {{
          backgroundColor: '#10101a', borderColor: '#2a2a44', borderWidth: 1,
          titleColor: '#e8e8f0', bodyColor: '#8888a0',
          callbacks: {{
            label: function(c) {{
              if (c.parsed.y === null || c.parsed.y === undefined) return null;
              return ' ' + c.dataset.label + ': ' + c.parsed.y.toFixed(1);
            }}
          }}
        }}
      }},
      scales: {{
        x: {{ grid: {{ color: 'rgba(255,255,255,0.04)' }}, ticks: {{ color: '#505068', font: {{ size: 10 }} }} }},
        y: {{
          grid: {{ color: 'rgba(255,255,255,0.04)' }},
          ticks: {{ color: '#505068', font: {{ size: 10 }} }},
          min: 35, max: 85
        }}
      }}
    }}
  }});
}})();
</script>
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
        breadth_html = build_breadth_html(market_history)
        sector_html  = build_sector_charts_html(market_history)

    now_str = datetime.now().strftime('%b %d, %Y at %H:%M')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ratatouille &#8212; Market Intelligence Archive</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
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
    display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:12px;
}}
.br-stat {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius-sm); padding:12px 14px;
}}
.br-stat-label {{
    font-size:.6rem; font-weight:600; text-transform:uppercase;
    letter-spacing:.8px; color:var(--text-muted); margin-bottom:5px;
}}
.br-stat-value {{
    font-family:'JetBrains Mono',monospace; font-size:1.15rem; font-weight:700;
}}
.br-delta {{ font-size:.7rem; font-weight:500; margin-left:4px; }}
.br-chart-card {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius); padding:20px 22px 16px;
}}
.br-chart-title {{
    font-size:.68rem; font-weight:600; text-transform:uppercase;
    letter-spacing:1px; color:var(--text-secondary); margin-bottom:14px;
}}
.br-chart-wrap {{ position:relative; height:220px; }}

/* ══ Sector Performance ════════════════════════════════════════════════ */
.sc-wrap {{
    max-width:1000px; margin:0 auto; padding:44px 24px 0;
}}
.sc-header-row {{
    display:flex; align-items:center; justify-content:space-between; margin-bottom:14px;
}}
.sc-section-title {{
    font-size:.78rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:1.2px;
}}
.sc-count {{
    font-size:.7rem; color:var(--text-muted); font-family:'JetBrains Mono',monospace;
}}
.sc-chart-card {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius); padding:20px 22px 16px; margin-bottom:14px;
}}
.sc-chart-title {{
    font-size:.68rem; font-weight:600; text-transform:uppercase;
    letter-spacing:1px; color:var(--text-secondary); margin-bottom:14px;
}}
.sc-chart-wrap {{ position:relative; height:340px; }}

/* ══ Sector Sparkline Grid ═════════════════════════════════════════════ */
.sec-grid {{
    display:grid;
    grid-template-columns:repeat(auto-fill,minmax(140px,1fr));
    gap:8px;
}}
.sec-card {{
    background:var(--bg-card); border:1px solid var(--border-subtle);
    border-radius:var(--radius-sm); padding:10px 12px 8px;
    transition:border-color .15s;
}}
.sec-card:hover {{ border-color:var(--border-accent); }}
.sec-card-top {{
    display:flex; align-items:flex-start; justify-content:space-between;
    margin-bottom:5px;
}}
.sec-name {{
    font-size:.58rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:.5px; line-height:1.35;
    flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
    margin-right:6px;
}}
.sec-score {{
    font-family:'JetBrains Mono',monospace; font-size:.82rem;
    font-weight:700; flex-shrink:0;
}}
.sec-spark {{ line-height:0; margin:2px 0 4px; }}
.sec-trend {{
    font-family:'JetBrains Mono',monospace; font-size:.58rem; font-weight:600;
}}

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
    .hero-title          {{ font-size:3rem; letter-spacing:-1px; }}
    .hero                {{ padding:44px 20px 38px; }}
    .lr-stats            {{ grid-template-columns:repeat(3,1fr); }}
    .lr-date             {{ font-size:1.1rem; }}
    .br-stats-row        {{ grid-template-columns:repeat(2,1fr); }}
    .sc-chart-wrap       {{ height:260px; }}
    .sec-grid            {{ grid-template-columns:repeat(auto-fill,minmax(110px,1fr)); }}
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

        if date_str not in history_by_date:
            ms = compute_market_scores(date_str)
            if ms:
                history_by_date[date_str] = ms
                print(f'     ↳ market scores: long={ms["long_breadth_pct"]}%  '
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
