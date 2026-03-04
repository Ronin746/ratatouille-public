"""
update_archive.py — Ratatouille Archive Builder
Scans Ratatouille_YYYY-MM-DD.html reports from Reports/, copies them to Archive/reports/,
reads the corresponding CSV files for stats, and regenerates Archive/index.html.
Only reports on or after CUTOFF_DATE are included.
"""

import os
import re
import shutil
from datetime import datetime


CUTOFF_DATE = '2025-03-02'  # only include reports on or after this date


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
    parent_dir = os.path.dirname(screener_dir)
    date_compact = date_str.replace('-', '')
    # Look in Ratatouille/Data/ first, fall back to Screener/ for legacy files
    csv_path = os.path.join(parent_dir, 'Data', f'screen_results_{date_compact}.csv')
    if not os.path.exists(csv_path):
        csv_path = os.path.join(screener_dir, f'screen_results_{date_compact}.csv')

    stats = {
        'total_stocks': 0,
        'top_ticker': '—',
        'top_score': 0.0,
        'long_candidates': 0,
        'short_candidates': 0
    }

    if not os.path.exists(csv_path):
        return stats

    try:
        import pandas as pd
        df = pd.read_csv(csv_path, index_col=0)
        stats['total_stocks'] = len(df)

        if 'Final_Score' in df.columns:
            top_idx = df['Final_Score'].idxmax()
            stats['top_ticker'] = str(top_idx)
            stats['top_score'] = round(float(df.loc[top_idx, 'Final_Score']), 1)
            stats['long_candidates'] = int((df['Final_Score'] >= 75).sum())

        if 'Short_Score' in df.columns:
            stats['short_candidates'] = int((df['Short_Score'] >= 70).sum())

    except Exception:
        pass

    return stats


def format_date_display(date_str):
    """2026-02-26 → Thursday, Feb 26, 2026"""
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%A, %b %d, %Y')
    except Exception:
        return date_str


def build_index_html(reports_with_stats):
    """Build the complete index.html for the archive site."""

    cards_html = ""
    for i, (date_str, stats) in enumerate(reports_with_stats):
        is_latest = (i == 0)
        date_display = format_date_display(date_str)

        latest_badge = (
            '<span class="latest-badge">● LATEST</span>'
            if is_latest else ''
        )
        card_class = 'report-card report-card--latest' if is_latest else 'report-card'

        total_str = f"{stats['total_stocks']:,}" if stats['total_stocks'] else '—'
        score_str = f"{stats['top_score']}" if stats['top_score'] else '—'
        long_str  = str(stats['long_candidates'])  if stats['long_candidates']  else '0'
        short_str = str(stats['short_candidates']) if stats['short_candidates'] else '0'

        cards_html += f"""
        <a href="reports/{date_str}.html" class="{card_class}">
            <div class="report-card-header">
                <div class="report-date-info">
                    <div class="report-date">{date_display}</div>
                    {latest_badge}
                </div>
                <div class="report-arrow">&#8594;</div>
            </div>
            <div class="report-stats">
                <div class="report-stat">
                    <div class="rstat-label">Scanned</div>
                    <div class="rstat-value stat-blue">{total_str}</div>
                </div>
                <div class="report-stat">
                    <div class="rstat-label">Top Pick</div>
                    <div class="rstat-value stat-gold">{stats['top_ticker']}</div>
                </div>
                <div class="report-stat">
                    <div class="rstat-label">Top Score</div>
                    <div class="rstat-value stat-green">{score_str}</div>
                </div>
                <div class="report-stat">
                    <div class="rstat-label">&#128994; Long</div>
                    <div class="rstat-value stat-green">{long_str}</div>
                </div>
                <div class="report-stat">
                    <div class="rstat-label">&#128308; Short</div>
                    <div class="rstat-value stat-red">{short_str}</div>
                </div>
            </div>
        </a>"""

    now_str    = datetime.now().strftime('%b %d, %Y at %H:%M')
    total_runs = len(reports_with_stats)

    empty_state = (
        "<div class='empty-state'>"
        "<div class='empty-state-icon'>&#128202;</div>"
        "<div class='empty-state-text'>No reports yet. Run the screener to generate your first report.</div>"
        "</div>"
    )

    content_html = empty_state if not reports_with_stats else cards_html

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ratatouille &#8212; Market Intelligence Archive</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root {{
    --bg-primary:   #080810;
    --bg-card:      #10101a;
    --bg-card-hover:#18182a;
    --border-subtle:#1c1c30;
    --border-accent:#2a2a44;
    --text-primary: #e8e8f0;
    --text-secondary:#8888a0;
    --text-muted:   #505068;
    --accent-green: #00d4aa;
    --accent-gold:  #f5a623;
    --accent-blue:  #4a9eff;
    --accent-red:   #ff4a6a;
    --radius:       16px;
    --radius-sm:    10px;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
    background:var(--bg-primary);
    color:var(--text-primary);
    min-height:100vh;
    line-height:1.6;
}}

/* ── Hero ── */
.hero {{
    background:linear-gradient(135deg,#0a0a18 0%,#100e22 50%,#0a1018 100%);
    border-bottom:1px solid var(--border-subtle);
    padding:72px 40px 64px;
    text-align:center;
    position:relative;
    overflow:hidden;
}}
.hero::before {{
    content:'';
    position:absolute; inset:0;
    background:
        radial-gradient(ellipse 80% 60% at 50% 0%,  rgba(74,158,255,0.07),transparent),
        radial-gradient(ellipse 60% 40% at 15% 100%,rgba(0,212,170,0.05), transparent),
        radial-gradient(ellipse 50% 40% at 85% 80%, rgba(139,92,246,0.04),transparent);
    pointer-events:none;
}}
.hero-eyebrow {{
    font-size:.7rem; font-weight:600; letter-spacing:3px;
    text-transform:uppercase; color:var(--accent-blue);
    margin-bottom:16px; opacity:.8;
}}
.hero-title {{
    font-size:4.5rem; font-weight:800; letter-spacing:-3px; line-height:1;
    background:linear-gradient(135deg,#e8e8f0 0%,#aaaacc 60%,#8888aa 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    margin-bottom:14px;
}}
.hero-subtitle {{
    font-size:1rem; color:var(--text-secondary); margin-bottom:40px;
}}
.hero-meta {{
    display:flex; justify-content:center; align-items:center;
    gap:40px; flex-wrap:wrap;
}}
.hero-stat-val {{
    font-family:'JetBrains Mono',monospace;
    font-size:2.2rem; font-weight:700; display:block;
    background:linear-gradient(135deg,var(--accent-green),var(--accent-blue));
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
}}
.hero-stat-label {{
    font-size:.7rem; color:var(--text-muted);
    text-transform:uppercase; letter-spacing:1.2px; font-weight:600;
    margin-top:2px;
}}
.hero-divider {{
    width:1px; height:44px; background:var(--border-accent);
}}

/* ── Main ── */
.main {{
    max-width:1000px; margin:0 auto; padding:52px 24px 80px;
}}
.section-header {{
    display:flex; align-items:center; justify-content:space-between; margin-bottom:24px;
}}
.section-title {{
    font-size:.78rem; font-weight:600; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:1.2px;
}}
.last-updated {{
    font-size:.7rem; color:var(--text-muted); font-family:'JetBrains Mono',monospace;
}}

/* ── Report Cards ── */
.reports-grid {{ display:flex; flex-direction:column; gap:12px; }}

.report-card {{
    background:var(--bg-card);
    border:1px solid var(--border-subtle);
    border-radius:var(--radius);
    padding:22px 28px;
    text-decoration:none; color:inherit; display:block;
    transition:all .2s ease; position:relative; overflow:hidden;
}}
.report-card::after {{
    content:''; position:absolute; inset:0;
    background:linear-gradient(135deg,transparent,rgba(74,158,255,.025));
    opacity:0; transition:opacity .2s;
}}
.report-card:hover {{
    border-color:var(--border-accent);
    background:var(--bg-card-hover);
    transform:translateY(-2px);
    box-shadow:0 12px 40px rgba(0,0,0,.5);
}}
.report-card:hover::after {{ opacity:1; }}
.report-card--latest {{
    border-color:rgba(0,212,170,.28);
    background:linear-gradient(135deg,#10101a,#0d1a18);
    box-shadow:0 0 50px rgba(0,212,170,.07);
}}
.report-card--latest:hover {{
    border-color:rgba(0,212,170,.5);
    box-shadow:0 12px 50px rgba(0,212,170,.12);
}}

.report-card-header {{
    display:flex; align-items:center; justify-content:space-between; margin-bottom:18px;
}}
.report-date-info {{ display:flex; align-items:center; gap:14px; }}
.report-date {{
    font-size:1.08rem; font-weight:600; color:var(--text-primary);
}}
.latest-badge {{
    font-size:.63rem; font-weight:700; letter-spacing:1px;
    color:var(--accent-green); background:rgba(0,212,170,.1);
    border:1px solid rgba(0,212,170,.2); border-radius:20px;
    padding:3px 12px; text-transform:uppercase;
}}
.report-arrow {{
    color:var(--text-muted); font-size:1.15rem;
    transition:all .2s; font-family:'JetBrains Mono',monospace;
}}
.report-card:hover .report-arrow {{ color:var(--accent-green); transform:translateX(6px); }}
.report-card--latest .report-arrow {{ color:var(--accent-green); }}

.report-stats {{
    display:grid; grid-template-columns:repeat(5,1fr); gap:10px;
}}
.report-stat {{
    background:rgba(255,255,255,.02); border-radius:var(--radius-sm);
    padding:10px 12px; border:1px solid var(--border-subtle);
}}
.rstat-label {{
    font-size:.62rem; font-weight:600; text-transform:uppercase;
    letter-spacing:.8px; color:var(--text-muted); margin-bottom:5px;
}}
.rstat-value {{
    font-family:'JetBrains Mono',monospace; font-size:1.05rem; font-weight:700;
}}
.stat-green {{ color:var(--accent-green); }}
.stat-gold  {{ color:var(--accent-gold);  }}
.stat-blue  {{ color:var(--accent-blue);  }}
.stat-red   {{ color:var(--accent-red);   }}

/* ── Empty State ── */
.empty-state {{
    text-align:center; padding:80px 20px; color:var(--text-muted);
}}
.empty-state-icon  {{ font-size:3rem; margin-bottom:16px; }}
.empty-state-text  {{ font-size:1rem; }}

/* ── Footer ── */
.footer {{
    border-top:1px solid var(--border-subtle); padding:28px 40px;
    text-align:center; font-size:.7rem; color:var(--text-muted);
    letter-spacing:.3px;
}}

/* ── Responsive ── */
@media (max-width:640px) {{
    .report-stats  {{ grid-template-columns:repeat(3,1fr); }}
    .hero-divider  {{ display:none; }}
    .hero-title    {{ font-size:3rem; letter-spacing:-1px; }}
    .hero          {{ padding:48px 20px 40px; }}
    .report-card   {{ padding:18px 20px; }}
}}
</style>
</head>
<body>

<div class="hero">
    <div class="hero-eyebrow">Market Intelligence System</div>
    <div class="hero-title">Ratatouille</div>
    <div class="hero-subtitle">7-Factor Momentum Screener &mdash; Full US Market Universe</div>
    <div class="hero-meta">
        <div class="hero-stat">
            <span class="hero-stat-val">{total_runs}</span>
            <div class="hero-stat-label">Total Runs</div>
        </div>
        <div class="hero-divider"></div>
        <div class="hero-stat">
            <span class="hero-stat-val">7K+</span>
            <div class="hero-stat-label">Tickers / Run</div>
        </div>
        <div class="hero-divider"></div>
        <div class="hero-stat">
            <span class="hero-stat-val">7</span>
            <div class="hero-stat-label">Factors</div>
        </div>
    </div>
</div>

<div class="main">
    <div class="section-header">
        <div class="section-title">Run Archive</div>
        <div class="last-updated">Updated {now_str}</div>
    </div>
    <div class="reports-grid">
        {content_html}
    </div>
</div>

<div class="footer">
    Ratatouille Screener &nbsp;&middot;&nbsp; Local Archive
</div>

</body>
</html>"""


def main():
    screener_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir   = os.path.dirname(screener_dir)
    archive_dir  = os.path.join(parent_dir, 'Archive')
    reports_dir  = os.path.join(archive_dir, 'reports')

    os.makedirs(reports_dir, exist_ok=True)

    reports = find_reports()

    # Remove archive reports no longer in the kept set (older than cutoff)
    keep_names = {f'{date_str}.html' for date_str, _ in reports}
    for fname in os.listdir(reports_dir):
        if fname.endswith('.html') and fname not in keep_names:
            try:
                os.remove(os.path.join(reports_dir, fname))
                print(f"  ✗ removed outdated: {fname}")
            except OSError:
                pass

    reports_with_stats = []
    for date_str, src_path in reports:
        dst_path = os.path.join(reports_dir, f'{date_str}.html')
        shutil.copy2(src_path, dst_path)
        stats = get_csv_stats(date_str)
        reports_with_stats.append((date_str, stats))
        print(f"  → {date_str} copied  |  {stats['total_stocks']} stocks, top: {stats['top_ticker']}")

    index_html = build_index_html(reports_with_stats)
    index_path = os.path.join(archive_dir, 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(index_html)

    print(f"\n✅  Archive updated: {len(reports_with_stats)} report(s)")
    print(f"✅  index.html → {index_path}")
    return True


if __name__ == '__main__':
    main()
