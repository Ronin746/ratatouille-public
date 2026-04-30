
import schedule
import time
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
import pytz
from tabulate import tabulate

from config import BENCHMARK_TICKER, WEIGHTS, get_market_tickers
from data_fetcher import fetch_data, get_ticker_data
import indicators as ind
import scorer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("screener.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_screener(tickers=None):
    logger.info("Starting Daily Screen...")
    from datetime import datetime
    
    # 0. Get Universe
    if tickers is None:
        tickers = get_market_tickers()
    
    logger.info("Total Universe Size: %d", len(tickers))
    
    # 1. Fetch Data
    logger.info("Fetching data...")
    # Fetch data for all tickers + benchmark
    # Use '1y' to ensure enough data for 200 SMA if needed, or safely covered 3M + indicators
    raw_data = fetch_data(tickers + [BENCHMARK_TICKER], period="1y")

    if raw_data.empty:
        logger.error("No data fetched. Aborting.")
        return

    # ── Use only the previous NY session close ────────────────────────────────
    # Drop any bar whose date is today (ET) — could be partial/intraday.
    # The screener always works on the last *completed* NY session.
    #
    # FIX (yfinance 1.x): The downloaded index may be tz-aware (UTC).
    # Comparing a tz-aware index to a tz-naive Timestamp raises a TypeError
    # in pandas ≥ 2.x (or silently misbehaves in older versions), causing the
    # "today" bar to slip through with NaN High/Low and Volume=0 — which
    # zeroes out atr_pct, adr_pct, volume_surge for every ticker.
    # Fix: normalize the index to tz-naive NYC dates before filtering.
    et_tz    = pytz.timezone("America/New_York")
    now_et   = datetime.now(et_tz)
    today_et = pd.Timestamp(now_et.date())

    if raw_data.index.tz is not None:
        # Convert UTC index → NYC local date → tz-naive for comparison
        idx_et = raw_data.index.tz_convert(et_tz).normalize().tz_localize(None)
    else:
        idx_et = raw_data.index.normalize()

    # After 16:00 ET the market is closed → today's bar is complete, include it.
    # Before 16:00 ET the session is still open → exclude the partial bar.
    if now_et.hour >= 16:
        raw_data = raw_data[idx_et <= today_et]
    else:
        raw_data = raw_data[idx_et < today_et]
    if raw_data.empty:
        logger.error("No completed session data available after date filter. Aborting.")
        return
    session_date = raw_data.index[-1].date()
    logger.info("Data truncated to last completed NY session: %s", session_date)

    benchmark_data = get_ticker_data(raw_data, BENCHMARK_TICKER)
    if benchmark_data is None or benchmark_data.empty:
        logger.error("Benchmark data missing. Aborting.")
        return

    results = []
    
    # 2. Calculate Indicators Loop
    logger.info("Calculating indicators...")
    for ticker in tickers:
        try:
            df = get_ticker_data(raw_data, ticker)
            if df is None or df.empty:
                continue
                
            # Metrics
            pp = ind.calc_price_performance(df)
            bc = ind.calc_bullish_candles(df)
            ma = ind.calc_ma_alignment(df)
            tc = ind.calc_trend_consistency(df)
            vol = ind.calc_volatility(df, atr_length=14, adr_length=20)
            v = ind.calc_volume(df)
            rs = ind.calc_relative_strength(df, benchmark_data)
            w30 = ind.calc_weekly_sma30_dist(df)
            
            if not all([pp, bc, ma, tc, vol, v, rs]):
                logger.warning("Insufficient data for %s, skipping.", ticker)
                continue

            # Derived: ATR distance from 50 SMA daily
            _last_price = pp.get('last_price', 0.0)
            _ma50 = ma.get('ma50', 0)
            _atr_pct = vol.get('atr_pct', 0.0)
            if _ma50 and _ma50 > 0 and _atr_pct and _atr_pct > 0:
                _dist_50_pct = (_last_price - _ma50) / _ma50
                _atr_dist_50 = _dist_50_pct / _atr_pct
            else:
                _atr_dist_50 = 0.0
                
            # Flatten into a dict
            row = {
                'Ticker': ticker,
                # Price Performance
                '3m_return':  pp['3m_return'],
                '1m_return':  pp['1m_return'],
                '1w_return':  pp.get('1w_return', 0.0),
                '3d_return':  pp.get('3d_return', 0.0),
                '1d_return':  pp.get('1d_return', 0.0),
                'last_price': pp.get('last_price', 0.0),
                'ema21':      pp.get('ema21', 0.0),
                'ema21_dist': pp.get('ema21_dist', 0.0),
                'ema60':      pp.get('ema60', 0.0),
                'ema60_dist': pp.get('ema60_dist', 0.0),
                'low_52w':    pp.get('low_52w', 0.0),
                'dist_from_52w_low': pp.get('dist_from_52w_low', 0.0),
                'perf_52w':   pp.get('perf_52w', 0.0),
                'avg_dollar_vol': pp.get('avg_dollar_vol', 0.0),
                'r_squared':  pp['r_squared'],
                'r_squared_15d': pp.get('r_squared_15d', 0.0),
                'slope':      pp['slope'],
                # Weekly 30 SMA & ATR dist from 50 SMA
                'sma30w_dist':    w30.get('sma30w_dist', 0.0),
                'atr_dist_50sma': round(_atr_dist_50, 2),
                # Bullish Candles
                'bullish_ratio': bc['bullish_ratio'],
                'strong_bullish_count': bc['strong_bullish_count'],
                # MA
                'ma_aligned': ma['aligned'],
                'ma_positive_slopes': ma['positive_slopes'],
                'ma10': ma.get('ma10', 0),
                'ma20': ma.get('ma20', 0),
                'ma30': ma.get('ma30', 0),
                'ma50': ma.get('ma50', 0),
                # Trend
                'consistency_score': tc['consistency_score'],
                'll_lh_score':       tc['ll_lh_score'],
                'max_drawdown':      tc['max_drawdown'],
                # Volatility
                'atr_stability': vol['atr_stability'],
                'adr_pct': vol.get('adr_pct', 0.0),
                'atr_pct': vol.get('atr_pct', 0.0),
                # Volume
                'up_down_ratio': v['up_down_ratio'],
                'volume_surge': v['volume_surge'],
                # RS
                'rs_rating': rs['rs_rating']
            }
            results.append(row)
            
        except Exception as e:
            logger.error("Error processing %s: %s", ticker, e)
            continue

    if not results:
        logger.error("No results generated.")
        return

    # 3. Score and Rank
    results_df = pd.DataFrame(results).set_index('Ticker')
    ranked_df = scorer.calculate_scores(results_df, WEIGHTS)

    # 3b. Compute Short Scores and merge into ranked_df
    # calculate_short_scores() inverts all 7 factors so that HIGH Short_Score = very weak stock.
    # Without this step, short analysis falls back to Final_Score < 45 which misses most candidates.
    short_scored_df = scorer.calculate_short_scores(results_df, WEIGHTS)
    short_cols = ['Short_Score', 'Short_Price', 'Short_Candles', 'Short_MA',
                  'Short_Trend', 'Short_Vol', 'Short_Volume', 'Short_RS']
    for col in short_cols:
        if col in short_scored_df.columns:
            ranked_df[col] = short_scored_df[col]
    short_count = (ranked_df['Short_Score'] > 60).sum() if 'Short_Score' in ranked_df.columns else 'N/A'
    logger.info("Short scores computed. Stocks with Short_Score > 60: %s", short_count)

    # 3c. Add Sector column from basket map
    import sector_baskets as _sb
    _basket_map = _sb.build_ticker_basket_map()
    ranked_df['Sector'] = ranked_df.index.map(lambda t: _basket_map.get(t, 'Other'))
    logger.info("Sector column added. %d tickers have a basket sector.", (ranked_df['Sector'] != 'Other').sum())

    # 4. output
    top_10 = ranked_df.head(10)
    
    # Console Output
    table = tabulate(top_10[['Final_Score', '3m_return', 'rs_rating', 'bullish_ratio']], 
                     headers='keys', tablefmt='psql', floatfmt=".2f")
    logger.info("\nTop 10 Stocks:\n%s", table)
    
    # Save to CSV → Ratatouille/Data/
    import os as _os
    _data_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'Data')
    _os.makedirs(_data_dir, exist_ok=True)
    _new_csv_name = f"screen_results_{session_date.strftime('%Y%m%d')}.csv"
    # ── Cleanup: remove CSV files from previous sessions ──────────────────────
    try:
        for _f in _os.listdir(_data_dir):
            if _f.startswith('screen_results_') and _f.endswith('.csv') and _f != _new_csv_name:
                _old = _os.path.join(_data_dir, _f)
                _os.remove(_old)
                logger.info("Removed old CSV: %s", _f)
    except Exception as _e:
        logger.warning("CSV cleanup warning: %s", _e)
    filename = _os.path.join(_data_dir, _new_csv_name)
    ranked_df.to_csv(filename)
    logger.info("Full results saved to %s", filename)
    
    # 6. Generate Reports
    import report_generator
    import webbrowser
    try:
        import os
        # Save HTML next to Basket.docx in the Ratatouille root folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)

        date_str = session_date.strftime('%Y-%m-%d')
        filename = f"BlackRat_{date_str}.html"
        reports_dir = os.path.join(project_root, 'Reports')
        os.makedirs(reports_dir, exist_ok=True)
        # ── Cleanup: remove HTML reports from previous sessions ───────────────
        try:
            import re as _re
            _html_pat = _re.compile(r'^BlackRat_\d{4}-\d{2}-\d{2}\.html$')
            for _f in os.listdir(reports_dir):
                if _html_pat.match(_f) and _f != filename:
                    _old = os.path.join(reports_dir, _f)
                    os.remove(_old)
                    logger.info("Removed old report: %s", _f)
        except Exception as _e:
            logger.warning("Report cleanup warning: %s", _e)
        full_path = os.path.join(reports_dir, filename)

        # ── LONG Analysis (using filtered ranked_df) ──
        import sector_baskets
        logger.info("Analyzing sector baskets (Long)...")
        basket_df = sector_baskets.analyze_baskets(ranked_df)

        import candidate_scanner
        logger.info("Running Big Winner Candidate Scanner...")
        candidates_df = candidate_scanner.scan_candidates(ranked_df, basket_df)
        logger.info("Long candidates found: %d", len(candidates_df) if candidates_df is not None and not candidates_df.empty else 0)

        # ── SHORT Analysis (using filtered ranked_df) ──
        logger.info("Analyzing sector baskets (Short)...")
        short_basket_df = sector_baskets.analyze_baskets_short(ranked_df)

        logger.info("Running Short Candidate Scanner...")
        short_candidates_df = candidate_scanner.scan_short_candidates(ranked_df, short_basket_df)
        logger.info("Short candidates found: %d", len(short_candidates_df) if short_candidates_df is not None and not short_candidates_df.empty else 0)

        # ── Generate HTML Dashboard ──
        logger.info("Generating HTML dashboard...")
        report_path = report_generator.generate_html_report(
            ranked_df,
            filename=full_path,
            basket_df=basket_df,
            candidates_df=candidates_df,
            short_basket_df=short_basket_df,
            short_candidates_df=short_candidates_df,
            session_date=session_date,
        )
        logger.info("Dashboard saved to: %s", report_path)

        # Google Sheets Export (Top 120 from filtered set)
        try:
            from sheets_manager import update_sheet
            top_120 = ranked_df.head(120)
            update_sheet(top_120)
        except ImportError:
            pass
        except Exception:
            pass

        # Report saved — site is opened by START_SCREENER.command after Netlify deploy

        # ── Auto git push (solo in locale, su GitHub Actions ci pensa il workflow) ──
        if os.environ.get("CI"):
            logger.info("Git push saltato: rilevato ambiente CI (GitHub Actions).")
        else:
            try:
                import subprocess, shutil, stat
                # Use git.exe directly to avoid git.cmd wrapper (causes CMD errors on Windows)
                git_cmd = shutil.which("git.exe") or shutil.which("git") or "git"
                repo_root = os.path.dirname(script_dir)
                logger.info("Git: staging and pushing changes...")

                # Fix COMMIT_EDITMSG permissions (common Windows lock issue)
                commit_msg_path = os.path.join(repo_root, ".git", "COMMIT_EDITMSG")
                if os.path.exists(commit_msg_path):
                    try:
                        os.chmod(commit_msg_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
                    except Exception:
                        pass

                subprocess.run([git_cmd, "add", "Screener/", "Archive/index.html",
                                "Archive/market_score_history.json"],
                               cwd=repo_root, check=True)
                subprocess.run([git_cmd, "commit", "-m",
                                f"Auto update - {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
                               cwd=repo_root, check=True)
                subprocess.run([git_cmd, "push", "origin", "main"],
                               cwd=repo_root, check=True)
                logger.info("Git: push completed.")
            except subprocess.CalledProcessError as e:
                logger.warning("Git push failed: %s", e)
            except Exception as e:
                logger.warning("Git push error: %s", e)

    except Exception as e:
        logger.error("Error generating outputs: %s", e)
        import traceback
        logger.error(traceback.format_exc())
    
    return ranked_df

def job():
    logger.info("Executing scheduled job...")
    run_screener()

if __name__ == "__main__":
    import sys
    import config
    
    # Check for argument to run immediately
    if len(sys.argv) > 1:
        if "--now" in sys.argv:
            run_screener()
        elif "--test" in sys.argv:
            logger.info("Running in TEST mode (Top 50 tickers)")
            all_tickers = get_market_tickers()
            test_tickers = all_tickers[:50]
            run_screener(tickers=test_tickers)
    else:
        logger.info("Scheduler started. Waiting for 22:30 (Mon–Fri)...")
        schedule.every().monday.at("22:30").do(job)
        schedule.every().tuesday.at("22:30").do(job)
        schedule.every().wednesday.at("22:30").do(job)
        schedule.every().thursday.at("22:30").do(job)
        schedule.every().friday.at("22:30").do(job)

        while True:
            schedule.run_pending()
            time.sleep(60)
