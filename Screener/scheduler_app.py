
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
    
    logger.info(f"Total Universe Size: {len(tickers)}")
    
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
    logger.info(f"Data truncated to last completed NY session: {session_date}")

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
            
            if not all([pp, bc, ma, tc, vol, v, rs]):
                logger.warning(f"Insufficient data for {ticker}, skipping.")
                continue
                
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
                'r_squared':  pp['r_squared'],
                'slope':      pp['slope'],
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
            logger.error(f"Error processing {ticker}: {e}")
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
    logger.info(f"Short scores computed. Stocks with Short_Score > 60: "
                f"{(ranked_df['Short_Score'] > 60).sum() if 'Short_Score' in ranked_df.columns else 'N/A'}")

    # 3c. Add Sector column from basket map
    import sector_baskets as _sb
    _basket_map = _sb.build_ticker_basket_map()
    ranked_df['Sector'] = ranked_df.index.map(lambda t: _basket_map.get(t, 'Other'))
    logger.info(f"Sector column added. {(ranked_df['Sector'] != 'Other').sum()} tickers have a basket sector.")

    # 4. output
    top_10 = ranked_df.head(10)
    
    # Console Output
    table = tabulate(top_10[['Final_Score', '3m_return', 'rs_rating', 'bullish_ratio']], 
                     headers='keys', tablefmt='psql', floatfmt=".2f")
    logger.info(f"\nTop 10 Stocks:\n{table}")
    
    # Save to CSV → Ratatouille/Data/
    import os as _os
    _data_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'Data')
    _os.makedirs(_data_dir, exist_ok=True)
    filename = _os.path.join(_data_dir, f"screen_results_{datetime.now().strftime('%Y%m%d')}.csv")
    ranked_df.to_csv(filename)
    logger.info(f"Full results saved to {filename}")
    
    # 6. Generate Reports
    import report_generator
    import webbrowser
    try:
        import os
        # Save HTML next to Basket.docx in the Ratatouille root folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)

        date_str = session_date.strftime('%Y-%m-%d')
        filename = f"Ratatouille_{date_str}.html"
        reports_dir = os.path.join(project_root, 'Reports')
        os.makedirs(reports_dir, exist_ok=True)
        full_path = os.path.join(reports_dir, filename)

        # ── LONG Analysis (using filtered ranked_df) ──
        import sector_baskets
        logger.info("Analyzing sector baskets (Long)...")
        basket_df = sector_baskets.analyze_baskets(ranked_df)

        import candidate_scanner
        logger.info("Running Big Winner Candidate Scanner...")
        candidates_df = candidate_scanner.scan_candidates(ranked_df, basket_df)
        logger.info(f"Long candidates found: {len(candidates_df) if candidates_df is not None and not candidates_df.empty else 0}")

        # ── SHORT Analysis (using filtered ranked_df) ──
        logger.info("Analyzing sector baskets (Short)...")
        short_basket_df = sector_baskets.analyze_baskets_short(ranked_df)

        logger.info("Running Short Candidate Scanner...")
        short_candidates_df = candidate_scanner.scan_short_candidates(ranked_df, short_basket_df)
        logger.info(f"Short candidates found: {len(short_candidates_df) if short_candidates_df is not None and not short_candidates_df.empty else 0}")

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
        logger.info(f"Dashboard saved to: {report_path}")

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

        # ── Auto git push ─────────────────────────────────────────────────────
        try:
            import subprocess
            repo_root = os.path.dirname(script_dir)
            logger.info("Git: staging and pushing changes...")
            subprocess.run(["git", "add", "Screener/", "Archive/index.html",
                            "Archive/market_score_history.json"],
                           cwd=repo_root, check=True)
            subprocess.run(["git", "commit", "-m",
                            f"Auto update - {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
                           cwd=repo_root, check=True)
            subprocess.run(["git", "push", "origin", "main"],
                           cwd=repo_root, check=True)
            logger.info("Git: push completed.")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Git push failed: {e}")
        except Exception as e:
            logger.warning(f"Git push error: {e}")

    except Exception as e:
        logger.error(f"Error generating outputs: {e}")
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
