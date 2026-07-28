#!/usr/bin/env python3
"""
daily_auto_backfill.py - Ratatouille Daily Auto-Backfill Orchestrator
======================================================================
Eseguito ogni giorno (via DAILY_BACKFILL.bat o Task Scheduler Windows) per:

  1. Sincronizzare sector_baskets.py con Basket.docx
  2. Eseguire backfill_market_history.py --days 90
     -> scarica 6 mesi di prezzi, ricalcola breadth/spread/sector scores
        per TUTTI i basket aggiornati, aggiorna market_score_history.json
  3. Lanciare lo screener completo (scheduler_app.run_screener())
     -> salva CSV + HTML con data sessione corrente
     -> elimina automaticamente i file della sessione precedente
  4. Aggiornare Archive/index.html

Opzioni:
  --skip-backfill   Salta il backfill storico (solo sync + screener + archivio)
  --days N          Numero di giorni da backfillare (default: 90 = 63 display + buffer)
"""

import argparse
import importlib
import logging
import os
import subprocess
import sys
from datetime import datetime

# Fix encoding on Windows consoles (cp1252 cannot print Unicode box chars)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

HERE = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            os.path.join(HERE, 'daily_backfill.log'),
            encoding='utf-8'
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Step 1 - Sync baskets from Basket.docx
# -----------------------------------------------------------------------------

def step_sync_baskets():
    logger.info("=" * 55)
    logger.info("STEP 1: Sync baskets from Basket.docx")
    logger.info("=" * 55)
    try:
        if HERE not in sys.path:
            sys.path.insert(0, HERE)
        import sync_baskets
        added = sync_baskets.sync_baskets()
        if added > 0:
            logger.info("  %d new basket(s) added to sector_baskets.py", added)
        else:
            logger.info("  sector_baskets.py already up to date.")
        return added
    except Exception as e:
        logger.warning("  Could not sync baskets: %s", e)
        return 0


# -----------------------------------------------------------------------------
# Step 2 - Backfill 3 months of market history (breadth + sector scores)
# -----------------------------------------------------------------------------

def step_backfill_history(days=63):
    logger.info("=" * 55)
    logger.info("STEP 2: Backfill %d trading days (~3 months) of market history", days)
    logger.info("  (breadth, L-S spread, sector scores per ogni basket)")
    logger.info("=" * 55)

    backfill_script = os.path.join(HERE, 'backfill_market_history.py')
    if not os.path.exists(backfill_script):
        logger.error("  backfill_market_history.py not found at %s", backfill_script)
        return False

    cmd = [sys.executable, backfill_script, '--days', str(days)]
    result = subprocess.run(cmd, cwd=HERE)
    if result.returncode != 0:
        logger.warning("  backfill_market_history.py exited with errors.")
        return False

    logger.info("  Market history backfill complete.")
    return True


# -----------------------------------------------------------------------------
# Step 3 - Full screener run (produces CSV + HTML, auto-cleans old files)
# -----------------------------------------------------------------------------

def step_run_screener(region="all", merge=False):
    logger.info("=" * 55)
    logger.info("STEP 3: Full screener run (region=%s, merge=%s)", region, merge)
    logger.info("=" * 55)
    try:
        import scheduler_app
        import importlib
        importlib.reload(scheduler_app)

        if region == "eu":
            # ── EU-only phase: score EU tickers, save intermediate, exit ──
            from ticker_universe import get_eu_tickers
            eu_tickers = get_eu_tickers()
            logger.info("EU ticker universe: %d", len(eu_tickers))
            scheduler_app.run_screener(tickers=eu_tickers)
            # After run_screener, load the generated CSV to get the scored df
            import glob, os, pandas as pd
            data_dir = os.path.join(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__))), 'Data')
            csvs = sorted(glob.glob(os.path.join(data_dir, 'screen_results_*.csv')))
            if csvs:
                eu_df = pd.read_csv(csvs[-1], index_col=0)
                scheduler_app.save_intermediate_scores(eu_df, "eu")
                logger.info("EU intermediate saved: %d rows", len(eu_df))
            return

        if region == "us_ca":
            # ── US+CA phase: score US+CA tickers ──
            from ticker_universe import get_us_ca_tickers
            us_ca_tickers = get_us_ca_tickers()
            logger.info("US+CA ticker universe: %d", len(us_ca_tickers))
            scheduler_app.run_screener(tickers=us_ca_tickers)
        else:
            # ── Default: US-only (backward compatible) ──
            scheduler_app.run_screener()

        if merge:
            # ── Merge EU intermediate into the current CSV + regenerate report ──
            _do_merge()

        logger.info("  Screener run complete.")
    except Exception as e:
        logger.error("  Screener run failed: %s", e)
        import traceback
        logger.error(traceback.format_exc())


def _do_merge():
    """Load EU intermediate scores, merge with current US/CA CSV, deduplicate, re-save."""
    import glob, pandas as pd, os
    import scheduler_app

    eu_df = scheduler_app.load_intermediate_scores("eu")
    if eu_df is None or eu_df.empty:
        logger.warning("No EU intermediate data to merge. Skipping merge.")
        return

    data_dir = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), 'Data')
    csvs = sorted(glob.glob(os.path.join(data_dir, 'screen_results_*.csv')))
    if not csvs:
        logger.warning("No US/CA CSV found to merge into.")
        return

    us_df = pd.read_csv(csvs[-1], index_col=0)
    logger.info("Merge: US/CA=%d rows, EU=%d rows", len(us_df), len(eu_df))

    # Align columns — EU may have fewer columns
    common_cols = us_df.columns.intersection(eu_df.columns)
    merged = pd.concat([us_df[common_cols], eu_df[common_cols]], axis=0)

    # Deduplicate cross-listings
    merged = scheduler_app.deduplicate_by_volume(merged)
    logger.info("After merge + dedup: %d rows", len(merged))

    # Overwrite the CSV
    merged.to_csv(csvs[-1])
    logger.info("Merged CSV saved: %s", csvs[-1])

    # Regenerate the HTML report with merged data
    _regenerate_report(merged, csvs[-1])


def _regenerate_report(merged_df, csv_path):
    """Re-generate the HTML report from the merged DataFrame."""
    import os, re, glob
    import report_generator
    import sector_baskets
    import candidate_scanner
    from datetime import datetime
    import pytz

    # Determine session date from CSV filename
    fname = os.path.basename(csv_path)
    m = re.search(r'(\d{8})', fname)
    if m:
        session_date = datetime.strptime(m.group(1), '%Y%m%d')
    else:
        et_tz = pytz.timezone("America/New_York")
        session_date = datetime.now(et_tz)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    reports_dir = os.path.join(project_root, 'Reports')
    os.makedirs(reports_dir, exist_ok=True)
    date_str = session_date.strftime('%Y-%m-%d')
    full_path = os.path.join(reports_dir, f"BlackRat_{date_str}.html")

    logger.info("Regenerating HTML report with %d merged tickers...", len(merged_df))

    basket_df = sector_baskets.analyze_baskets(merged_df)
    candidates_df = candidate_scanner.scan_candidates(merged_df, basket_df)
    short_basket_df = sector_baskets.analyze_baskets_short(merged_df)
    short_candidates_df = candidate_scanner.scan_short_candidates(merged_df, short_basket_df)

    report_generator.generate_html_report(
        merged_df,
        filename=full_path,
        basket_df=basket_df,
        candidates_df=candidates_df,
        short_basket_df=short_basket_df,
        short_candidates_df=short_candidates_df,
        session_date=session_date,
    )
    logger.info("Merged report saved: %s", full_path)


# -----------------------------------------------------------------------------
# Step 3b - Archivia il CSV della sessione in Data/archive/
# -----------------------------------------------------------------------------

def step_archive_csv(keep=90):
    """
    Copia il CSV della sessione in Data/archive/ e rimuove i CSV più vecchi
    che uscirebbero dalla finestra del backfill (default: ultimi 90 giorni).

    In questo modo l'archivio mantiene sempre esattamente i CSV necessari
    per il calcolo del 7-factor reale nella chart, senza crescere all'infinito.
    """
    import glob
    import shutil

    data_dir    = os.path.join(os.path.dirname(HERE), 'Data')
    archive_dir = os.path.join(data_dir, 'archive')
    os.makedirs(archive_dir, exist_ok=True)

    # Copia il CSV corrente
    csvs = sorted(glob.glob(os.path.join(data_dir, 'screen_results_*.csv')))
    if not csvs:
        logger.warning("  Nessun CSV trovato in Data/ da archiviare.")
        return

    src   = csvs[-1]
    fname = os.path.basename(src)
    dst   = os.path.join(archive_dir, fname)

    if not os.path.exists(dst):
        shutil.copy2(src, dst)
        logger.info("  CSV archiviato: %s", fname)
    else:
        logger.info("  CSV già in archivio: %s", fname)

    # Rimuovi i CSV più vecchi che escono dalla finestra
    archived = sorted(glob.glob(os.path.join(archive_dir, 'screen_results_*.csv')))
    excess   = len(archived) - keep
    if excess > 0:
        for old in archived[:excess]:
            os.remove(old)
            logger.info("  CSV rimosso (fuori finestra): %s", os.path.basename(old))
        logger.info("  Archivio: %d CSV mantenuti (finestra %d giorni)", keep, keep)


# -----------------------------------------------------------------------------
# Step 4 - Update Archive/index.html
# -----------------------------------------------------------------------------

def step_update_archive():
    logger.info("=" * 55)
    logger.info("STEP 4: Update Archive/index.html")
    logger.info("=" * 55)
    update_script = os.path.join(HERE, 'update_archive.py')
    if not os.path.exists(update_script):
        logger.warning("  update_archive.py not found - skipping.")
        return
    result = subprocess.run([sys.executable, update_script], cwd=HERE)
    if result.returncode != 0:
        logger.warning("  update_archive.py exited with errors.")
    else:
        logger.info("  Archive updated.")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Ratatouille daily auto-backfill')
    parser.add_argument('--skip-backfill', action='store_true',
                        help='Skip the 3-month history backfill')
    parser.add_argument('--days', type=int, default=90,
                        help='Trading days to backfill (default 90)')
    parser.add_argument('--region', type=str, default='all',
                        choices=['all', 'eu', 'us_ca'],
                        help='Market region to process (eu=EU only, us_ca=US+CA, all=US only legacy)')
    parser.add_argument('--merge', action='store_true',
                        help='Merge EU intermediate scores into the final report')
    args = parser.parse_args()

    start = datetime.now()
    logger.info("")
    logger.info("=" * 55)
    logger.info("  RATATOUILLE - DAILY AUTO-BACKFILL")
    logger.info("  Region: %s | Merge: %s", args.region, args.merge)
    logger.info("  Avviato: %s", start.strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("=" * 55)
    logger.info("")

    if args.region == "eu":
        # EU-only: skip baskets, run screener, save intermediate, done
        step_run_screener(region="eu")
    else:
        # 1. Sync baskets from Basket.docx
        step_sync_baskets()

        # 2. Full screener run
        step_run_screener(region=args.region, merge=args.merge)

        # 2b. Archive CSV
        step_archive_csv()

        # 3. Backfill
        if not args.skip_backfill:
            step_backfill_history(days=args.days)
        else:
            logger.info("[SKIP] Market history backfill skipped.")

        # 4. Regenerate Archive/index.html
        step_update_archive()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("")
    logger.info("=" * 55)
    logger.info("  COMPLETATO - durata %.1f min", elapsed / 60)
    logger.info("=" * 55)


if __name__ == '__main__':
    main()
