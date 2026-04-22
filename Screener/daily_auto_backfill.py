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

def step_run_screener():
    logger.info("=" * 55)
    logger.info("STEP 3: Full screener run")
    logger.info("  (salva CSV + HTML con data sessione, elimina file precedenti)")
    logger.info("=" * 55)
    try:
        import scheduler_app
        importlib.reload(scheduler_app)
        scheduler_app.run_screener()
        logger.info("  Screener run complete.")
    except Exception as e:
        logger.error("  Screener run failed: %s", e)
        import traceback
        logger.error(traceback.format_exc())


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
                        help='Skip the 3-month history backfill (useful for quick tests)')
    parser.add_argument('--days', type=int, default=90,
                        help='Trading days to backfill (default 90: 63 display + 27 warmup buffer)')
    args = parser.parse_args()

    start = datetime.now()
    logger.info("")
    logger.info("=" * 55)
    logger.info("  RATATOUILLE - DAILY AUTO-BACKFILL")
    logger.info("  Avviato: %s", start.strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("=" * 55)
    logger.info("")

    # 1. Sync baskets from Basket.docx
    step_sync_baskets()

    # 2. Full screener run - CSV + HTML with session date, old files deleted
    # (must run BEFORE backfill so the new entry exists when ratios are injected)
    step_run_screener()

    # 2b. Archivia il CSV in Data/archive/ per storico 7-factor
    step_archive_csv()

    # 3. Backfill: reads the CSV produced above, injects yfinance macro ratios
    # into ALL history entries including today's new entry, then saves the JSON
    if not args.skip_backfill:
        step_backfill_history(days=args.days)
    else:
        logger.info("[SKIP] Market history backfill skipped (--skip-backfill).")

    # 4. Regenerate Archive/index.html (JSON now has accurate macro ratios)
    step_update_archive()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("")
    logger.info("=" * 55)
    logger.info("  COMPLETATO - durata %.1f min", elapsed / 60)
    logger.info("=" * 55)


if __name__ == '__main__':
    main()
