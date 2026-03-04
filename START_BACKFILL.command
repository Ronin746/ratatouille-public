#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  RATATOUILLE — Market History Backfill
#  Double-click to run.
#  Scarica 90 giorni di prezzi e ricostruisce la storia del mercato.
#  Da eseguire una volta sola (o dopo un lungo periodo di inattività).
# ─────────────────────────────────────────────────────────────────

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR/Screener"

echo ""
echo "═══════════════════════════════════════════════════"
echo "   RATATOUILLE — Market Backfill (90 giorni)"
echo "═══════════════════════════════════════════════════"
echo ""

# ── 1. Python environment ──────────────────────────────
if [ ! -d "venv" ]; then
    echo "First run: installing dependencies (1-2 min)..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt --quiet
else
    source venv/bin/activate
fi
echo "✅  Python environment ready"
echo ""

# ── 2. Backfill storico ────────────────────────────────
echo "Scaricando prezzi storici via yfinance (~3-5 min)..."
echo "   Tickers: tutti quelli dell'ultimo CSV"
echo "   Periodo:  6 mesi di prezzi → ultimi 90 giorni calcolati"
echo "───────────────────────────────────────────────────"
python3 backfill_market_history.py --days 90
BACKFILL_EXIT=$?
echo "───────────────────────────────────────────────────"

if [ $BACKFILL_EXIT -ne 0 ]; then
    echo ""
    echo "⚠️  Backfill uscito con errori (codice $BACKFILL_EXIT)."
    echo "   Controlla l'output sopra."
    echo ""
    echo "Press ENTER to close."
    read
    exit 1
fi

# ── 3. Rigenera Archive/index.html ────────────────────
echo ""
echo "Aggiornando archivio con i nuovi dati storici..."
python3 update_archive.py
ARCHIVE_EXIT=$?

if [ $ARCHIVE_EXIT -ne 0 ]; then
    echo "⚠️  Aggiornamento archivio fallito."
    echo "Press ENTER to close."
    read
    exit 1
fi

# ── 4. Deploy a Cloudflare Pages ──────────────────────
echo ""
echo "Deploying archivio aggiornato su Cloudflare Pages..."
CF_OUTPUT=$(python3 "$ROOT_DIR/deploy_to_cloudflare.py" 2>&1)
DEPLOY_EXIT=$?
echo "$CF_OUTPUT"

SITE_URL="https://ratatouille-screener.pages.dev"

if [ $DEPLOY_EXIT -eq 0 ]; then
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ✅  Backfill completato e deployato!"
    echo "  📊  90 giorni di storia caricati"
    echo "  🌐  $SITE_URL"
    echo "═══════════════════════════════════════════════════"
    open "$SITE_URL"
else
    echo "⚠️  Deploy fallito. Apro archivio locale..."
    open "$ROOT_DIR/Archive/index.html"
fi

echo ""
echo "Press ENTER to close."
read
