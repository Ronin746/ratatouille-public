#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  RATATOUILLE — Aggiorna Archivio
#  Double-click to run.
#  Rigenera Archive/index.html dai CSV già presenti e lo deploya
#  su Cloudflare Pages. Non riesegue lo screener.
# ─────────────────────────────────────────────────────────────────

SITE_URL="https://ratatouille-screener.pages.dev"

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR/Screener"

echo ""
echo "═══════════════════════════════════════════════════"
echo "   RATATOUILLE — Aggiorna Archivio"
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

# ── 2. Aggiorna archivio ───────────────────────────────
echo "Rigenerando Archive/index.html dai CSV esistenti..."
echo "───────────────────────────────────────────────────"
python3 update_archive.py
ARCHIVE_EXIT=$?
echo "───────────────────────────────────────────────────"

if [ $ARCHIVE_EXIT -ne 0 ]; then
    echo ""
    echo "⚠️  update_archive.py uscito con errori (codice $ARCHIVE_EXIT)."
    echo "   Controlla l'output sopra."
    echo ""
    echo "Press ENTER to close."
    read
    exit 1
fi

echo ""

# ── 3. Deploy a Cloudflare Pages ──────────────────────
echo "Deploying archivio su Cloudflare Pages..."
CF_OUTPUT=$(python3 "$ROOT_DIR/deploy_to_cloudflare.py" 2>&1)
DEPLOY_EXIT=$?
echo "$CF_OUTPUT"

if [ $DEPLOY_EXIT -eq 0 ]; then
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ✅  Archivio aggiornato e deployato!"
    echo "  🌐  $SITE_URL"
    echo "  📁  Locale: $ROOT_DIR/Archive/index.html"
    echo "═══════════════════════════════════════════════════"
    open "$SITE_URL"
else
    echo "⚠️  Deploy fallito. Apro archivio locale..."
    open "$ROOT_DIR/Archive/index.html"
fi

echo ""
echo "Press ENTER to close."
read
