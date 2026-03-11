#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  RATATOUILLE SCREENER
#  Double-click to run. Auto-deploys to Cloudflare Pages when done.
#  Site: https://ratatouille-screener.pages.dev
# ─────────────────────────────────────────────────────────────────

SITE_URL="https://ratatouille-screener.pages.dev"

# Vai nella cartella Screener (dove sta il codice Python)
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR/Screener"

echo ""
echo "═══════════════════════════════════════════════════"
echo "   RATATOUILLE — Stock Screener"
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

# ── 2. Sync baskets from Basket.docx ─────────────────
echo "Checking Basket.docx for new baskets..."
python3 sync_baskets.py
echo ""

# ── 3. Run the screener ───────────────────────────────
echo "Starting scan (~20-30 min for 7000 tickers)."
echo "Do not close this window."
echo "───────────────────────────────────────────────────"
python3 scheduler_app.py --now
SCREENER_EXIT=$?

echo "───────────────────────────────────────────────────"

if [ $SCREENER_EXIT -ne 0 ]; then
    echo "⚠️  Screener exited with errors (code $SCREENER_EXIT)."
    echo "   Check screener.log for details."
    echo ""
fi

# ── 4. Build / update the archive page ───────────────
echo ""
echo "Building archive page..."
python3 update_archive.py
ARCHIVE_EXIT=$?

if [ $ARCHIVE_EXIT -ne 0 ]; then
    echo "⚠️  Archive update failed. Skipping deploy."
else
    echo ""
    # ── 5. Deploy to Cloudflare Pages ─────────────────
    echo "Deploying to Cloudflare Pages..."
    CF_OUTPUT=$(python3 "$ROOT_DIR/deploy_to_cloudflare.py" 2>&1)
    DEPLOY_EXIT=$?
    echo "$CF_OUTPUT"

    if [ $DEPLOY_EXIT -eq 0 ]; then
        echo ""
        echo "═══════════════════════════════════════════════════"
        echo "  ✅  Deployed!"
        echo "  🌐  $SITE_URL"
        echo "  📁  Archive locale: $ROOT_DIR/Archive/index.html"
        echo "═══════════════════════════════════════════════════"
        open "$SITE_URL"
    else
        echo "⚠️  Deploy failed (code $DEPLOY_EXIT)."
        echo "    Apro archivio locale..."
        open "$ROOT_DIR/Archive/index.html"
    fi
fi

echo ""
echo "Press ENTER to close."
read
