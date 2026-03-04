#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  DEPLOY A CLOUDFLARE PAGES
#  Fai doppio clic per caricare il sito su ratatouille-screener.pages.dev
# ─────────────────────────────────────────────────────────────────

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "═══════════════════════════════════════════════════"
echo "   DEPLOY → ratatouille-screener.pages.dev"
echo "═══════════════════════════════════════════════════"
echo ""

python3 "$ROOT_DIR/deploy_to_cloudflare.py"
DEPLOY_EXIT=$?

echo ""
if [ $DEPLOY_EXIT -eq 0 ]; then
    echo "═══════════════════════════════════════════════════"
    echo "  ✅  Sito aggiornato!"
    echo "  🌐  https://ratatouille-screener.pages.dev"
    echo "═══════════════════════════════════════════════════"
    sleep 2
    open "https://ratatouille-screener.pages.dev"
else
    echo "⚠️  Deploy fallito (codice $DEPLOY_EXIT)."
fi

echo ""
echo "Premi INVIO per chiudere."
read
