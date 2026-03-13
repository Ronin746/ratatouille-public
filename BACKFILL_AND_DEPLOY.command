#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
#  BACKFILL_AND_DEPLOY.command
#  Backfill uno o più basket, aggiorna archivio, deploya su Cloudflare e
#  fa force push su GitHub.
#  Uso: doppio click oppure ./BACKFILL_AND_DEPLOY.command
# ─────────────────────────────────────────────────────────────────────────────

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCREENER_DIR="$ROOT_DIR/Screener"

cd "$SCREENER_DIR" || { echo "❌ Cartella Screener non trovata."; exit 1; }

echo "═══════════════════════════════════════════════════"
echo "   BACKFILL + UPDATE ARCHIVE + DEPLOY + PUSH"
echo "═══════════════════════════════════════════════════"
echo ""
echo "Inserisci i nomi dei basket da backfillare (separati da virgola)."
echo "Esempio: CHEMICALS, CONTRACT DRILLING"
echo "Lascia vuoto per saltare il backfill e fare solo update+deploy+push."
echo ""
echo -n "Basket: "
read BASKET_INPUT

# ── Backfill ──────────────────────────────────────────────────────────────────
if [ -n "$BASKET_INPUT" ]; then
    IFS=',' read -ra BASKETS <<< "$BASKET_INPUT"
    for BASKET in "${BASKETS[@]}"; do
        BASKET=$(echo "$BASKET" | xargs)   # trim spazi
        echo ""
        echo "📥 Backfill: $BASKET ..."
        python3 screener.py --basket "$BASKET" --backfill
        if [ $? -ne 0 ]; then
            echo "⚠️  Backfill fallito per '$BASKET'. Continuo comunque."
        else
            echo "✅ Backfill OK: $BASKET"
        fi
    done
else
    echo "⏩ Backfill saltato."
fi

# ── Update Archive ────────────────────────────────────────────────────────────
echo ""
echo "📊 Aggiorno archivio..."
python3 update_archive.py
if [ $? -ne 0 ]; then
    echo "❌ update_archive.py fallito. Interrompo."
    exit 1
fi
echo "✅ Archivio aggiornato."

# ── Deploy Cloudflare ─────────────────────────────────────────────────────────
echo ""
echo "🚀 Deploy su Cloudflare Pages..."
python3 "$ROOT_DIR/deploy_to_cloudflare.py"
if [ $? -ne 0 ]; then
    echo "⚠️  Deploy Cloudflare fallito. Continuo con il push GitHub."
else
    echo "✅ Deploy Cloudflare OK."
fi

# ── Git force push ────────────────────────────────────────────────────────────
echo ""
echo "📦 Git: staging e force push..."
cd "$ROOT_DIR"
git add -A
git commit -m "Backfill + archive update: $BASKET_INPUT" 2>/dev/null || \
git commit -m "Archive update" 2>/dev/null || \
echo "   (nessuna modifica da committare)"
git push --force origin main
if [ $? -eq 0 ]; then
    echo "✅ Push GitHub OK."
else
    echo "⚠️  Push GitHub fallito."
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "   ✅  TUTTO COMPLETATO"
echo "═══════════════════════════════════════════════════"
