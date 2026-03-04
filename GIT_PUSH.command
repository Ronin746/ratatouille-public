#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  RATATOUILLE — Git Push su GitHub
#  Double-click to run.
#  Fa git add di tutto, commit con data odierna e push.
# ─────────────────────────────────────────────────────────────────

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

echo ""
echo "═══════════════════════════════════════════════════"
echo "   RATATOUILLE — Push su GitHub"
echo "═══════════════════════════════════════════════════"
echo ""

# ── Controlla che siamo in un repo git ────────────────
if [ ! -d ".git" ]; then
    echo "⚠️  Questa cartella non è un repository Git."
    echo "   Esegui prima: git init && git remote add origin <URL>"
    echo ""
    echo "Press ENTER to close."
    read
    exit 1
fi

# ── Stato attuale ──────────────────────────────────────
echo "File modificati:"
git status --short
echo ""

# ── Aggiunge tutto ────────────────────────────────────
git add -A

# ── Controlla se ci sono modifiche da committare ──────
if git diff --cached --quiet; then
    echo "ℹ️  Nessuna modifica da committare. Il repo è già aggiornato."
    echo ""
    echo "Press ENTER to close."
    read
    exit 0
fi

# ── Commit con data e ora ─────────────────────────────
COMMIT_MSG="update: screener run $(date '+%Y-%m-%d %H:%M')"
echo "Commit: \"$COMMIT_MSG\""
git commit -m "$COMMIT_MSG"
COMMIT_EXIT=$?

if [ $COMMIT_EXIT -ne 0 ]; then
    echo "⚠️  Commit fallito."
    echo ""
    echo "Press ENTER to close."
    read
    exit 1
fi

# ── Push ──────────────────────────────────────────────
echo ""
echo "Push verso GitHub..."
echo "───────────────────────────────────────────────────"
git push
PUSH_EXIT=$?
echo "───────────────────────────────────────────────────"

if [ $PUSH_EXIT -eq 0 ]; then
    REMOTE_URL=$(git remote get-url origin 2>/dev/null || echo "GitHub")
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ✅  Push completato!"
    echo "  📦  $REMOTE_URL"
    echo "═══════════════════════════════════════════════════"
else
    echo ""
    echo "⚠️  Push fallito (codice $PUSH_EXIT)."
    echo "   Possibili cause:"
    echo "   - Nessuna connessione internet"
    echo "   - Remote non configurato (git remote add origin <URL>)"
    echo "   - Conflitti: fai prima 'git pull'"
fi

echo ""
echo "Press ENTER to close."
read
