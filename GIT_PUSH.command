#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  RATATOUILLE — Git Push su GitHub
#  Double-click to run.
#  Fa git add di tutto, commit con data odierna e push.
# ─────────────────────────────────────────────────────────────────

REPO_URL="https://github.com/Ronin746/ratatouille.git"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

echo ""
echo "═══════════════════════════════════════════════════"
echo "   RATATOUILLE — Push su GitHub"
echo "═══════════════════════════════════════════════════"
echo ""

# ── Inizializza repo se necessario ────────────────────
if [ ! -d ".git" ]; then
    echo "Inizializzazione repo Git..."
    git init
    git branch -M main
fi

# ── Configura remote origin ───────────────────────────
CURRENT_REMOTE=$(git remote get-url origin 2>/dev/null)
if [ -z "$CURRENT_REMOTE" ]; then
    echo "Configurazione remote origin..."
    git remote add origin "$REPO_URL"
elif [ "$CURRENT_REMOTE" != "$REPO_URL" ]; then
    echo "Aggiornamento remote origin..."
    git remote set-url origin "$REPO_URL"
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
git push -u origin main
PUSH_EXIT=$?
echo "───────────────────────────────────────────────────"

if [ $PUSH_EXIT -eq 0 ]; then
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ✅  Push completato!"
    echo "  📦  $REPO_URL"
    echo "═══════════════════════════════════════════════════"
else
    echo ""
    echo "⚠️  Push fallito (codice $PUSH_EXIT)."
    echo "   Possibili cause:"
    echo "   - Nessuna connessione internet"
    echo "   - Conflitti: provo force push..."
    echo ""
    read -p "   Vuoi forzare il push? (s/n) " FORCE
    if [ "$FORCE" = "s" ] || [ "$FORCE" = "S" ]; then
        git push --force origin main
        if [ $? -eq 0 ]; then
            echo ""
            echo "  ✅  Force push completato!"
            echo "  📦  $REPO_URL"
        else
            echo "  ⚠️  Anche il force push è fallito."
        fi
    fi
fi

echo ""
echo "Press ENTER to close."
read
