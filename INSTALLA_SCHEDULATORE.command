#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  RATATOUILLE — Installa schedulatore automatico (22:02 ogni giorno)
# ═══════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
START_CMD="$SCRIPT_DIR/START_SCREENER.command"
PLIST_LABEL="com.ratatouille.screener"
PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_LABEL.plist"
LOG_DIR="$SCRIPT_DIR/Screener/logs"

echo ""
echo "🍽️  Ratatouille — Schedulatore Automatico"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Controlla che START_SCREENER.command esista
if [ ! -f "$START_CMD" ]; then
    echo "❌  Errore: START_SCREENER.command non trovato in:"
    echo "    $SCRIPT_DIR"
    read -p "Premi Invio per chiudere..."
    exit 1
fi

# Rendi eseguibile lo script principale
chmod +x "$START_CMD"

# Crea cartella log
mkdir -p "$LOG_DIR"

# Rimuovi vecchio plist se esiste
if [ -f "$PLIST_PATH" ]; then
    launchctl unload "$PLIST_PATH" 2>/dev/null
    echo "⟳  Aggiornamento schedulatore esistente..."
fi

# Scrivi il file plist
cat > "$PLIST_PATH" << PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ratatouille.screener</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${START_CMD}</string>
    </array>

    <!-- Ogni giorno alle 22:02 ora locale (Italia) -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>22</integer>
        <key>Minute</key>
        <integer>2</integer>
    </dict>

    <!-- Log output -->
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/ratatouille_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/ratatouille_stderr.log</string>

    <!-- Variabili d'ambiente necessarie -->
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin</string>
        <key>HOME</key>
        <string>${HOME}</string>
        <key>LANG</key>
        <string>it_IT.UTF-8</string>
    </dict>

    <!-- Riprova se il Mac era spento all'orario previsto -->
    <key>RunAtLoad</key>
    <false/>

</dict>
</plist>
PLIST_EOF

# Carica il plist
launchctl load "$PLIST_PATH"

if [ $? -eq 0 ]; then
    echo "✅  Schedulatore installato con successo!"
    echo ""
    echo "   📅  Lo screener girerà ogni giorno alle 22:02 (ora italiana)"
    echo "   📂  Log salvati in: Screener/logs/"
    echo "   🌐  Il sito si aggiornerà automaticamente dopo ogni run"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "📋  Comandi utili (da eseguire nel Terminale):"
    echo ""
    echo "   Verifica stato:"
    echo "   launchctl list | grep ratatouille"
    echo ""
    echo "   Disattiva schedulatore:"
    echo "   launchctl unload ~/Library/LaunchAgents/com.ratatouille.screener.plist"
    echo ""
    echo "   Riattiva schedulatore:"
    echo "   launchctl load ~/Library/LaunchAgents/com.ratatouille.screener.plist"
    echo ""
else
    echo "❌  Errore durante l'installazione."
    echo "    Prova a rieseguire questo script."
fi

echo ""
read -p "Premi Invio per chiudere..."
