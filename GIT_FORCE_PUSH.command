#!/bin/bash
cd "$(dirname "$0")"
echo "📦 Staging all changes..."
git add -A
echo ""
echo "✏️  Commit message (lascia vuoto per 'Update'):"
read MSG
MSG=${MSG:-"Update"}
git commit -m "$MSG"
echo ""
echo "🚀 Force push a origin/main..."
git push --force origin main
echo ""
echo "✅ Done."
