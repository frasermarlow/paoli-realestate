#!/bin/bash
# Weekly local Redfin estimate update
# Runs ci_update_redfin.py, commits, and pushes if data changed.

set -e

cd "$(dirname "$0")"

LOG="data/redfin_update.log"
echo "=== $(date) ===" >> "$LOG"

python3 ci_update_redfin.py 2>&1 | tee -a "$LOG"

if git diff --quiet docs/data.json properties.csv; then
    echo "No changes to data.json or properties.csv" | tee -a "$LOG"
    exit 0
fi

git add docs/data.json properties.csv
git commit -m "Update Redfin estimates $(date -u +%Y-%m-%d)"
git push

echo "Committed and pushed." | tee -a "$LOG"
