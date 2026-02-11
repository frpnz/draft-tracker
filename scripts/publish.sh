#!/usr/bin/env bash
set -euo pipefail
DB_PATH="${1:-data/draft_tracker.sqlite}"
python backend/export_stats.py --db "$DB_PATH"
git add docs/data/stats.v1.json docs/data/stats.v1.schema.json
git status
