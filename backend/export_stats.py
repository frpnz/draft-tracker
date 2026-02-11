"""Convenience wrapper to export the static site.

Run from repo root:
  python backend/export_stats.py --db data/draft_tracker.sqlite
"""

from pathlib import Path
import sys

# Ensure `backend/` is on sys.path so imports work when executed as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from draft_stats.cli import main

if __name__ == '__main__':
    raise SystemExit(main())
