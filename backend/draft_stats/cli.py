from __future__ import annotations
import argparse, json, shutil
from pathlib import Path
from .db import connect
from .compute import compute_stats
from .checks import validate_db

def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Export Draft Tracker stats + static site")
    p.add_argument("--db", required=True)
    p.add_argument("--frontend", default=str(Path(__file__).resolve().parents[2] / "frontend" / "site"))
    p.add_argument("--docs", default=str(Path(__file__).resolve().parents[2] / "docs"))
    p.add_argument(
        "--check",
        action="store_true",
        help="Run logical DB consistency checks before exporting (fails with non-zero exit if issues are found).",
    )
    args = p.parse_args(argv)

    db_path = Path(args.db)
    frontend = Path(args.frontend)
    docs = Path(args.docs)
    data_dir = docs / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if docs.exists():
        for child in docs.iterdir():
            if child.name == "data":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    shutil.copytree(frontend, docs, dirs_exist_ok=True)

    with connect(db_path) as conn:
        if args.check:
            issues = validate_db(conn)
            if issues:
                for msg in issues:
                    print(f"[check] {msg}")
                return 2
        stats = compute_stats(conn)

    stats_json = json.dumps(stats, indent=2, sort_keys=True)
    (data_dir / "stats.v1.json").write_text(stats_json, encoding="utf-8")
    # Also write a JS wrapper so the dashboard works when opened via file://
    (data_dir / "stats.v1.js").write_text(
        "window.__DRAFT_STATS__ = " + stats_json + ";\n",
        encoding="utf-8",
    )
    schema_src = Path(__file__).resolve().parents[1] / "stats.v1.schema.json"
    (data_dir / "stats.v1.schema.json").write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
