"""
One-shot cleanup for treasury_jobs.csv.

Applies the same title-level relevance filter that scraper.py now uses
(TreasuryWebScraper.TREASURY_KEYWORDS) to remove false positives that
were collected before the filter was added — e.g. forestry/vegetation
jobs that matched "ION" as a substring of "Vegetation", "Region", etc.

Behavior:
  * Reads treasury_jobs.csv
  * Writes a timestamped backup alongside it
  * Keeps rows whose `title` matches TREASURY_KEYWORDS
  * Overwrites treasury_jobs.csv with the filtered rows
  * Prints a short summary + the titles it removed (so you can spot-check)

Run:
    python cleanup_jobs_csv.py
    python cleanup_jobs_csv.py --dry-run       # just show what would go
    python cleanup_jobs_csv.py --file other.csv
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from scraper import TreasuryWebScraper


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="treasury_jobs.csv", help="CSV to clean")
    parser.add_argument("--dry-run", action="store_true", help="Show removals without writing")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"❌ {path} not found")
        return 1

    df = pd.read_csv(path)
    if "title" not in df.columns:
        print(f"❌ {path} has no 'title' column")
        return 1

    before = len(df)
    is_relevant = TreasuryWebScraper._is_relevant
    keep_mask = df["title"].fillna("").astype(str).apply(is_relevant)

    kept = df[keep_mask].copy()
    removed = df[~keep_mask].copy()

    print(f"📂 {path}: {before} rows")
    print(f"✅ Keeping:  {len(kept)}")
    print(f"🗑️  Removing: {len(removed)}")

    if len(removed):
        print("\n── removed titles ──")
        # Show grouped counts so duplicate noise is obvious at a glance
        grouped = (
            removed.groupby(["source", "company", "title"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        for _, r in grouped.iterrows():
            tag = f"[{r['source']}] {r['company']}"
            count = f" (x{r['count']})" if r["count"] > 1 else ""
            print(f"  - {tag}: {r['title']}{count}")

    if args.dry_run:
        print("\n(dry-run — no file changes)")
        return 0

    if len(removed) == 0:
        print("\nNothing to clean up.")
        return 0

    backup = path.with_name(f"{path.stem}.backup-{datetime.now():%Y%m%d-%H%M%S}{path.suffix}")
    shutil.copy2(path, backup)
    kept.to_csv(path, index=False)

    print(f"\n💾 Wrote {len(kept)} rows to {path}")
    print(f"🗄️  Backup: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
