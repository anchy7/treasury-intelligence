"""
One-shot cleanup for treasury_jobs.csv.

Removes false positives using the same relevance filter as scraper.py —
e.g. forestry/vegetation jobs that matched "ION" as a substring of
"Vegetation", "Region", etc.

Behaviour:
  * Reads treasury_jobs.csv (or --file <path>)
  * Writes a timestamped backup alongside it
  * Keeps rows whose `title` matches the treasury keyword pattern
  * Overwrites the file with the filtered rows
  * Prints a short summary + removed titles so you can spot-check

Usage:
    python cleanup_jobs_csv.py
    python cleanup_jobs_csv.py --dry-run        # show what would go, no writes
    python cleanup_jobs_csv.py --file other.csv
"""
from __future__ import annotations

import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Relevance filter — kept in sync with scraper.py by hand (no import needed)
# ---------------------------------------------------------------------------
TREASURY_RE = re.compile(
    r"treasury|treasurer|treasuri|"
    r"cash\s*manage(?:r|ment)?|"
    r"liquidit|"
    r"kyriba|nomentia|coupa|"
    r"zahlungsverkehr|finanz(?:ierung|manage)|"
    r"\btms\b|\bfx\b|"
    r"hedging|hedge\s*account|"
    r"\bion\s+(?:treasury|trading)\b|"
    r"sap\s+(?:treasury|tr)|fis\s+treasury",
    re.IGNORECASE,
)


def is_relevant(title: str) -> bool:
    return bool(TREASURY_RE.search(title or ""))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="treasury_jobs.csv", help="CSV to clean")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be removed without writing any files",
    )
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"❌ File not found: {path}")
        return 1

    df = pd.read_csv(path)

    if "title" not in df.columns:
        print(f"❌ {path} has no 'title' column")
        return 1

    before = len(df)
    keep_mask = df["title"].fillna("").astype(str).apply(is_relevant)
    kept    = df[keep_mask].copy()
    removed = df[~keep_mask].copy()

    print(f"\n📂 File   : {path}")
    print(f"📊 Total  : {before} rows")
    print(f"✅ Keeping: {len(kept)}")
    print(f"🗑️  Removed: {len(removed)}")

    if not removed.empty:
        print("\n── removed titles ──────────────────────────────────────")
        grouped = (
            removed
            .groupby(["source", "company", "title"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        for _, r in grouped.iterrows():
            tag   = f"[{r['source']}] {r['company']}"
            count = f" (x{r['count']})" if r["count"] > 1 else ""
            print(f"  - {tag}: {r['title']}{count}")
        print("─────────────────────────────────────────────────────────")

    if args.dry_run:
        print("\n(dry-run — no files changed)")
        return 0

    if removed.empty:
        print("\n✅ Nothing to clean up — file unchanged.")
        return 0

    # Backup before overwriting
    ts     = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = path.with_name(f"{path.stem}.backup-{ts}{path.suffix}")
    shutil.copy2(path, backup)

    kept.to_csv(path, index=False)

    print(f"\n💾 Wrote {len(kept)} rows → {path}")
    print(f"🗄️  Backup → {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
