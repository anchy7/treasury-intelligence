"""
export_tab1_to_csv.py
---------------------
Headless / CLI version of the "📋 All Jobs Table" (tab1) from sales_dashboard.py.

Produces a CSV with the exact same columns that tab1 displays in the Streamlit
dashboard, plus an extra `Job URL` column sourced from the scraper's `url`
field. All of tab1's sidebar filters (date range, source, country, company
search, technology search) and the sort selector are exposed as CLI flags.

Usage:
    python export_tab1_to_csv.py
    python export_tab1_to_csv.py --days 30
    python export_tab1_to_csv.py --source StepStone.de --country Germany
    python export_tab1_to_csv.py --company Kyriba --tech SAP --sort company-asc
    python export_tab1_to_csv.py --input treasury_jobs.csv --out my_export.csv
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_INPUT = "treasury_jobs.csv"
# One-shot runs get a dated filename; the daily workflow passes
# --append with its own stable filename (e.g. treasury_jobs_export.csv).
DEFAULT_OUTPUT = f"tab1_export_{datetime.now():%Y%m%d}.csv"
APPEND_DEFAULT_OUTPUT = "treasury_jobs_export.csv"

# Column mapping: internal CSV name -> display name used in tab1.
# Order here is the order in the output file; `Job URL` is the new column.
TAB1_COLUMNS: list[tuple[str, str]] = [
    ("company",       "Company"),
    ("title",         "Job Title"),
    ("location",      "Location"),
    ("Country",       "Country"),
    ("source",        "Job Source"),
    ("date_scraped",  "Posted Date"),
    ("technologies",  "Technologies"),
    ("url",           "Job URL"),       # <-- new column
]


def _country_from_source_vectorized(df: pd.DataFrame) -> np.ndarray:
    """
    Vectorized country detection — copied verbatim from sales_dashboard.py so
    this script has no Streamlit dependency and produces identical `Country`
    values to the dashboard.
    """
    src = df["source"].fillna("").astype(str).str.strip().str.lower()
    from_source = np.where(
        src.str.contains("jobs.ch|jobs_ch", regex=True, na=False),
        "Switzerland",
        np.where(src.str.contains("stepstone", na=False), "Germany", None),
    )
    loc = df["location"].fillna("").astype(str).str.strip()
    de = loc.str.contains(
        "Munich|Frankfurt|Berlin|Hamburg|Stuttgart|Düsseldorf|Cologne|München|Germany|Deutschland",
        case=False, regex=True, na=False,
    )
    ch = loc.str.contains(
        "Zurich|Basel|Geneva|Zug|Lausanne|Switzerland|Schweiz",
        case=False, regex=True, na=False,
    )
    at = loc.str.contains(
        "Vienna|Wien|Austria|Österreich",
        case=False, regex=True, na=False,
    )
    from_loc = np.select([de, ch, at], ["Germany", "Switzerland", "Austria"], default="Unknown")
    mask = (from_source == "Switzerland") | (from_source == "Germany")
    return np.where(mask, from_source, from_loc)


def load_jobs(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date_scraped"] = pd.to_datetime(df["date_scraped"], errors="coerce")
    df["Country"] = _country_from_source_vectorized(df)
    # Ensure optional columns exist so downstream selection never KeyErrors
    for col in ("url", "technologies"):
        if col not in df.columns:
            df[col] = ""
    return df


def apply_filters(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    """Mirror tab1 sidebar filters."""
    out = df
    if args.days:
        cutoff = datetime.now() - timedelta(days=args.days)
        out = out[out["date_scraped"] >= cutoff]
    if args.source:
        out = out[out["source"] == args.source]
    if args.country:
        out = out[out["Country"] == args.country]
    if args.company:
        out = out[out["company"].astype(str).str.contains(args.company, case=False, na=False)]
    if args.tech:
        out = out[out["technologies"].astype(str).str.contains(args.tech, case=False, na=False)]
    return out


def apply_sort(df: pd.DataFrame, sort: str) -> pd.DataFrame:
    """Mirror tab1 sort selector."""
    spec = {
        "newest":       ("date_scraped", False),
        "oldest":       ("date_scraped", True),
        "company-asc":  ("company", True),
        "company-desc": ("company", False),
    }[sort]
    return df.sort_values(spec[0], ascending=spec[1])


def build_tab1_table(df: pd.DataFrame) -> pd.DataFrame:
    """Project down to tab1's columns (+ Job URL) with display-friendly names."""
    internal, display = zip(*TAB1_COLUMNS)
    out = df[list(internal)].copy()
    out["date_scraped"] = out["date_scraped"].dt.strftime("%Y-%m-%d")
    out.columns = list(display)
    return out


def merge_with_existing(new_df: pd.DataFrame, out_path: Path) -> pd.DataFrame:
    """
    Append-mode merge. Preserves every row already in `out_path` (keeps their
    original order) and adds only rows from `new_df` whose `Job URL` is not
    yet present. New rows are appended at the bottom.

    If the output file doesn't exist yet, returns `new_df` unchanged so the
    first daily run seeds the file.
    """
    if not out_path.exists():
        print(f"🆕 No existing {out_path.name}; writing fresh with {len(new_df)} rows.")
        return new_df

    try:
        existing = pd.read_csv(out_path)
    except Exception as e:
        print(f"⚠️  Could not read {out_path} ({e}); overwriting.")
        return new_df

    if "Job URL" not in existing.columns:
        print(f"⚠️  {out_path.name} has no 'Job URL' column; overwriting.")
        return new_df

    existing_urls = set(existing["Job URL"].dropna().astype(str))
    mask_new = ~new_df["Job URL"].astype(str).isin(existing_urls)
    new_rows = new_df[mask_new]

    if new_rows.empty:
        print(f"ℹ️  No new jobs. {out_path.name} unchanged ({len(existing)} rows).")
        return existing

    # Reconcile column order: keep existing layout, add any missing columns
    # on either side so pd.concat doesn't produce NaN-padding in unexpected spots.
    for col in new_rows.columns:
        if col not in existing.columns:
            existing[col] = ""
    new_rows = new_rows.reindex(columns=existing.columns, fill_value="")

    combined = pd.concat([existing, new_rows], ignore_index=True)
    print(f"➕ Appended {len(new_rows)} new row(s) → {len(combined)} total in {out_path.name}.")
    return combined


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input", default=DEFAULT_INPUT,
                   help=f"Input CSV (default: {DEFAULT_INPUT})")
    p.add_argument("--out", default=DEFAULT_OUTPUT,
                   help=f"Output CSV (default: {DEFAULT_OUTPUT})")
    p.add_argument("--days", type=int, metavar="N",
                   help="Only include jobs scraped in the last N days")
    p.add_argument("--source", metavar="NAME",
                   help="Filter by exact source (e.g. 'StepStone.de', 'Jobs.ch')")
    p.add_argument("--country", metavar="NAME",
                   help="Filter by country (e.g. 'Germany', 'Switzerland', 'Austria')")
    p.add_argument("--company", metavar="TEXT",
                   help="Case-insensitive substring search on company")
    p.add_argument("--tech", metavar="TEXT",
                   help="Case-insensitive substring search on technologies")
    p.add_argument("--sort",
                   choices=["newest", "oldest", "company-asc", "company-desc"],
                   default="newest",
                   help="Sort order (default: newest)")
    p.add_argument("--append", action="store_true",
                   help=("Append mode: keep existing rows in the output file and "
                         "add only rows with a Job URL not already present. Useful "
                         "for building an accumulating history via a daily job."))
    args = p.parse_args()

    # In append mode, default to a stable filename unless the caller overrode --out
    if args.append and args.out == DEFAULT_OUTPUT:
        args.out = APPEND_DEFAULT_OUTPUT

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"❌ Input file not found: {in_path}")
        return 1

    df = load_jobs(in_path)
    before = len(df)
    df = apply_filters(df, args)
    df = apply_sort(df, args.sort)
    out = build_tab1_table(df)

    out_path = Path(args.out)
    if args.append:
        out = merge_with_existing(out, out_path)

    out.to_csv(out_path, index=False)

    print(f"📂 Input:   {in_path}  ({before} rows)")
    print(f"📤 Output:  {out_path.resolve()}  ({len(out)} rows)")
    if not args.append and len(out) < before:
        print(f"🔍 Filters applied: kept {len(out)} of {before}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
