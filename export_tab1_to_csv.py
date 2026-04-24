"""
export_tab1_to_csv.py
---------------------
Headless / CLI version of the "📋 All Jobs Table" (tab1) from sales_dashboard.py.

Produces a CSV with the exact same columns that tab1 displays in the Streamlit
dashboard, plus an extra `Job URL` column sourced from the scraper's `url`
field, plus two enrichment placeholder columns — `Suggested Topics` and
`Last Projects` — that the weekly Copilot enrichment flow fills in afterwards.
All of tab1's sidebar filters (date range, source, country, company search,
technology search) and the sort selector are exposed as CLI flags.

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
# `In CRM` and `Last Contacted` are computed from crm_all_companies.csv
# (semicolon-delimited) using the same normalized-name match used by
# sales_dashboard (2).py. `Job URL` is sourced from the scraper's `url` field.
# `Suggested Topics` and `Last Projects` are placeholders for values written
# back by the weekly Copilot enrichment flow — the scraper emits them empty.
TAB1_COLUMNS: list[tuple[str, str]] = [
    ("company",          "Company"),
    ("title",            "Job Title"),
    ("location",         "Location"),
    ("Country",          "Country"),
    ("Company_in_CRM",   "In CRM"),          # computed from CRM
    ("Last_Contacted",   "Last Contacted"),   # computed from CRM
    ("source",           "Job Source"),
    ("date_scraped",     "Posted Date"),
    ("technologies",     "Technologies"),
    ("url",              "Job URL"),
    ("suggested_topics", "Suggested Topics"),  # populated by Copilot enrichment flow
    ("last_projects",    "Last Projects"),     # populated by Copilot enrichment flow
]

DEFAULT_CRM_FILE = "crm_all_companies.csv"

# Use plain UTF-8 (no BOM) for all CSV I/O. This keeps the file a clean UTF-8
# byte stream that Power Automate, VS Code and modern Excel can all parse
# without the leading ï»¿ BOM sneaking into the first cell header.
CSV_ENCODING = "utf-8"

# Field separator for the main tab1 export. Semicolon is used (instead of the
# default comma) because the dataset contains free-text fields — Job Title,
# Technologies, Suggested Topics, Last Projects — that frequently embed commas.
# Using ';' lets a naïve split('\n') + split(';') CSV parser (e.g. the Power
# Automate expressions in the weekly enrichment flow) tokenize rows correctly
# without needing full RFC 4180 quote-aware parsing. The CRM file
# (crm_all_companies.csv) is ALSO semicolon-delimited but kept as its own
# read with an explicit sep=';' for clarity.
CSV_SEPARATOR = ";"


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


# ---------------------------------------------------------------------------
# CRM enrichment — mirrors sales_dashboard (2).py so values match the dashboard
# ---------------------------------------------------------------------------

_LEGAL_SUFFIXES = (
    " gmbh & co. kg", " gmbh", " ag", " se", " kg", " kgaa", " ltd", " limited",
    " inc", " inc.", " corp", " corporation", " sa", " plc", " b.v.", " n.v.",
    " llc",
)


def normalize_company_name(company) -> str:
    """Lower-case, strip legal suffixes + punctuation — used for CRM matching."""
    if pd.isna(company):
        return ""
    s = str(company).strip().lower()
    # Strip the longest matching suffix first (e.g. " gmbh & co. kg" before " gmbh")
    for suffix in _LEGAL_SUFFIXES:
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
            break
    s = s.replace(".", "").replace(",", "")
    s = " ".join(s.split())  # collapse whitespace
    return s


def load_crm(path: Path) -> pd.DataFrame:
    """Load crm_all_companies.csv (semicolon-delimited). Returns empty df if missing."""
    if not path.exists():
        print(f"⚠️  CRM file not found ({path}); 'In CRM' will be 'Nein' for every row.")
        return pd.DataFrame(columns=["Company name", "Last Contacted", "company_clean"])
    try:
        df = pd.read_csv(path, sep=";", encoding=CSV_ENCODING)
    except Exception as e:
        print(f"⚠️  Could not read CRM file {path}: {e}; skipping CRM enrichment.")
        return pd.DataFrame(columns=["Company name", "Last Contacted", "company_clean"])

    if "Company name" not in df.columns or "Last Contacted" not in df.columns:
        print(f"⚠️  {path} missing required columns; skipping CRM enrichment.")
        return pd.DataFrame(columns=["Company name", "Last Contacted", "company_clean"])

    df["Company name"] = df["Company name"].fillna("").astype(str).str.strip()
    df["company_clean"] = df["Company name"].apply(normalize_company_name)
    df["Last Contacted"] = df["Last Contacted"].replace("", pd.NaT)
    df["Last Contacted"] = pd.to_datetime(
        df["Last Contacted"], format="%d/%m/%Y %H:%M", errors="coerce"
    )
    return df


def _build_crm_lookup(crm: pd.DataFrame) -> tuple[dict, list]:
    """Pre-compute exact-match dict + substring-search list once per run."""
    if crm.empty:
        return {}, []
    # Exact match: normalized_name -> most-recent Last Contacted
    exact: dict[str, pd.Timestamp] = {}
    for name, last in zip(crm["company_clean"], crm["Last Contacted"]):
        if not name:
            continue
        prev = exact.get(name)
        if prev is None or (pd.notna(last) and (pd.isna(prev) or last > prev)):
            exact[name] = last
    substring_items = [(k, v) for k, v in exact.items() if len(k) > 3]
    return exact, substring_items


def enrich_with_crm(jobs: pd.DataFrame, crm: pd.DataFrame) -> pd.DataFrame:
    """
    Add `Company_in_CRM` ("Ja"/"Nein") and `Last_Contacted` (datetime or NaT).
    Matches exactly first, falls back to substring containment (both sides > 3
    chars), and caches per unique normalized company name for speed.
    """
    jobs = jobs.copy()
    if crm.empty:
        jobs["Company_in_CRM"] = "Nein"
        jobs["Last_Contacted"] = pd.NaT
        return jobs

    exact, substring_items = _build_crm_lookup(crm)
    cache: dict[str, tuple[bool, object]] = {}

    def lookup(company) -> tuple[bool, object]:
        norm = normalize_company_name(company)
        if not norm:
            return (False, pd.NaT)
        if norm in cache:
            return cache[norm]
        if norm in exact:
            result = (True, exact[norm])
        elif len(norm) > 3:
            result = (False, pd.NaT)
            for crm_name, last in substring_items:
                if norm in crm_name or crm_name in norm:
                    result = (True, last)
                    break
        else:
            result = (False, pd.NaT)
        cache[norm] = result
        return result

    matches = jobs["company"].apply(lookup)
    jobs["Company_in_CRM"] = matches.apply(lambda r: "Ja" if r[0] else "Nein")
    jobs["Last_Contacted"] = matches.apply(lambda r: r[1])
    return jobs


def format_last_contacted(series: pd.Series) -> pd.Series:
    """Render the Last_Contacted column as YYYY-MM-DD strings (empty if NaT)."""
    s = pd.to_datetime(series, errors="coerce")
    return s.dt.strftime("%Y-%m-%d").fillna("")


# ---------------------------------------------------------------------------


def load_jobs(path: Path) -> pd.DataFrame:
    # The input scraper CSV is comma-delimited (historical format), so we do
    # NOT pass CSV_SEPARATOR here — let pandas use its default comma. Only the
    # OUTPUT written by this script uses CSV_SEPARATOR.
    df = pd.read_csv(path, encoding="utf-8-sig")
    df["date_scraped"] = pd.to_datetime(df["date_scraped"], errors="coerce")
    df["Country"] = _country_from_source_vectorized(df)
    # Ensure optional columns exist so downstream selection never KeyErrors.
    # `suggested_topics` / `last_projects` are populated later by the Copilot
    # enrichment flow (Power Automate writes back into the CSV); we seed them
    # empty here so fresh scraper rows carry the correct column layout.
    for col in ("url", "technologies", "suggested_topics", "last_projects"):
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
    """Project down to tab1's columns (+ Job URL + CRM columns) with display names."""
    internal, display = zip(*TAB1_COLUMNS)
    # Defensive: if CRM enrichment was skipped upstream, seed the columns empty
    for col in ("Company_in_CRM", "Last_Contacted"):
        if col not in df.columns:
            df[col] = "" if col == "Company_in_CRM" else pd.NaT
    # Defensive: enrichment columns ('Suggested Topics', 'Last Projects') are
    # populated by the Power Automate flow, not the scraper. Seed them empty
    # if they aren't already present so the column selection below doesn't
    # KeyError on fresh data.
    for col in ("suggested_topics", "last_projects"):
        if col not in df.columns:
            df[col] = ""
    out = df[list(internal)].copy()
    out["date_scraped"] = out["date_scraped"].dt.strftime("%Y-%m-%d")
    out["Last_Contacted"] = format_last_contacted(out["Last_Contacted"])
    out.columns = list(display)
    return out


def _refresh_crm_cols_in_existing(existing: pd.DataFrame, crm: pd.DataFrame) -> pd.DataFrame:
    """
    CRM fields (`In CRM`, `Last Contacted`) are derived — not a historical
    record of what they were the day the row was exported. Every run re-derives
    them from the current CRM snapshot so old rows stay accurate.

    For back-compat: if the existing CSV still has the legacy `Letzter Kontakt`
    column, it is dropped and replaced by the new `Last Contacted` column so
    the file migrates cleanly on the next run.
    """
    if crm.empty or "Company" not in existing.columns:
        return existing
    tmp = existing.rename(columns={"Company": "company"})
    enriched = enrich_with_crm(tmp[["company"]], crm)
    existing = existing.copy()
    existing["In CRM"] = enriched["Company_in_CRM"].values
    if "Letzter Kontakt" in existing.columns:
        existing = existing.drop(columns=["Letzter Kontakt"])
    existing["Last Contacted"] = format_last_contacted(enriched["Last_Contacted"]).values
    return existing


def merge_with_existing(new_df: pd.DataFrame, out_path: Path, crm: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Append-mode merge. Preserves every row already in `out_path` (keeps their
    original order) and adds only rows from `new_df` whose `Job URL` is not
    yet present. New rows are appended at the bottom.

    If a CRM dataframe is supplied, the CRM columns on *existing* rows are
    also refreshed, so changing contact dates propagate to old rows.

    If the output file doesn't exist yet, returns `new_df` unchanged so the
    first daily run seeds the file.
    """
    if not out_path.exists():
        print(f"🆕 No existing {out_path.name}; writing fresh with {len(new_df)} rows.")
        return new_df

    try:
        # Read back the previous run's output using the same separator + encoding
        # it was written with. `utf-8-sig` here is deliberate (not CSV_ENCODING) —
        # it transparently handles both plain UTF-8 (current format) and legacy
        # files that were written with a BOM, so the first run after the
        # UTF-8-with-BOM → plain-UTF-8 migration doesn't fail.
        existing = pd.read_csv(out_path, sep=CSV_SEPARATOR, encoding="utf-8-sig")
    except Exception as e:
        print(f"⚠️  Could not read {out_path} ({e}); overwriting.")
        return new_df

    if "Job URL" not in existing.columns:
        print(f"⚠️  {out_path.name} has no 'Job URL' column; overwriting.")
        return new_df

    # Refresh CRM-derived columns on existing rows using the current CRM snapshot
    if crm is not None and not crm.empty:
        existing = _refresh_crm_cols_in_existing(existing, crm)

    existing_urls = set(existing["Job URL"].dropna().astype(str))
    mask_new = ~new_df["Job URL"].astype(str).isin(existing_urls)
    new_rows = new_df[mask_new]

    if new_rows.empty:
        print(f"ℹ️  No new jobs. {out_path.name} updated in place ({len(existing)} rows, CRM refreshed).")
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
    p.add_argument("--crm", default=DEFAULT_CRM_FILE,
                   help=f"CRM CSV used to compute 'In CRM' + 'Last Contacted' "
                        f"(default: {DEFAULT_CRM_FILE}, semicolon-delimited)")
    p.add_argument("--no-crm", action="store_true",
                   help="Skip CRM enrichment entirely — those columns will be empty.")
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

    # CRM enrichment (per-company cached lookup; mirrors sales_dashboard (2).py logic)
    if args.no_crm:
        crm = pd.DataFrame()
    else:
        crm = load_crm(Path(args.crm))
    df = enrich_with_crm(df, crm)

    df = apply_filters(df, args)
    df = apply_sort(df, args.sort)
    out = build_tab1_table(df)

    out_path = Path(args.out)
    if args.append:
        out = merge_with_existing(out, out_path, crm=crm if not args.no_crm else None)

    out.to_csv(out_path, index=False, sep=CSV_SEPARATOR, encoding=CSV_ENCODING)

    print(f"📂 Input:   {in_path}  ({before} rows)")
    print(f"📤 Output:  {out_path.resolve()}  ({len(out)} rows)")
    if not args.append and len(out) < before:
        print(f"🔍 Filters applied: kept {len(out)} of {before}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
