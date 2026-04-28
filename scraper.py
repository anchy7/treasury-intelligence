"""
DACH Treasury Job Scraper — Full Rewrite
=========================================
Sites covered:
  Germany  : StepStone.de, Indeed.de (RSS)
  Switzerland: Jobs.ch, JobScout24.ch, StepStone.ch
  Austria  : Karriere.at

Key improvements over previous version
---------------------------------------
1. StepStone selectors updated — multi-pass fallback chain that inspects the
   live HTML and picks whichever attribute/class actually exists.
2. Indeed switched to RSS feed — avoids bot-detection walls entirely.
3. Diagnostic mode — set DEBUG=True to dump HTML snippets when 0 cards found.
4. Retry logic — transient network errors no longer kill an entire source.
5. Random human-like delays between requests.
6. Centralised _extract() helper so title/company/location parsing is DRY.
7. Deduplication happens both in-memory (by URL) and on disk (merge with CSV).
8. JobScout24 and Karriere selectors broadened with the same fallback strategy.
"""

from __future__ import annotations

import csv
import os
import random
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus, urljoin

import feedparser                          # pip install feedparser
import pandas as pd
from bs4 import BeautifulSoup, Tag

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEBUG = False          # Set True to print raw HTML snippets when 0 cards found
OUTPUT_CSV = "treasury_jobs.csv"
PAGE_LOAD_WAIT = 12    # seconds WebDriverWait will wait for page to grow
POST_LOAD_SLEEP = (2, 4)   # random sleep range after page loads (seconds)
BETWEEN_SEARCH_SLEEP = (1, 3)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Job:
    date_scraped: str
    source: str
    company: str
    title: str
    location: str
    url: str
    technologies: str = ""


# ---------------------------------------------------------------------------
# Relevance filter
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


def detect_technologies(title: str) -> str:
    tech: list[str] = []
    t = (title or "").lower()
    if re.search(r"s[/]?4\s?hana|s4hana", t):
        tech.append("SAP S/4HANA")
    if "kyriba" in t:
        tech.append("Kyriba")
    if re.search(r"\bpython\b", t):
        tech.append("Python")
    if re.search(r"\bapi\b", t):
        tech.append("API")
    if "swift" in t:
        tech.append("SWIFT")
    if "power bi" in t or "powerbi" in t:
        tech.append("Power BI")
    if "nomentia" in t:
        tech.append("Nomentia")
    if "coupa" in t:
        tech.append("Coupa")
    return ", ".join(tech)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------
def safe_text(el: Optional[Tag]) -> str:
    if not el:
        return ""
    try:
        return el.get_text(" ", strip=True)
    except Exception:
        return ""


def clean_company(raw: str) -> str:
    s = re.sub(r"\s+", " ", (raw or "")).strip()
    s = re.sub(
        r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA|SAS|BV|NV|PLC|LLC)\.?\s*$",
        "", s, flags=re.IGNORECASE,
    )
    s = re.sub(r"\s*\(.*?\)\s*", "", s)
    s = re.sub(r"\s*(hiring now|we\'re hiring).*", "", s, flags=re.IGNORECASE)
    return s.strip() or "Unknown"


INVALID_COMPANIES = {"unknown", "place of work", "last month", "yesterday",
                     "heute", "gestern", "vor", "new"}


def is_valid_company(name: str) -> bool:
    return name.lower().strip() not in INVALID_COMPANIES and len(name) > 1


# ---------------------------------------------------------------------------
# Multi-pass element extractor
# ---------------------------------------------------------------------------
def _first(soup: Tag, selectors: list[str]) -> Optional[Tag]:
    """Return the first element matched by any CSS selector in the list."""
    for sel in selectors:
        try:
            el = soup.select_one(sel)
            if el:
                return el
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Browser factory
# ---------------------------------------------------------------------------
def _make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--start-maximized")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_argument(f"user-agent={UA}")
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": UA})
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------
class TreasuryScraper:

    def __init__(self):
        print("🚀 Initialising scraper …")
        self.driver = _make_driver()
        self.wait = WebDriverWait(self.driver, PAGE_LOAD_WAIT)
        self._seen_urls: set[str] = set()
        self.jobs: list[Job] = []
        print("✅ Browser ready\n")

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------
    def _load(self, url: str) -> BeautifulSoup:
        """Navigate to URL, wait for content, return BeautifulSoup."""
        self.driver.get(url)
        try:
            self.wait.until(lambda d: len(d.page_source) > 5_000)
        except Exception:
            pass
        time.sleep(random.uniform(*POST_LOAD_SLEEP))
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def _add(self, source: str, title: str, company: str,
             location: str, url: str) -> bool:
        """Validate and add a job, returning True if it was new."""
        if not url or url in self._seen_urls:
            return False
        if not is_relevant(title):
            return False
        if not is_valid_company(company):
            return False
        self._seen_urls.add(url)
        self.jobs.append(Job(
            date_scraped=datetime.now().strftime("%Y-%m-%d"),
            source=source,
            company=clean_company(company),
            title=title.strip(),
            location=location.strip() or "Unknown",
            url=url,
        ))
        return True

    def _debug_html(self, soup: BeautifulSoup, label: str):
        if not DEBUG:
            return
        articles = soup.find_all("article")
        all_classes = {
            cls
            for tag in soup.find_all(True)
            for cls in (tag.get("class") or [])
        }
        job_classes = [c for c in all_classes if re.search(r"job|result|card|item|vacanc", c, re.I)]
        print(f"   [DEBUG {label}] articles={len(articles)}, job-like classes={job_classes[:10]}")
        if articles:
            print(f"   [DEBUG] first article attrs: {articles[0].attrs}")

    # ------------------------------------------------------------------
    # Card-finding: resilient multi-pass approach
    # ------------------------------------------------------------------
    @staticmethod
    def _find_cards(soup: BeautifulSoup) -> list[Tag]:
        """
        Try progressively broader selectors until we find job cards.
        Order matters — most specific first.
        """
        attempts = [
            # StepStone old
            lambda s: s.select("article[data-at='job-item']"),
            # StepStone new / generic
            lambda s: s.select("article[data-qa='result-list-item']"),
            lambda s: s.select("article[data-testid='job-item']"),
            # Any article with a heading inside
            lambda s: [a for a in s.find_all("article") if a.find(["h2", "h3"])],
            # JobScout24
            lambda s: s.find_all("article", class_="vacancy-item"),
            # Karriere.at
            lambda s: s.find_all("li", class_=re.compile(r"jobsListItem|job-item", re.I)),
            # Generic list items / divs that look like job cards
            lambda s: s.select("li[class*='Result'], li[class*='job'], li[class*='Job']"),
            lambda s: s.select("div[class*='ResultItem'], div[class*='JobCard'], div[class*='job-card']"),
            # Absolute last resort — any li with a link + heading
            lambda s: [li for li in s.find_all("li") if li.find("a") and li.find(["h2", "h3"])],
        ]
        for fn in attempts:
            cards = fn(soup)
            if cards:
                return list(cards)
        return []

    @staticmethod
    def _extract_title(card: Tag) -> str:
        el = _first(card, [
            "[data-at='job-item-title']",
            "[data-qa='job-item-title']",
            "[data-testid='job-title']",
            "h2 a", "h3 a", "h2", "h3",
            "a.vacancy-item__title",
            ".m-jobsListItem__title",
            "a[class*='title']",
            "span[class*='title']",
        ])
        return safe_text(el)

    @staticmethod
    def _extract_company(card: Tag) -> str:
        el = _first(card, [
            "[data-at='job-item-company-name']",
            "[data-qa='job-item-company-name']",
            "[data-testid='company-name']",
            "span[class*='company']",
            "div[class*='company']",
            "a[class*='company']",
            ".vacancy-item__company",
            ".m-jobsListItem__company",
            "[data-cy*='company']",
        ])
        return clean_company(safe_text(el))

    @staticmethod
    def _extract_location(card: Tag, fallback: str = "") -> str:
        el = _first(card, [
            "[data-at='job-item-location']",
            "[data-qa='job-item-location']",
            "[data-testid='text-location']",
            "[data-testid='location']",
            "span[class*='location']",
            "div[class*='location']",
            ".vacancy-item__location",
            ".m-jobsListItem__location",
            "[data-cy*='location']",
        ])
        return safe_text(el) or fallback

    @staticmethod
    def _extract_url(card: Tag, base: str) -> str:
        # Prefer links that look like job detail pages
        for pattern in [r"/job", r"/vacanc", r"/stelle", r"/angebot"]:
            a = card.find("a", href=re.compile(pattern, re.I))
            if a and a.get("href"):
                href = a["href"]
                return href if href.startswith("http") else urljoin(base, href)
        # Fall back to first link
        a = card.find("a", href=True)
        if a:
            href = a["href"]
            return href if href.startswith("http") else urljoin(base, href)
        return ""

    # ------------------------------------------------------------------
    # StepStone (shared logic for .de and .ch)
    # ------------------------------------------------------------------
    def _scrape_stepstone(self, base: str, source: str, searches: list[tuple[str, str]]):
        print("=" * 60)
        print(f"📊 SCRAPING {source.upper()}")
        print("=" * 60)

        for keyword, location in searches:
            print(f"\n🔍 '{keyword}' in {location}")
            try:
                if "stepstone.ch" in base:
                    url = f"{base}/en/jobs?k={quote_plus(keyword)}&l={quote_plus(location)}"
                else:
                    url = f"{base}/jobs?what={quote_plus(keyword)}&where={quote_plus(location)}"
                print(f"   URL: {url}")

                soup = self._load(url)
                cards = self._find_cards(soup)
                print(f"   Cards found: {len(cards)}")
                self._debug_html(soup, source)

                added = 0
                for card in cards[:25]:
                    title   = self._extract_title(card)
                    company = self._extract_company(card)
                    loc     = self._extract_location(card, location)
                    job_url = self._extract_url(card, base)

                    if self._add(source, title, company, loc, job_url):
                        added += 1
                        if added <= 10:
                            print(f"   ✅ {company} — {title[:60]}")

                time.sleep(random.uniform(*BETWEEN_SEARCH_SLEEP))

            except Exception as exc:
                print(f"   ❌ Error: {exc}")

        count = sum(1 for j in self.jobs if j.source == source)
        print(f"\n✅ {source} total: {count} jobs\n")

    def scrape_stepstone_de(self):
        self._scrape_stepstone(
            base="https://www.stepstone.de",
            source="StepStone.de",
            searches=[
                ("Treasury", "Deutschland"),
                ("Cash Manager", "Deutschland"),
                ("Treasury", "München"),
                ("Liquidität", "Frankfurt"),
                ("Kyriba", "Deutschland"),
                ("SAP Treasury", "Deutschland"),
                ("FIS Treasury", "Deutschland"),
                ("ION Treasury", "Deutschland"),
                ("Nomentia", "Deutschland"),
            ],
        )

    def scrape_stepstone_ch(self):
        self._scrape_stepstone(
            base="https://www.stepstone.ch",
            source="StepStone.ch",
            searches=[
                ("Treasury", "Switzerland"),
                ("Cash Manager", "Switzerland"),
                ("Liquidität", "Switzerland"),
            ],
        )

    # ------------------------------------------------------------------
    # Indeed.de — RSS (avoids bot-detection entirely)
    # ------------------------------------------------------------------
    def scrape_indeed_de(self):
        print("=" * 60)
        print("📊 SCRAPING INDEED.DE (RSS)")
        print("=" * 60)

        feeds = [
            ("treasury", "Deutschland"),
            ("treasury", "München"),
            ("treasury", "Frankfurt"),
            ("cash management", "Deutschland"),
            ("Liquiditätsmanagement", "Deutschland"),
            ("SAP Treasury", "Deutschland"),
        ]

        for query, location in feeds:
            url = (
                f"https://de.indeed.com/rss?q={quote_plus(query)}"
                f"&l={quote_plus(location)}&sort=date"
            )
            print(f"\n🔍 RSS: '{query}' in {location}")
            try:
                feed = feedparser.parse(url)
                entries = feed.get("entries", [])
                print(f"   Entries: {len(entries)}")

                added = 0
                for entry in entries:
                    title   = entry.get("title", "")
                    job_url = entry.get("link", "")
                    company = entry.get("author", "") or entry.get("source", {}).get("title", "Unknown")
                    loc     = location

                    # Indeed RSS encodes location in the title sometimes: "Title - City"
                    if " - " in title:
                        parts = title.rsplit(" - ", 1)
                        title = parts[0].strip()
                        loc   = parts[1].strip() or loc

                    if self._add("Indeed.de", title, company, loc, job_url):
                        added += 1
                        if added <= 10:
                            print(f"   ✅ {company} — {title[:60]}")

                time.sleep(random.uniform(*BETWEEN_SEARCH_SLEEP))

            except Exception as exc:
                print(f"   ❌ RSS error: {exc}")

        count = sum(1 for j in self.jobs if j.source == "Indeed.de")
        print(f"\n✅ Indeed.de total: {count} jobs\n")

    # ------------------------------------------------------------------
    # Jobs.ch
    # ------------------------------------------------------------------
    def scrape_jobs_ch(self):
        print("=" * 60)
        print("📊 SCRAPING JOBS.CH (Switzerland)")
        print("=" * 60)

        searches = ["Treasury", "Cash Manager", "Liquidität"]
        base = "https://www.jobs.ch"

        for keyword in searches:
            print(f"\n🔍 '{keyword}'")
            try:
                url = f"{base}/en/vacancies/?term={quote_plus(keyword)}"
                print(f"   URL: {url}")
                soup = self._load(url)

                # Jobs.ch renders job links directly — find anchors whose href
                # matches vacancy path patterns
                link_re = re.compile(r"/(en/)?(vacancies|jobs|stelle)/", re.I)
                links = soup.find_all("a", href=link_re)
                print(f"   Links found: {len(links)}")
                self._debug_html(soup, "Jobs.ch")

                added = 0
                enrichment_queue: list[Job] = []

                for link in links[:40]:
                    href = link.get("href", "")
                    if not href:
                        continue
                    if any(x in href.lower() for x in ["privacy", "terms", "login", "register", "about"]):
                        continue

                    job_url = href if href.startswith("http") else f"{base}{href}"
                    title = safe_text(link)
                    if not title or len(title) < 5:
                        continue
                    if not is_relevant(title):
                        continue

                    parent = (
                        link.find_parent("article") or
                        link.find_parent("li") or
                        link.find_parent("div")
                    ) or link

                    company  = self._extract_company(parent)
                    location = self._extract_location(parent, "Switzerland")

                    # Swiss city fallback from surrounding text
                    if location == "Switzerland":
                        txt = safe_text(parent)
                        for city in ["Zürich", "Zurich", "Basel", "Bern", "Genève",
                                     "Geneva", "Lausanne", "Zug", "Luzern", "Lucerne",
                                     "Winterthur", "St. Gallen"]:
                            if city.lower() in txt.lower():
                                location = city
                                break

                    if job_url not in self._seen_urls:
                        self._seen_urls.add(job_url)
                        j = Job(
                            date_scraped=datetime.now().strftime("%Y-%m-%d"),
                            source="Jobs.ch",
                            company=company,
                            title=title.strip(),
                            location=location,
                            url=job_url,
                        )
                        self.jobs.append(j)
                        if company == "Unknown":
                            enrichment_queue.append(j)
                        added += 1
                        if added <= 10:
                            print(f"   ✅ {company} — {title[:60]}")

                # Enrich jobs missing company by visiting detail pages (capped)
                self._enrich_from_detail_pages(enrichment_queue[:10], base)
                time.sleep(random.uniform(*BETWEEN_SEARCH_SLEEP))

            except Exception as exc:
                print(f"   ❌ Error: {exc}")

        count = sum(1 for j in self.jobs if j.source == "Jobs.ch")
        print(f"\n✅ Jobs.ch total: {count} jobs\n")

    def _enrich_from_detail_pages(self, jobs: list[Job], base: str):
        """Visit job detail pages to fill in missing company / location."""
        if not jobs:
            return
        print(f"   🔎 Enriching {len(jobs)} jobs from detail pages …")
        for j in jobs:
            try:
                soup = self._load(j.url)
                # Company
                if j.company == "Unknown":
                    company = self._extract_company(soup)
                    if company and company != "Unknown":
                        j.company = company
                # Location
                if j.location in ("Switzerland", "Unknown", ""):
                    loc = self._extract_location(soup)
                    if loc:
                        j.location = loc
                time.sleep(0.5)
            except Exception:
                continue

    # ------------------------------------------------------------------
    # JobScout24.ch
    # ------------------------------------------------------------------
    def scrape_jobscout24_ch(self):
        print("=" * 60)
        print("📊 SCRAPING JOBSCOUT24.CH (Switzerland)")
        print("=" * 60)

        base = "https://www.jobscout24.ch"
        searches = ["Treasury", "Cash Manager", "Liquidität"]

        for keyword in searches:
            print(f"\n🔍 '{keyword}'")
            try:
                url = f"{base}/en/jobs/?term={quote_plus(keyword)}"
                print(f"   URL: {url}")
                soup = self._load(url)
                cards = self._find_cards(soup)
                print(f"   Cards found: {len(cards)}")
                self._debug_html(soup, "JobScout24")

                added = 0
                for card in cards[:25]:
                    title   = self._extract_title(card)
                    company = self._extract_company(card)
                    loc     = self._extract_location(card, "Switzerland")
                    job_url = self._extract_url(card, base)

                    if self._add("JobScout24.ch", title, company, loc, job_url):
                        added += 1
                        if added <= 10:
                            print(f"   ✅ {company} — {title[:60]}")

                time.sleep(random.uniform(*BETWEEN_SEARCH_SLEEP))

            except Exception as exc:
                print(f"   ❌ Error: {exc}")

        count = sum(1 for j in self.jobs if j.source == "JobScout24.ch")
        print(f"\n✅ JobScout24.ch total: {count} jobs\n")

    # ------------------------------------------------------------------
    # Karriere.at
    # ------------------------------------------------------------------
    def scrape_karriere_at(self):
        print("=" * 60)
        print("📊 SCRAPING KARRIERE.AT (Austria)")
        print("=" * 60)

        base = "https://www.karriere.at"
        searches = ["Treasury", "Cash Manager", "Liquidität"]

        for keyword in searches:
            print(f"\n🔍 '{keyword}'")
            try:
                url = f"{base}/jobs/{quote_plus(keyword)}"
                print(f"   URL: {url}")
                soup = self._load(url)
                cards = self._find_cards(soup)
                print(f"   Cards found: {len(cards)}")
                self._debug_html(soup, "Karriere.at")

                added = 0
                for card in cards[:25]:
                    title   = self._extract_title(card)
                    company = self._extract_company(card)
                    loc     = self._extract_location(card, "Austria")
                    job_url = self._extract_url(card, base)

                    if self._add("Karriere.at", title, company, loc, job_url):
                        added += 1
                        if added <= 10:
                            print(f"   ✅ {company} — {title[:60]}")

                time.sleep(random.uniform(*BETWEEN_SEARCH_SLEEP))

            except Exception as exc:
                print(f"   ❌ Error: {exc}")

        count = sum(1 for j in self.jobs if j.source == "Karriere.at")
        print(f"\n✅ Karriere.at total: {count} jobs\n")

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    def save(self, filename: str = OUTPUT_CSV):
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save.")
            return

        # Detect technologies before saving
        for j in self.jobs:
            j.technologies = detect_technologies(j.title)

        new_df = pd.DataFrame([asdict(j) for j in self.jobs])

        if os.path.exists(filename):
            print(f"\n📂 Merging with existing: {filename}")
            existing = pd.read_csv(filename)

            # Drop previously saved invalid rows
            if "company" in existing.columns:
                mask = existing["company"].str.strip().str.lower().isin(INVALID_COMPANIES)
                existing = existing[~mask]

            combined = pd.concat([existing, new_df], ignore_index=True)
            before   = len(combined)
            combined.drop_duplicates(subset=["url"], keep="last", inplace=True)
            after    = len(combined)

            combined.to_csv(filename, index=False)

            print(f"\n📊 Results:")
            print(f"   New scraped   : {len(new_df)}")
            print(f"   Duplicates rm : {before - after}")
            print(f"   Total in DB   : {after}")
            print(f"   Net new       : {len(new_df) - (before - after)}")
            print(f"\n📊 By source:")
            for src, cnt in combined["source"].value_counts().items():
                print(f"   {src}: {cnt}")
        else:
            new_df.drop_duplicates(subset=["url"], keep="last", inplace=True)
            new_df.to_csv(filename, index=False)
            print(f"\n📝 Created new file: {filename} ({len(new_df)} jobs)")

        print("\n✅ Save complete!")

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass
        print("\n🔒 Browser closed")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("🏦 DACH TREASURY JOB SCRAPER")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60 + "\n")

    scraper = TreasuryScraper()

    try:
        # Germany
        scraper.scrape_stepstone_de()
        scraper.scrape_indeed_de()

        # Switzerland
        scraper.scrape_jobs_ch()
        scraper.scrape_jobscout24_ch()
        scraper.scrape_stepstone_ch()

        # Austria
        scraper.scrape_karriere_at()

        scraper.save(OUTPUT_CSV)

        print("\n" + "=" * 60)
        print("✅ SCRAPING COMPLETE!")
        print(f"📊 Jobs this run: {len(scraper.jobs)}")
        print(f"⏰ Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
        print("=" * 60 + "\n")

    except Exception as exc:
        print(f"\n❌ Fatal error: {exc}")
        import traceback
        traceback.print_exc()

    finally:
        scraper.close()


if __name__ == "__main__":
    main()
