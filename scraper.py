"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions (or locally)

Key improvements vs. your earlier version:
- StepStone: uses stable query parameters (what/where) + URL encoding
- Jobs.ch: extracts from job links + tries multiple company selectors
- Jobs.ch fallback: opens a few detail pages to fetch company/location if still Unknown
- Uses Selenium Manager (no webdriver-manager)
- Dedupes by URL (most reliable)
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


@dataclass
class Job:
    date_scraped: str
    source: str
    company: str
    title: str
    location: str
    url: str
    technologies: str = ""


class TreasuryWebScraper:
    def __init__(self):
        print("🚀 Initializing web scraper...")

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # A realistic modern UA (keep it consistent across runs)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        )

        # Selenium Manager resolves driver/browser
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)

        self.jobs: list[dict] = []
        print("✅ Web scraper ready\n")

    # ---------------------------
    # STEPSTONE
    # ---------------------------
    def scrape_stepstone_de(self):
        """Scrape StepStone.de for treasury jobs"""
        print("=" * 60)
        print("📊 SCRAPING STEPSTONE.DE")
        print("=" * 60)

        searches = [
            ("Treasury", "Deutschland"),
            ("Cash Manager", "Deutschland"),
            ("Treasury", "München"),
            ("Liquidity", "Frankfurt"),
        ]

        for keyword, location in searches:
            print(f"\n🔍 Searching: '{keyword}' in {location}")

            try:
                # Stable params
                url = (
                    "https://www.stepstone.de/jobs?"
                    f"what={quote_plus(keyword)}&where={quote_plus(location)}"
                )
                print(f"   URL: {url}")

                self.driver.get(url)
                self._wait_for_html()

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                job_cards = soup.select("article[data-at='job-item']")
                if not job_cards:
                    job_cards = soup.find_all("article", class_=re.compile("job", re.IGNORECASE))

                print(f"   Found {len(job_cards)} job cards")

                added = 0
                for card in job_cards[:20]:
                    try:
                        title_elem = (
                            card.select_one('[data-at="job-item-title"]')
                            or card.find("h2")
                            or card.find("a", class_=re.compile("title", re.IGNORECASE))
                        )
                        title = title_elem.get_text(strip=True) if title_elem else "Unknown"

                        company_elem = (
                            card.select_one('[data-at="job-item-company-name"]')
                            or card.find("span", class_=re.compile("company", re.IGNORECASE))
                        )
                        company = company_elem.get_text(strip=True) if company_elem else "Unknown"
                        company = self._clean_company(company)

                        location_elem = (
                            card.select_one('[data-at="job-item-location"]')
                            or card.find("span", class_=re.compile("location", re.IGNORECASE))
                        )
                        job_location = location_elem.get_text(strip=True) if location_elem else location

                        link_elem = card.find("a", href=True)
                        job_url = ""
                        if link_elem:
                            href = link_elem["href"]
                            job_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                        if title != "Unknown" and company != "Unknown" and job_url:
                            self.jobs.append(
                                {
                                    "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                                    "source": "StepStone.de",
                                    "company": company,
                                    "title": title,
                                    "location": job_location,
                                    "url": job_url,
                                }
                            )
                            added += 1
                            if added <= 15:
                                print(f"   ✅ [{added}] {company} - {title[:55]}")

                    except Exception as e:
                        print(f"   ⚠️  Error parsing job card: {str(e)[:120]}")
                        continue

                time.sleep(1)

            except Exception as e:
                print(f"   ❌ Error scraping StepStone: {e}")

        stepstone_count = len([j for j in self.jobs if j["source"] == "StepStone.de"])
        print(f"\n✅ StepStone Total: {stepstone_count} jobs\n")

    # ---------------------------
    # JOBS.CH
    # ---------------------------
    def scrape_jobs_ch(self):
        """
        Scrape Jobs.ch for Swiss treasury jobs.

        Why company was "Unknown" before:
        - list pages often don't contain "at Company" text
        - company sits in specific spans/divs and your regex didn't hit
        - sometimes company is only reliable on detail pages

        This version:
        - extracts job URLs from list page
        - tries multiple selectors for company/location in the list item
        - falls back to opening a limited number of detail pages to fill Unknowns
        """
        print("=" * 60)
        print("📊 SCRAPING JOBS.CH (Switzerland)")
        print("=" * 60)

        searches = ["Treasury", "Cash Manager", "Liquidität"]

        for keyword in searches:
            print(f"\n🔍 Searching: '{keyword}'")

            try:
                url = f"https://www.jobs.ch/en/vacancies/?term={quote_plus(keyword)}"
                print(f"   URL: {url}")

                self.driver.get(url)
                self._wait_for_html()

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # Link patterns tend to be more stable than card markup
                links = soup.find_all(
                    "a",
                    href=re.compile(r"(/en/vacancies/|/vacancies/|/en/jobs/|/jobs/)", re.IGNORECASE),
                )
                print(f"   Found {len(links)} job links")

                added = 0
                seen_urls: set[str] = set()

                # Collect candidates from the list page first
                candidates: list[dict] = []
                for link in links:
                    href = (link.get("href") or "").strip()
                    if not href:
                        continue

                    # Basic noise filter: ignore obvious non-job links
                    if any(x in href.lower() for x in ["privacy", "terms", "login", "register"]):
                        continue

                    job_url = href if href.startswith("http") else f"https://www.jobs.ch{href}"

                    if job_url in seen_urls:
                        continue
                    seen_urls.add(job_url)

                    title = link.get_text(strip=True)
                    if not title or len(title) < 6:
                        continue

                    parent = link.find_parent(["article", "li", "div", "section"]) or link.parent
                    parent_soup = parent if parent else link

                    company = self._extract_company_jobs_ch(parent_soup)
                    job_location = self._extract_location_jobs_ch(parent_soup)

                    candidates.append(
                        {
                            "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                            "source": "Jobs.ch",
                            "company": company,
                            "title": title,
                            "location": job_location,
                            "url": job_url,
                        }
                    )

                    if len(candidates) >= 35:
                        break

                # Fallback: open detail pages for missing company/location (cap to keep runtime OK)
                candidates = self._enrich_jobs_ch_from_detail_pages(candidates, max_details=12)

                for c in candidates:
                    self.jobs.append(c)
                    added += 1
                    if added <= 15:
                        print(f"   ✅ [{added}] {c['company']} - {c['title'][:55]}")

                print(f"   Added {added} Jobs.ch jobs")
                time.sleep(1)

            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        jobsch_count = len([j for j in self.jobs if j["source"] == "Jobs.ch"])
        print(f"\n✅ Jobs.ch Total: {jobsch_count} jobs\n")

    # ---------------------------
    # JOBS.CH helpers
    # ---------------------------
    def _extract_company_jobs_ch(self, node) -> str:
        """
        Try many likely selectors/attributes for company on Jobs.ch list cards.
        If none hit, return "Unknown".
        """
        # 1) Try attribute-driven elements (data-cy is common on modern sites)
        selectors = [
            "[data-cy*=company]",
            "[data-testid*=company]",
            "[class*=company]",
            "span[class*=Company]",
            "div[class*=Company]",
        ]
        for sel in selectors:
            el = node.select_one(sel) if hasattr(node, "select_one") else None
            if el:
                txt = el.get_text(" ", strip=True)
                txt = self._clean_company(txt)
                if txt and txt.lower() != "unknown":
                    return txt

        # 2) Try common tag/class regex
        if hasattr(node, "find"):
            el = node.find(["span", "div"], class_=re.compile(r"company|employer|firm", re.IGNORECASE))
            if el:
                txt = self._clean_company(el.get_text(" ", strip=True))
                if txt and txt.lower() != "unknown":
                    return txt

        # 3) Fallback regex on surrounding text
        text = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
        # Sometimes displayed like: "CompanyName • Zurich"
        m = re.search(r"^(.+?)\s*[•|]\s*(Zurich|Zürich|Basel|Bern|Geneva|Genève|Lausanne|Zug|Luzern|Lucerne)\b", text)
        if m:
            return self._clean_company(m.group(1).strip())

        # Sometimes contains labels like "Company: X"
        m = re.search(r"(?:Company|Firma|Unternehmen)\s*[:\-]\s*(.+?)(?:\s{2,}|•|\||$)", text, re.IGNORECASE)
        if m:
            return self._clean_company(m.group(1).strip())

        return "Unknown"

    def _extract_location_jobs_ch(self, node) -> str:
        """Best-effort location extraction from list card text."""
        # 1) Try selector
        selectors = [
            "[data-cy*=location]",
            "[data-testid*=location]",
            "[class*=location]",
        ]
        for sel in selectors:
            el = node.select_one(sel) if hasattr(node, "select_one") else None
            if el:
                txt = self._safe_text(el)
                if txt:
                    return txt

        # 2) Regex city guess
        text = self._safe_text(node) if hasattr(node, "get_text") else ""
        swiss_cities = [
            "Zürich", "Zurich", "Basel", "Bern", "Genève", "Geneva", "Lausanne",
            "Zug", "Luzern", "Lucerne", "Winterthur", "St. Gallen", "St Gallen",
        ]
        for city in swiss_cities:
            if city.lower() in text.lower():
                return city

        return "Switzerland"

    def _enrich_jobs_ch_from_detail_pages(self, candidates: list[dict], max_details: int = 10) -> list[dict]:
        """
        For candidates with company/location Unknown, open the job detail page and try to extract.
        Keeps the run time under control by limiting detail page fetches.
        """
        to_enrich = [c for c in candidates if (c.get("company") == "Unknown" or c.get("location") in ("Switzerland", "", None))]
        if not to_enrich:
            return candidates

        # Only enrich a limited number (keep daily Actions quick)
        to_enrich = to_enrich[:max_details]

        for c in to_enrich:
            try:
                self.driver.get(c["url"])
                self._wait_for_html()

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # Common patterns on detail pages:
                # - Company in header area
                # - Location near title
                # We'll attempt broad but safe extraction.
                title_text = self._safe_text(soup.select_one("h1")) or c["title"]

                # Company attempts
                company = c["company"]
                company_candidates = [
                    soup.select_one("[data-cy*=company]"),
                    soup.select_one("[data-testid*=company]"),
                    soup.select_one("[class*=company]"),
                    soup.find(["span", "div"], class_=re.compile(r"company|employer|firm", re.IGNORECASE)),
                ]
                for el in company_candidates:
                    txt = self._safe_text(el)
                    txt = self._clean_company(txt)
                    if txt and txt.lower() != "unknown" and len(txt) > 1:
                        company = txt
                        break

                # Location attempts
                location = c["location"]
                loc_candidates = [
                    soup.select_one("[data-cy*=location]"),
                    soup.select_one("[data-testid*=location]"),
                    soup.select_one("[class*=location]"),
                    soup.find(["span", "div"], class_=re.compile(r"location|ort|place", re.IGNORECASE)),
                ]
                for el in loc_candidates:
                    txt = self._safe_text(el)
                    if txt and len(txt) > 1:
                        location = txt
                        break

                c["title"] = title_text
                c["company"] = company
                c["location"] = location if location else c["location"]

                time.sleep(0.5)

            except Exception:
                # Don't fail the whole run because one detail page differs
                continue

        return candidates

    @staticmethod
    def _safe_text(el) -> str:
        if not el:
            return ""
        try:
            return el.get_text(" ", strip=True)
        except Exception:
            return ""

    # ---------------------------
    # GENERAL helpers
    # ---------------------------
    def _wait_for_html(self):
        # Wait until the HTML is non-trivial (works across many sites)
        self.wait.until(lambda d: len(d.page_source) > 5000)
        # Allow JavaScript-rendered content to load
        time.sleep(2)

    @staticmethod
    def _clean_company(company: str) -> str:
        company = re.sub(r"\s+", " ", (company or "")).strip()

        # Remove common suffixes
        company = re.sub(
            r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?\s*$",
            "",
            company,
            flags=re.IGNORECASE,
        )

        # Remove bracketed notes
        company = re.sub(r"\s*\(.*?\)\s*", "", company)

        # Remove "hiring now" style tails
        company = re.sub(r"\s*(hiring now|we\'re hiring).*", "", company, flags=re.IGNORECASE)

        return company.strip() if company else "Unknown"

    @staticmethod
    def detect_technologies(title: str) -> list[str]:
        tech: list[str] = []
        text = (title or "").lower()

        if re.search(r"s[/]?4\s?hana|s4hana", text):
            tech.append("SAP S/4HANA")
        if "kyriba" in text:
            tech.append("Kyriba")
        if re.search(r"\bpython\b", text):
            tech.append("Python")
        if re.search(r"\bapi\b", text):
            tech.append("API")
        if "swift" in text:
            tech.append("SWIFT")
        if "power bi" in text or "powerbi" in text:
            tech.append("Power BI")

        return tech

    def save_to_csv(self, filename: str = "treasury_jobs.csv"):
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        # Company names to exclude (Unknown or misparsed UI labels)
        invalid_companies = {"unknown", "place of work", "last month", "yesterday"}

        df = pd.DataFrame(self.jobs)

        if "company" in df.columns:
            before_drop = len(df)
            company_clean = df["company"].fillna("").astype(str).str.strip().str.lower()
            df = df[~company_clean.isin(invalid_companies)]
            dropped = before_drop - len(df)
            if dropped:
                print(f"\n🗑️  Excluded {dropped} job(s) with invalid company name (Unknown, Place of work, Last month, Yesterday)")

        print("\n🔍 Detecting technologies...")
        df["technologies"] = df["title"].apply(lambda x: ", ".join(self.detect_technologies(x)))

        # Best unique key is the URL
        if "url" not in df.columns:
            df["url"] = ""

        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)
            print(f"   Current database: {len(existing_df)} jobs")

            # Remove existing rows with invalid company names from the CSV
            if "company" in existing_df.columns:
                before_drop = len(existing_df)
                company_clean = existing_df["company"].fillna("").astype(str).str.strip().str.lower()
                existing_df = existing_df[~company_clean.isin(invalid_companies)]
                removed = before_drop - len(existing_df)
                if removed:
                    print(f"   Removed {removed} existing row(s) with invalid company name")

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            combined_df.drop_duplicates(subset=["url"], keep="last", inplace=True)
            after_count = len(combined_df)
            removed = before_count - after_count

            combined_df.to_csv(filename, index=False)

            print(f"\n📊 Statistics:")
            print(f"   New jobs scraped: {len(df)}")
            print(f"   Duplicates removed: {removed}")
            print(f"   Total in database: {after_count}")
            print(f"   Net new jobs: {len(df) - removed}")
        else:
            print(f"\n📝 Creating new file: {filename}")
            df.drop_duplicates(subset=["url"], keep="last", inplace=True)
            df.to_csv(filename, index=False)
            print(f"   Saved {len(df)} jobs")

        print("\n✅ Save complete!")

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass
        print("\n🔒 Browser closed")


def main():
    print("\n" + "=" * 60)
    print("🏦 TREASURY WEB SCRAPER")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    scraper = TreasuryWebScraper()

    try:
        scraper.scrape_stepstone_de()
        scraper.scrape_jobs_ch()
        scraper.save_to_csv("treasury_jobs.csv")

        print("\n" + "=" * 60)
        print("✅ WEB SCRAPING COMPLETE!")
        print(f"📊 Total jobs collected this run: {len(scraper.jobs)}")
        print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        scraper.close()


if __name__ == "__main__":
    main()
