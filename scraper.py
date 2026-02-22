"""
Treasury Web Scraper (cleanest approach)
- StepStone.de (Germany) + Jobs.ch (Switzerland)
- Uses Selenium Manager (no webdriver_manager)
- Saves/merges to treasury_jobs.csv
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ----------------------------
# Data model
# ----------------------------
@dataclass
class Job:
    date_scraped: str
    source: str
    country: str
    company: str
    title: str
    location: str
    url: str
    technologies: str = ""


# ----------------------------
# Scraper
# ----------------------------
class TreasuryWebScraper:
    def __init__(self, headless: bool = True):
        print("🚀 Initializing web scraper (Selenium Manager)...")

        chrome_options = Options()
        if headless:
            # "new" headless is more stable on modern Chrome
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # ✅ Cleanest approach: Selenium Manager auto-provisions correct driver
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 12)

        self.jobs: List[Job] = []
        print("✅ Web scraper ready\n")

    # -------- Utilities --------
    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass
        print("\n🔒 Browser closed")

    def _sleep_human(self, base: float = 1.5):
        time.sleep(base)

    def _clean_text(self, s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    def _clean_company(self, company: str) -> str:
        company = self._clean_text(company)
        company = re.sub(r"\s*\(.*?\)\s*", " ", company).strip()
        company = re.sub(
            r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA|S\.A\.|Sàrl|SARL)\.?$",
            "",
            company,
            flags=re.IGNORECASE,
        ).strip()
        return company

    def detect_technologies(self, text: str) -> str:
        t = (text or "").lower()
        tech = []
        if re.search(r"s[/]?\s*4\s*hana|s4hana", t):
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
        return ", ".join(tech)

    def _try_accept_cookies(self):
        """
        Best-effort cookie consent click for common patterns.
        Safe to call on every page.
        """
        candidates = [
            # German
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ', 'abcdefghijklmnopqrstuvwxyzäöü'), 'akzept')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'zustimmen')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'einverstanden')]",
            # English
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]",
        ]
        for xp in candidates:
            try:
                btn = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, xp))
                )
                btn.click()
                self._sleep_human(0.8)
                return
            except Exception:
                continue

    def _scroll_to_load(self, times: int = 2):
        for _ in range(times):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                pass
            self._sleep_human(1.2)

    # -------- StepStone --------
    def scrape_stepstone_de(self):
        print("=" * 60)
        print("📊 SCRAPING STEPSTONE.DE (Germany)")
        print("=" * 60)

        searches: List[Tuple[str, str]] = [
            ("Treasury", "Deutschland"),
            ("Cash Manager", "Deutschland"),
            ("Treasury", "München"),
            ("Liquidity", "Frankfurt"),
        ]

        for keyword, location in searches:
            print(f"\n🔍 Searching: '{keyword}' in {location}")
            keyword_clean = keyword.replace(" ", "-").lower()
            url = f"https://www.stepstone.de/jobs/{keyword_clean}?location={location}"
            print(f"   URL: {url}")

            try:
                self.driver.get(url)
                self._sleep_human(2.0)
                self._try_accept_cookies()

                # Wait for page to have content
                try:
                    self.wait.until(lambda d: len(d.page_source) > 5000)
                except TimeoutException:
                    pass

                self._scroll_to_load(times=2)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                job_cards = soup.find_all("article", {"data-at": "job-item"})
                if not job_cards:
                    job_cards = soup.find_all("article", class_=re.compile("job", re.IGNORECASE))

                print(f"   Found {len(job_cards)} job cards")

                added = 0
                for card in job_cards[:20]:
                    title = ""
                    company = ""
                    job_location = ""
                    job_url = ""

                    # Title
                    title_elem = (
                        card.find("a", {"data-at": "job-item-title"})
                        or card.find("h2")
                        or card.find("a", class_=re.compile("title", re.IGNORECASE))
                    )
                    title = self._clean_text(title_elem.get_text(" ", strip=True) if title_elem else "")

                    # Company
                    company_elem = (
                        card.find("span", {"data-at": "job-item-company-name"})
                        or card.find("span", class_=re.compile("company", re.IGNORECASE))
                    )
                    company = self._clean_company(company_elem.get_text(" ", strip=True) if company_elem else "")

                    # Location
                    loc_elem = (
                        card.find("span", {"data-at": "job-item-location"})
                        or card.find("span", class_=re.compile("location", re.IGNORECASE))
                    )
                    job_location = self._clean_text(loc_elem.get_text(" ", strip=True) if loc_elem else location)

                    # URL
                    link = card.find("a", href=True)
                    if link and link.get("href"):
                        href = link["href"]
                        job_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                    # Filter & add
                    if title and company and company.lower() != "unknown":
                        self.jobs.append(
                            Job(
                                date_scraped=datetime.now().strftime("%Y-%m-%d"),
                                source="StepStone.de",
                                country="Germany",
                                company=company,
                                title=title,
                                location=job_location,
                                url=job_url,
                                technologies=self.detect_technologies(title),
                            )
                        )
                        added += 1
                        if added <= 15:
                            print(f"   ✅ {company} - {title[:60]}")
                print(f"   ➕ Added {added} jobs")
                self._sleep_human(1.2)

            except WebDriverException as e:
                print(f"   ❌ Error scraping StepStone: {e}")

        count = sum(1 for j in self.jobs if j.source == "StepStone.de")
        print(f"\n✅ StepStone Total: {count} jobs\n")

    # -------- Jobs.ch --------
    def scrape_jobs_ch(self):
        print("=" * 60)
        print("📊 SCRAPING JOBS.CH (Switzerland)")
        print("=" * 60)

        searches = ["Treasury", "Cash Manager", "Liquidität"]

        for keyword in searches:
            print(f"\n🔍 Searching: '{keyword}'")

            try:
                keyword_encoded = keyword.replace(" ", "%20")
                url = f"https://www.jobs.ch/en/vacancies/?term={keyword_encoded}"
                print(f"   URL: {url}")

                self.driver.get(url)
                self._sleep_human(2.0)
                self._try_accept_cookies()

                try:
                    self.wait.until(lambda d: len(d.page_source) > 5000)
                except TimeoutException:
                    pass

                self._scroll_to_load(times=2)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # ✅ Jobs.ch often renders cards with links. We focus on job detail links.
                # Heuristics: href contains "/en/vacancies/detail/" OR "/en/jobs/" with an id-like tail.
                job_link_candidates = soup.find_all(
                    "a",
                    href=re.compile(r"(/en/vacancies/detail/|/en/jobs/)", re.IGNORECASE),
                )

                # Deduplicate by href
                seen_hrefs = set()
                job_links = []
                for a in job_link_candidates:
                    href = a.get("href", "")
                    if not href:
                        continue
                    if href in seen_hrefs:
                        continue
                    # skip navigation / search links that are not job detail pages
                    if "vacancies/?term=" in href:
                        continue
                    seen_hrefs.add(href)
                    job_links.append(a)

                print(f"   Found {len(job_links)} job links (candidates)")

                added = 0
                for a in job_links[:30]:
                    href = a.get("href", "")
                    job_url = href if href.startswith("http") else f"https://www.jobs.ch{href}"

                    # Find a "card" container near the link
                    card = (
                        a.find_parent("article")
                        or a.find_parent("li")
                        or a.find_parent("div")
                        or a.parent
                    )

                    # Title: prefer the link text itself, but clean it heavily
                    title = self._clean_text(a.get_text(" ", strip=True))

                    # Some links have generic text; fallback to first heading inside card
                    if not title or len(title) < 5 or "job offers" in title.lower():
                        h = card.find(["h2", "h3", "h4"]) if card else None
                        title = self._clean_text(h.get_text(" ", strip=True) if h else title)

                    # Company: on jobs.ch it is often in a dedicated element *below* title
                    company = ""
                    if card:
                        # Try common patterns first
                        company_elem = (
                            card.find(attrs={"data-cy": re.compile(r"company", re.IGNORECASE)})
                            or card.find("span", class_=re.compile(r"company|employer", re.IGNORECASE))
                            or card.find("div", class_=re.compile(r"company|employer", re.IGNORECASE))
                        )
                        if company_elem:
                            company = self._clean_company(company_elem.get_text(" ", strip=True))

                    # Fallback: extract from nearby text lines (often: title \n company \n location)
                    if not company and card:
                        text_lines = [
                            self._clean_text(x)
                            for x in card.get_text("\n", strip=True).split("\n")
                            if self._clean_text(x)
                        ]
                        # Remove noisy lines
                        noisy = {
                            "join our team",
                            "place of work:",
                            "workload:",
                            "apply",
                            "apply now",
                            "save",
                            "new",
                        }
                        filtered = [ln for ln in text_lines if ln.lower() not in noisy]

                        # Heuristic: company is often the first "proper name" line after title
                        # Find title line index, take next non-empty that isn't location/date/percent
                        t_idx = -1
                        for i, ln in enumerate(filtered[:10]):
                            if title and ln.lower() == title.lower():
                                t_idx = i
                                break
                        candidates = filtered[t_idx + 1 : t_idx + 5] if t_idx >= 0 else filtered[:6]

                        for ln in candidates:
                            if re.search(r"\b\d+%|\b\d+\s+days?\s+ago\b", ln.lower()):
                                continue
                            if re.search(r"\b(zurich|zürich|basel|geneva|genf|bern|zug|lausanne|lucerne|luzern)\b", ln.lower()):
                                continue
                            if len(ln) < 2:
                                continue
                            # looks like a company (contains letters, not mostly punctuation)
                            if re.search(r"[A-Za-zÄÖÜäöü]", ln) and not re.search(r"^\d+", ln):
                                company = self._clean_company(ln)
                                break

                    # Location: try common elements, else parse from text
                    location = ""
                    if card:
                        loc_elem = (
                            card.find(attrs={"data-cy": re.compile(r"location", re.IGNORECASE)})
                            or card.find("span", class_=re.compile(r"location|place", re.IGNORECASE))
                            or card.find("div", class_=re.compile(r"location|place", re.IGNORECASE))
                        )
                        if loc_elem:
                            location = self._clean_text(loc_elem.get_text(" ", strip=True))

                    if not location and card:
                        txt = card.get_text(" ", strip=True)
                        # try: Place of work: Geneva
                        m = re.search(r"Place of work:\s*([A-Za-zÄÖÜäöü\s\-]+)", txt, re.IGNORECASE)
                        if m:
                            location = self._clean_text(m.group(1))
                        else:
                            # Swiss cities fallback
                            swiss_cities = [
                                "Zürich", "Zurich", "Basel", "Geneva", "Genf", "Bern",
                                "Zug", "Lausanne", "Lucerne", "Luzern", "St. Gallen",
                            ]
                            found = next((c for c in swiss_cities if c.lower() in txt.lower()), None)
                            location = found or "Switzerland"

                    # Filters: prevent junk rows like "63 Cash Manager job offers"
                    if not title or len(title) < 6:
                        continue
                    if "job offer" in title.lower() or "job offers" in title.lower():
                        continue
                    if not company or company.lower() in {"unknown", "unbekannt"}:
                        continue

                    self.jobs.append(
                        Job(
                            date_scraped=datetime.now().strftime("%Y-%m-%d"),
                            source="Jobs.ch",
                            country="Switzerland",
                            company=company,
                            title=title,
                            location=location,
                            url=job_url,
                            technologies=self.detect_technologies(title),
                        )
                    )
                    added += 1
                    if added <= 15:
                        print(f"   ✅ {company} - {title[:60]}")

                print(f"   ➕ Added {added} jobs")
                self._sleep_human(1.2)

            except WebDriverException as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        count = sum(1 for j in self.jobs if j.source == "Jobs.ch")
        print(f"\n✅ Jobs.ch Total: {count} jobs\n")

    # -------- Save --------
    def save_to_csv(self, filename: str = "treasury_jobs.csv"):
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        df_new = pd.DataFrame([asdict(j) for j in self.jobs])

        # Normalize date
        df_new["date_scraped"] = pd.to_datetime(df_new["date_scraped"], errors="coerce").dt.strftime("%Y-%m-%d")

        # Merge with existing
        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            df_old = pd.read_csv(filename)

            # Ensure same columns exist
            for col in df_new.columns:
                if col not in df_old.columns:
                    df_old[col] = ""
            for col in df_old.columns:
                if col not in df_new.columns:
                    df_new[col] = ""

            combined = pd.concat([df_old, df_new], ignore_index=True)

            before = len(combined)
            combined.drop_duplicates(
                subset=["source", "company", "title", "location", "country"],
                keep="last",
                inplace=True,
            )
            after = len(combined)

            combined.to_csv(filename, index=False)
            print(f"\n📊 Statistics:")
            print(f"   New jobs scraped: {len(df_new)}")
            print(f"   Duplicates removed: {before - after}")
            print(f"   Total in database: {after}")
        else:
            df_new.to_csv(filename, index=False)
            print(f"\n📝 Created new file: {filename}")
            print(f"   Saved {len(df_new)} jobs")

        print("\n✅ Save complete!")


def main():
    print("\n" + "=" * 60)
    print("🏦 TREASURY WEB SCRAPER")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    scraper = TreasuryWebScraper(headless=True)

    try:
        scraper.scrape_stepstone_de()
        scraper.scrape_jobs_ch()
        scraper.save_to_csv("treasury_jobs.csv")

        print("\n" + "=" * 60)
        print("✅ WEB SCRAPING COMPLETE!")
        print(f"📊 Total jobs collected (this run): {len(scraper.jobs)}")
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
