"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ----------------------------
# Helpers / Models
# ----------------------------

@dataclass
class Job:
    date_scraped: str
    source: str
    company: str
    title: str
    location: str
    country: str
    url: str
    technologies: str


def _now_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _sleep(min_s: float = 1.2, max_s: float = 2.2) -> None:
    # deterministic small jitter without random (keeps environments consistent)
    time.sleep((min_s + max_s) / 2)


def _clean_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _clean_company(company: str) -> str:
    company = _clean_whitespace(company)

    # Remove "Hiring now" / bracket noise
    company = re.sub(r"\s*\(.*?\)\s*", " ", company).strip()

    # Normalize common suffixes (optional)
    company = re.sub(
        r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$",
        "",
        company,
        flags=re.IGNORECASE,
    ).strip()

    return company or "Unknown"


def detect_technologies(title: str) -> str:
    tech: List[str] = []
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

    return ", ".join(tech)


# ----------------------------
# Main Scraper
# ----------------------------

class TreasuryWebScraper:
    def __init__(self) -> None:
        print("🚀 Initializing web scraper...")

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        )

        # Use webdriver_manager so GitHub Actions downloads matching chromedriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.jobs: List[Dict[str, Any]] = []
        print("✅ Web scraper ready\n")

    # ----------------------------
    # StepStone.de
    # ----------------------------

    def scrape_stepstone_de(self) -> None:
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
                keyword_clean = keyword.replace(" ", "-").lower()
                url = f"https://www.stepstone.de/jobs/{keyword_clean}?location={location}"
                print(f"   URL: {url}")

                self.driver.get(url)
                _sleep(3.5, 4.5)

                # Scroll once to load more
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                _sleep(1.8, 2.6)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                job_cards = soup.find_all("article", {"data-at": "job-item"})
                if not job_cards:
                    job_cards = soup.find_all("article")

                print(f"   Found {len(job_cards)} job cards")

                added = 0
                for card in job_cards[:20]:  # take up to 20 per query
                    title = self._stepstone_extract_title(card)
                    company = self._stepstone_extract_company(card)
                    loc = self._stepstone_extract_location(card) or location
                    job_url = self._stepstone_extract_url(card)

                    if title and company and title != "Unknown" and company != "Unknown":
                        self.jobs.append(
                            {
                                "date_scraped": _now_date(),
                                "source": "StepStone.de",
                                "company": _clean_company(company),
                                "title": _clean_whitespace(title),
                                "location": _clean_whitespace(loc),
                                "country": "Germany",
                                "url": job_url,
                            }
                        )
                        added += 1
                        print(f"   ✅ {company} - {title[:55]}")
                print(f"   ➕ Added: {added}")

                _sleep(1.5, 2.5)

            except Exception as e:
                print(f"   ❌ Error scraping StepStone: {e}")

        stepstone_count = sum(1 for j in self.jobs if j.get("source") == "StepStone.de")
        print(f"\n✅ StepStone Total: {stepstone_count} jobs\n")

    def _stepstone_extract_title(self, card: Any) -> str:
        title_elem = card.find("a", {"data-at": "job-item-title"})
        if not title_elem:
            h2 = card.find("h2")
            if h2:
                title_elem = h2
        if not title_elem:
            # fallback: first link that looks like a job link
            title_elem = card.find("a", href=True)
        return _clean_whitespace(title_elem.get_text(" ", strip=True)) if title_elem else "Unknown"

    def _stepstone_extract_company(self, card: Any) -> str:
        company_elem = card.find("span", {"data-at": "job-item-company-name"})
        if not company_elem:
            company_elem = card.find("span", class_=re.compile("company", re.I))
        return _clean_whitespace(company_elem.get_text(" ", strip=True)) if company_elem else "Unknown"

    def _stepstone_extract_location(self, card: Any) -> str:
        loc_elem = card.find("span", {"data-at": "job-item-location"})
        if not loc_elem:
            loc_elem = card.find("span", class_=re.compile("location", re.I))
        return _clean_whitespace(loc_elem.get_text(" ", strip=True)) if loc_elem else ""

    def _stepstone_extract_url(self, card: Any) -> str:
        link = card.find("a", href=True)
        if not link:
            return ""
        href = link["href"]
        return href if href.startswith("http") else f"https://www.stepstone.de{href}"

    # ----------------------------
    # Jobs.ch (Switzerland)
    # ----------------------------

    def scrape_jobs_ch(self) -> None:
        """
        Robust approach:
        1) Open search page
        2) Collect detail links (/en/vacancies/detail/<uuid>)
        3) Visit each detail link
        4) Parse JSON-LD JobPosting (title/company/location/country)
        """
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
                _sleep(3.5, 4.5)

                # Scroll a bit to load more results
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                _sleep(1.8, 2.6)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                links = self._jobs_ch_collect_detail_links(soup)
                print(f"   Found {len(links)} detail links")

                # Visit up to N links per keyword
                max_per_keyword = 12
                added = 0

                for job_url in list(links)[:max_per_keyword]:
                    job_data = self._jobs_ch_parse_detail(job_url)
                    if not job_data:
                        continue

                    company = _clean_company(job_data.get("company", "Unknown"))
                    title = _clean_whitespace(job_data.get("title", "Unknown"))
                    location = _clean_whitespace(job_data.get("location", "Switzerland"))
                    country = _clean_whitespace(job_data.get("country", "Switzerland"))

                    # guardrails
                    if title == "Unknown" or len(title) < 4:
                        continue
                    if company == "Unknown":
                        # keep it, but you can also skip if you prefer:
                        # continue
                        pass

                    self.jobs.append(
                        {
                            "date_scraped": _now_date(),
                            "source": "Jobs.ch",
                            "company": company,
                            "title": title,
                            "location": location,
                            "country": country,
                            "url": job_url,
                        }
                    )
                    added += 1
                    print(f"   ✅ {company} - {title[:55]}")

                    _sleep(1.1, 1.9)

                print(f"   ➕ Added: {added}")
                _sleep(1.2, 2.0)

            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        jobsch_count = sum(1 for j in self.jobs if j.get("source") == "Jobs.ch")
        print(f"\n✅ Jobs.ch Total: {jobsch_count} jobs\n")

    def _jobs_ch_collect_detail_links(self, soup: BeautifulSoup) -> List[str]:
        """
        Collect job detail links from result page.
        Typical format: /en/vacancies/detail/<uuid>/
        """
        anchors = soup.find_all("a", href=True)
        urls: Set[str] = set()

        for a in anchors:
            href = a.get("href", "")
            if not href:
                continue

            if re.search(r"^/en/vacancies/detail/[0-9a-f-]{10,}/?$", href, re.I):
                full = href if href.startswith("http") else f"https://www.jobs.ch{href}"
                urls.add(full)

        # if jobs.ch changes format, add more patterns here:
        # e.g. /de/stellenangebote/detail/<uuid>
        for a in anchors:
            href = a.get("href", "")
            if re.search(r"^/de/stellenangebote/detail/[0-9a-f-]{10,}/?$", href, re.I):
                full = href if href.startswith("http") else f"https://www.jobs.ch{href}"
                urls.add(full)

        return sorted(urls)

    def _jobs_ch_parse_detail(self, job_url: str) -> Optional[Dict[str, str]]:
        """
        Parse a Jobs.ch detail page using JSON-LD JobPosting if available.
        Fallback to HTML heuristics.
        """
        try:
            self.driver.get(job_url)
            _sleep(2.8, 3.6)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # 1) JSON-LD (best)
            data = self._extract_jobposting_jsonld(soup)
            if data:
                return data

            # 2) Fallback HTML
            title = "Unknown"
            h1 = soup.find("h1")
            if h1:
                title = _clean_whitespace(h1.get_text(" ", strip=True))

            # company often appears as a prominent link near the top, or under "About the company"
            company = "Unknown"
            about_company = soup.find(string=re.compile(r"About the company", re.I))
            if about_company:
                container = about_company.find_parent()
                if container:
                    a = container.find_next("a")
                    if a:
                        company = _clean_whitespace(a.get_text(" ", strip=True))

            if company == "Unknown":
                # try first "Save Apply" block company link
                a = soup.find("a", href=re.compile(r"/en/company|company", re.I))
                if a:
                    company = _clean_whitespace(a.get_text(" ", strip=True))

            # location fallback: swiss postal code + city
            text = soup.get_text("\n", strip=True)
            m = re.search(r"\b(\d{4}\s+[A-Za-zÀ-ÖØ-öø-ÿ' -]{2,})\b", text)
            location = _clean_whitespace(m.group(1)) if m else "Switzerland"

            return {
                "title": title,
                "company": company,
                "location": location,
                "country": "Switzerland",
            }

        except Exception:
            return None

    def _extract_jobposting_jsonld(self, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
        """
        Look for <script type="application/ld+json"> JobPosting
        and extract title, hiringOrganization, location (locality), country.
        """
        scripts = soup.find_all("script", {"type": "application/ld+json"})
        for s in scripts:
            raw = (s.string or "").strip()
            if not raw:
                continue

            # jobs.ch sometimes outputs multiple JSON objects or a list
            try:
                payload = json.loads(raw)
            except Exception:
                continue

            candidates: List[Dict[str, Any]] = []
            if isinstance(payload, dict):
                candidates = [payload]
            elif isinstance(payload, list):
                candidates = [p for p in payload if isinstance(p, dict)]

            for obj in candidates:
                if obj.get("@type") != "JobPosting":
                    # sometimes nested in @graph
                    if "@graph" in obj and isinstance(obj["@graph"], list):
                        for g in obj["@graph"]:
                            if isinstance(g, dict) and g.get("@type") == "JobPosting":
                                obj = g
                                break
                        else:
                            continue
                    else:
                        continue

                title = _clean_whitespace(obj.get("title", "")) or "Unknown"

                org = obj.get("hiringOrganization") or {}
                if isinstance(org, dict):
                    company = _clean_whitespace(org.get("name", "")) or "Unknown"
                else:
                    company = "Unknown"

                # location can be dict or list
                location = "Switzerland"
                country = "Switzerland"

                jl = obj.get("jobLocation")
                if isinstance(jl, list) and jl:
                    jl = jl[0]
                if isinstance(jl, dict):
                    addr = jl.get("address") or {}
                    if isinstance(addr, dict):
                        locality = addr.get("addressLocality") or ""
                        region = addr.get("addressRegion") or ""
                        postal = addr.get("postalCode") or ""
                        ctry = addr.get("addressCountry") or ""

                        loc_parts = [postal, locality, region]
                        location = _clean_whitespace(" ".join([p for p in loc_parts if p]))
                        if not location:
                            location = "Switzerland"

                        if isinstance(ctry, dict):
                            country = _clean_whitespace(ctry.get("name", "")) or "Switzerland"
                        else:
                            country = _clean_whitespace(str(ctry)) or "Switzerland"

                return {
                    "title": title,
                    "company": company,
                    "location": location,
                    "country": country,
                }

        return None

    # ----------------------------
    # Save
    # ----------------------------

    def save_to_csv(self, filename: str = "treasury_jobs.csv") -> None:
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        df = pd.DataFrame(self.jobs)

        # ensure consistent columns
        for col in ["date_scraped", "source", "company", "title", "location", "country", "url"]:
            if col not in df.columns:
                df[col] = ""

        print("\n🔍 Detecting technologies...")
        df["technologies"] = df["title"].apply(detect_technologies)

        # Merge with existing data
        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            combined_df.drop_duplicates(
                subset=["company", "title", "location", "country", "source"],
                keep="last",
                inplace=True,
            )
            after_count = len(combined_df)

            combined_df.to_csv(filename, index=False)

            print(f"\n📊 Statistics:")
            print(f"   New jobs scraped: {len(df)}")
            print(f"   Duplicates removed: {before_count - after_count}")
            print(f"   Total in database: {after_count}")
        else:
            print(f"\n📝 Creating new file: {filename}")
            df.to_csv(filename, index=False)
            print(f"   Saved {len(df)} jobs")

        print("\n✅ Save complete!")

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass
        print("\n🔒 Browser closed")


def main() -> None:
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
