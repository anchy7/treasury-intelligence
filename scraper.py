"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions

Key fix:
- Jobs.ch: collect detail links from search results, open each detail page,
  parse JSON-LD JobPosting to reliably get title/company/location/country.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ----------------------------
# Small helpers
# ----------------------------

def now_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_company(company: str) -> str:
    company = clean_ws(company)

    # remove bracket noise
    company = re.sub(r"\s*\(.*?\)\s*", " ", company).strip()

    # normalize suffixes (optional)
    company = re.sub(
        r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$",
        "",
        company,
        flags=re.IGNORECASE,
    ).strip()

    return company if company else "Unknown"


def detect_technologies(title: str) -> str:
    tech: List[str] = []
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

    return ", ".join(tech)


def polite_sleep(seconds: float) -> None:
    time.sleep(seconds)


# ----------------------------
# Scraper
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

        # webdriver-manager ensures matching chromedriver in GitHub Actions
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.jobs: List[Dict[str, Any]] = []
        print("✅ Web scraper ready\n")

    # ----------------------------
    # StepStone.de
    # ----------------------------

    def scrape_stepstone_de(self) -> None:
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
                keyword_clean = keyword.replace(" ", "-").lower()
                url = f"https://www.stepstone.de/jobs/{keyword_clean}?location={location}"
                print(f"   URL: {url}")

                self.driver.get(url)
                polite_sleep(4)

                # scroll once to load more
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                polite_sleep(2)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                job_cards = soup.find_all("article", {"data-at": "job-item"})
                if not job_cards:
                    job_cards = soup.find_all("article")

                print(f"   Found {len(job_cards)} job cards")

                added = 0
                for card in job_cards[:20]:
                    title = self._stepstone_extract_title(card)
                    company = self._stepstone_extract_company(card)
                    loc = self._stepstone_extract_location(card) or location
                    job_url = self._stepstone_extract_url(card)

                    if title != "Unknown" and company != "Unknown":
                        self.jobs.append(
                            {
                                "date_scraped": now_date(),
                                "source": "StepStone.de",
                                "company": clean_company(company),
                                "title": clean_ws(title),
                                "location": clean_ws(loc),
                                "country": "Germany",
                                "url": job_url,
                            }
                        )
                        added += 1
                        print(f"   ✅ {company} - {title[:55]}")

                print(f"   ➕ Added: {added}")
                polite_sleep(2)

            except Exception as e:
                print(f"   ❌ Error scraping StepStone: {e}")

        total = sum(1 for j in self.jobs if j.get("source") == "StepStone.de")
        print(f"\n✅ StepStone Total: {total} jobs\n")

    def _stepstone_extract_title(self, card: Any) -> str:
        a = card.find("a", {"data-at": "job-item-title"})
        if not a:
            h2 = card.find("h2")
            if h2:
                a = h2
        if not a:
            a = card.find("a", href=True)
        return clean_ws(a.get_text(" ", strip=True)) if a else "Unknown"

    def _stepstone_extract_company(self, card: Any) -> str:
        s = card.find("span", {"data-at": "job-item-company-name"})
        if not s:
            s = card.find("span", class_=re.compile("company", re.I))
        return clean_ws(s.get_text(" ", strip=True)) if s else "Unknown"

    def _stepstone_extract_location(self, card: Any) -> str:
        s = card.find("span", {"data-at": "job-item-location"})
        if not s:
            s = card.find("span", class_=re.compile("location", re.I))
        return clean_ws(s.get_text(" ", strip=True)) if s else ""

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
        """Scrape Jobs.ch for Swiss treasury jobs via detail pages + JSON-LD"""
        print("=" * 60)
        print("📊 SCRAPING JOBS.CH (Switzerland)")
        print("=" * 60)

        searches = ["Treasury", "Cash Manager", "Liquidität"]

        for keyword in searches:
            print(f"\n🔍 Searching: '{keyword}'")

            try:
                term = keyword.replace(" ", "%20")
                url = f"https://www.jobs.ch/en/vacancies/?term={term}"
                print(f"   URL: {url}")

                self.driver.get(url)
                polite_sleep(4)

                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                polite_sleep(2)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                detail_links = self._jobs_ch_collect_detail_links(soup)

                print(f"   Found {len(detail_links)} detail links")

                max_per_keyword = 12
                added = 0

                for job_url in detail_links[:max_per_keyword]:
                    data = self._jobs_ch_parse_detail(job_url)
                    if not data:
                        continue

                    title = clean_ws(data.get("title", "Unknown"))
                    company = clean_company(data.get("company", "Unknown"))
                    location = clean_ws(data.get("location", "Switzerland"))
                    country = clean_ws(data.get("country", "Switzerland"))

                    if title == "Unknown" or len(title) < 4:
                        continue

                    self.jobs.append(
                        {
                            "date_scraped": now_date(),
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
                    polite_sleep(1.5)

                print(f"   ➕ Added: {added}")
                polite_sleep(2)

            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        total = sum(1 for j in self.jobs if j.get("source") == "Jobs.ch")
        print(f"\n✅ Jobs.ch Total: {total} jobs\n")

    def _jobs_ch_collect_detail_links(self, soup: BeautifulSoup) -> List[str]:
        """
        Collect job detail links from results.
        Common patterns:
        - /en/vacancies/detail/<uuid>/
        - /de/stellenangebote/detail/<uuid>/
        """
        urls: Set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href:
                continue

            if re.search(r"^/en/vacancies/detail/[0-9a-f-]{10,}/?$", href, re.I):
                urls.add(href if href.startswith("http") else f"https://www.jobs.ch{href}")

            if re.search(r"^/de/stellenangebote/detail/[0-9a-f-]{10,}/?$", href, re.I):
                urls.add(href if href.startswith("http") else f"https://www.jobs.ch{href}")

        return sorted(urls)

    def _jobs_ch_parse_detail(self, job_url: str) -> Optional[Dict[str, str]]:
        """Open job detail page and parse JSON-LD JobPosting, fallback to HTML."""
        try:
            self.driver.get(job_url)
            polite_sleep(3)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Best: JSON-LD JobPosting
            data = self._extract_jobposting_jsonld(soup)
            if data:
                return data

            # Fallback HTML
            title = "Unknown"
            h1 = soup.find("h1")
            if h1:
                title = clean_ws(h1.get_text(" ", strip=True))

            company = "Unknown"
            # try meta tags
            og = soup.find("meta", {"property": "og:site_name"})
            if og and og.get("content"):
                # not always company, but keep as last resort
                pass

            # try something that looks like "Company" label nearby
            text = soup.get_text("\n", strip=True)
            m = re.search(r"\bCompany\b[:\s]+(.+)", text, re.I)
            if m:
                company = clean_ws(m.group(1))

            # Swiss location heuristic: "8000 Zürich"
            loc = "Switzerland"
            m2 = re.search(r"\b(\d{4}\s+[A-Za-zÀ-ÖØ-öø-ÿ' -]{2,})\b", text)
            if m2:
                loc = clean_ws(m2.group(1))

            return {"title": title, "company": company, "location": loc, "country": "Switzerland"}

        except Exception:
            return None

    def _extract_jobposting_jsonld(self, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
        """Extract JobPosting from JSON-LD."""
        scripts = soup.find_all("script", {"type": "application/ld+json"})
        for s in scripts:
            raw = (s.string or "").strip()
            if not raw:
                continue

            try:
                payload = json.loads(raw)
            except Exception:
                continue

            objs: List[Dict[str, Any]] = []
            if isinstance(payload, dict):
                objs = [payload]
            elif isinstance(payload, list):
                objs = [x for x in payload if isinstance(x, dict)]

            for obj in objs:
                # sometimes nested in @graph
                if obj.get("@type") != "JobPosting":
                    g = obj.get("@graph")
                    if isinstance(g, list):
                        jp = next((x for x in g if isinstance(x, dict) and x.get("@type") == "JobPosting"), None)
                        if jp:
                            obj = jp
                        else:
                            continue
                    else:
                        continue

                title = clean_ws(obj.get("title", "")) or "Unknown"

                org = obj.get("hiringOrganization") or {}
                company = "Unknown"
                if isinstance(org, dict):
                    company = clean_ws(org.get("name", "")) or "Unknown"

                location = "Switzerland"
                country = "Switzerland"

                jl = obj.get("jobLocation")
                if isinstance(jl, list) and jl:
                    jl = jl[0]
                if isinstance(jl, dict):
                    addr = jl.get("address") or {}
                    if isinstance(addr, dict):
                        postal = clean_ws(str(addr.get("postalCode", "") or ""))
                        locality = clean_ws(str(addr.get("addressLocality", "") or ""))
                        region = clean_ws(str(addr.get("addressRegion", "") or ""))
                        location = clean_ws(" ".join([x for x in [postal, locality, region] if x])) or "Switzerland"

                        ctry = addr.get("addressCountry") or ""
                        if isinstance(ctry, dict):
                            country = clean_ws(ctry.get("name", "")) or "Switzerland"
                        else:
                            country = clean_ws(str(ctry)) or "Switzerland"

                return {"title": title, "company": company, "location": location, "country": country}

        return None

    # ----------------------------
    # Save
    # ----------------------------

    def save_to_csv(self, filename: str = "treasury_jobs.csv") -> None:
        """Save scraped jobs to CSV; merge with existing file; dedupe."""
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        df = pd.DataFrame(self.jobs)

        # ensure required columns exist
        for col in ["date_scraped", "source", "company", "title", "location", "country", "url"]:
            if col not in df.columns:
                df[col] = ""

        print("\n🔍 Detecting technologies...")
        df["technologies"] = df["title"].apply(detect_technologies)

        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)
            print(f"   Current database: {len(existing_df)} jobs")

            combined = pd.concat([existing_df, df], ignore_index=True)

            before = len(combined)
            combined.drop_duplicates(
                subset=["source", "company", "title", "location", "country"],
                keep="last",
                inplace=True,
            )
            after = len(combined)

            combined.to_csv(filename, index=False)

            print(f"\n📊 Statistics:")
            print(f"   New jobs scraped: {len(df)}")
            print(f"   Duplicates removed: {before - after}")
            print(f"   Total in database: {after}")
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
