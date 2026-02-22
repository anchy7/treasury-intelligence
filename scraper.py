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
import json
import time
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class TreasuryWebScraper:
    def __init__(self):
        print("🚀 Initializing web scraper...")

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # new headless is more stable
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Selenium Manager will fetch a compatible driver automatically
        self.driver = webdriver.Chrome(options=chrome_options)

        self.jobs: list[dict] = []
        print("✅ Web scraper ready\n")

    # -------------------------
    # STEPSTONE.DE
    # -------------------------
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
                # StepStone works better with what/where
                url = (
                    "https://www.stepstone.de/jobs?"
                    f"what={quote_plus(keyword)}&where={quote_plus(location)}"
                )
                print(f"   URL: {url}")

                self.driver.get(url)
                time.sleep(4)

                # gentle scroll
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                job_cards = soup.find_all("article", {"data-at": "job-item"})
                if not job_cards:
                    job_cards = soup.find_all("article", class_=re.compile("job", re.IGNORECASE))

                print(f"   Found {len(job_cards)} job cards")

                for i, card in enumerate(job_cards[:15], 1):
                    try:
                        title_elem = (
                            card.find("a", {"data-at": "job-item-title"})
                            or card.find("h2")
                            or card.find("a", class_=re.compile("title", re.IGNORECASE))
                        )
                        title = title_elem.get_text(" ", strip=True) if title_elem else "Unknown"

                        company_elem = (
                            card.find("span", {"data-at": "job-item-company-name"})
                            or card.find("span", class_=re.compile("company", re.IGNORECASE))
                        )
                        company = company_elem.get_text(" ", strip=True) if company_elem else "Unknown"
                        company = self._clean_company(company)

                        location_elem = (
                            card.find("span", {"data-at": "job-item-location"})
                            or card.find("span", class_=re.compile("location", re.IGNORECASE))
                        )
                        job_location = location_elem.get_text(" ", strip=True) if location_elem else location

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
                            print(f"   ✅ [{i}] {company} - {title[:60]}")

                    except Exception as e:
                        print(f"   ⚠️  Error parsing job card: {str(e)[:80]}")
                        continue

                time.sleep(1.5)

            except Exception as e:
                print(f"   ❌ Error scraping StepStone: {e}")

        stepstone_count = len([j for j in self.jobs if j["source"] == "StepStone.de"])
        print(f"\n✅ StepStone Total: {stepstone_count} jobs\n")

    # -------------------------
    # JOBS.CH
    # -------------------------
    def scrape_jobs_ch(self):
        """Scrape Jobs.ch for Swiss treasury jobs"""
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
                time.sleep(4)

                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # Jobs.ch pages change; most stable is: find job links, then build "cards" around them
                job_links = soup.find_all("a", href=re.compile(r"/en/jobs/\d+"))
                if not job_links:
                    job_links = soup.find_all("a", href=re.compile(r"/jobs/\d+"))

                cards = []
                for a in job_links:
                    parent = a.find_parent(["article", "li", "div"])
                    if parent:
                        cards.append(parent)

                # de-dup cards
                uniq = []
                seen = set()
                for c in cards:
                    key = id(c)
                    if key not in seen:
                        seen.add(key)
                        uniq.append(c)
                job_cards = uniq

                print(f"   Found {len(job_cards)} job cards")

                batch = []
                for i, card in enumerate(job_cards[:12], 1):
                    try:
                        # URL
                        link_elem = card.find("a", href=re.compile(r"/en/jobs/\d+")) or card.find(
                            "a", href=re.compile(r"/jobs/\d+")
                        )
                        job_url = ""
                        if link_elem and link_elem.get("href"):
                            href = link_elem["href"]
                            job_url = href if href.startswith("http") else f"https://www.jobs.ch{href}"

                        # Title
                        title = self._extract_title_jobs_ch(card)
                        # Company (often missing on list pages)
                        company = self._extract_company_jobs_ch(card)
                        # Location (sometimes present)
                        location = self._extract_location_jobs_ch(card) or "Switzerland"

                        if title != "Unknown" and job_url:
                            job_obj = {
                                "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                                "source": "Jobs.ch",
                                "company": self._clean_company(company) if company else "Unknown",
                                "title": title,
                                "location": location,
                                "url": job_url,
                                "country": "Switzerland",
                            }
                            batch.append(job_obj)
                            print(f"   ✅ [{i}] {job_obj['company']} - {title[:60]}")

                    except Exception as e:
                        print(f"   ⚠️  Error parsing job card: {str(e)[:80]}")
                        continue

                # Enrich missing companies from detail pages (JSON-LD preferred)
                batch = self._enrich_jobs_ch_from_detail_pages(batch, max_pages=8)

                # Append to global list
                self.jobs.extend(batch)

                time.sleep(1.5)

            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        jobsch_count = len([j for j in self.jobs if j["source"] == "Jobs.ch"])
        print(f"\n✅ Jobs.ch Total: {jobsch_count} jobs\n")

    # -------------------------
    # JOBS.CH helpers
    # -------------------------
    def _extract_title_jobs_ch(self, card):
        # titles can be in h2/h3 or link text
        title_elem = card.find("h2") or card.find("h3")
        if not title_elem:
            a = card.find("a", href=re.compile(r"/en/jobs/\d+")) or card.find("a", href=re.compile(r"/jobs/\d+"))
            if a:
                txt = a.get_text(" ", strip=True)
                if txt and len(txt) > 5:
                    return txt
        if title_elem:
            txt = title_elem.get_text(" ", strip=True)
            return txt if txt else "Unknown"
        return "Unknown"

    def _extract_company_jobs_ch(self, card):
        # List pages are unreliable. Try best-effort selectors.
        candidates = []

        # data attributes
        for el in card.select("[data-cy*=company], [data-testid*=company]"):
            txt = el.get_text(" ", strip=True)
            if txt:
                candidates.append(txt)

        # common classes
        for el in card.find_all(["span", "div"], class_=re.compile(r"company|employer|org", re.IGNORECASE)):
            txt = el.get_text(" ", strip=True)
            if txt:
                candidates.append(txt)

        # heuristic: line after title (in some layouts)
        title = self._extract_title_jobs_ch(card)
        if title and title != "Unknown":
            text_lines = [t.strip() for t in card.get_text("\n", strip=True).split("\n") if t.strip()]
            try:
                idx = text_lines.index(title)
                if idx + 1 < len(text_lines):
                    nxt = text_lines[idx + 1]
                    if len(nxt) <= 80 and not re.search(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", nxt):
                        candidates.append(nxt)
            except ValueError:
                pass

        for c in candidates:
            c_clean = self._clean_company(c)
            if c_clean and c_clean.lower() not in ("unknown", "switzerland"):
                return c_clean

        return "Unknown"

    def _extract_location_jobs_ch(self, card):
        for el in card.select("[data-cy*=location], [data-testid*=location]"):
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt

        for el in card.find_all(["span", "div"], class_=re.compile(r"location|city|place", re.IGNORECASE)):
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt

        text = card.get_text(" ", strip=True)
        swiss_cities = ["Zurich", "Basel", "Geneva", "Lausanne", "Bern", "Zug", "Lucerne", "Winterthur", "St. Gallen"]
        found_city = next((city for city in swiss_cities if city.lower() in text.lower()), None)
        return found_city

    def _enrich_jobs_ch_from_detail_pages(self, jobs, max_pages: int = 6):
        """Jobs.ch list pages often omit the employer.
        For entries where company/title/location/country are missing, open a few detail pages
        and try to enrich from JSON-LD (preferred) and stable HTML selectors.

        max_pages: safety cap per run to avoid hammering Jobs.ch.
        """
        checked = 0

        for job in jobs:
            if checked >= max_pages:
                break

            needs_company = (job.get("company") in (None, "", "Unknown"))
            needs_title = (job.get("title") in (None, "", "Unknown"))
            needs_location = (job.get("location") in (None, "", "Unknown", "Switzerland"))
            needs_country = (job.get("country") in (None, "", "Unknown"))

            if not (needs_company or needs_title or needs_location or needs_country):
                continue

            url = job.get("url") or ""
            if not url:
                continue

            checked += 1

            try:
                self.driver.get(url)
                time.sleep(2.5)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # 1) JSON-LD (best)
                company_ld = title_ld = city_ld = country_ld = None
                for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
                    raw = (s.string or "").strip()
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue

                    candidates = data if isinstance(data, list) else [data]
                    for d in candidates:
                        if not isinstance(d, dict):
                            continue
                        t = str(d.get("@type", "")).lower()

                        if "jobposting" in t or ("title" in d and ("hiringOrganization" in d or "jobLocation" in d)):
                            if not title_ld and isinstance(d.get("title"), str):
                                title_ld = d.get("title")

                            ho = d.get("hiringOrganization")
                            if not company_ld:
                                if isinstance(ho, dict) and isinstance(ho.get("name"), str):
                                    company_ld = ho.get("name")
                                elif isinstance(ho, list):
                                    for org in ho:
                                        if isinstance(org, dict) and isinstance(org.get("name"), str):
                                            company_ld = org.get("name")
                                            break

                            jl = d.get("jobLocation")
                            jl_list = jl if isinstance(jl, list) else ([jl] if isinstance(jl, dict) else [])
                            if jl_list and (not city_ld or not country_ld):
                                addr = jl_list[0].get("address") if isinstance(jl_list[0], dict) else None
                                if isinstance(addr, dict):
                                    if not city_ld and isinstance(addr.get("addressLocality"), str):
                                        city_ld = addr.get("addressLocality")
                                    if not country_ld:
                                        c = addr.get("addressCountry")
                                        if isinstance(c, str):
                                            country_ld = c
                                        elif isinstance(c, dict) and isinstance(c.get("name"), str):
                                            country_ld = c.get("name")

                        if company_ld and (title_ld or not needs_title) and (city_ld or not needs_location):
                            break
                    if company_ld and (title_ld or not needs_title) and (city_ld or not needs_location):
                        break

                # 2) Fallback HTML selectors
                if needs_title and not title_ld:
                    h1 = soup.find("h1")
                    if h1:
                        title_ld = h1.get_text(" ", strip=True)

                if needs_company and not company_ld:
                    sels = [
                        "[data-cy*=company]",
                        "[data-testid*=company]",
                        "a[href*='/en/companies/']",
                        "a[href*='/companies/']",
                        "span[class*=company]",
                        "div[class*=company]",
                        "span[class*=employer]",
                        "div[class*=employer]",
                    ]
                    for sel in sels:
                        el = soup.select_one(sel)
                        if el:
                            txt = self._clean_company(el.get_text(" ", strip=True))
                            if txt and txt.lower() != "unknown":
                                company_ld = txt
                                break

                if needs_location and not city_ld:
                    for sel in ["[data-cy*=location]", "[data-testid*=location]", "span[class*=location]", "div[class*=location]"]:
                        el = soup.select_one(sel)
                        if el:
                            txt = el.get_text(" ", strip=True)
                            txt = re.sub(r"\s+", " ", txt).strip()
                            if txt:
                                city_ld = txt
                                break

                if needs_country and not country_ld:
                    country_ld = "Switzerland"

                if needs_title and title_ld:
                    job["title"] = title_ld.strip()
                if needs_company and company_ld:
                    job["company"] = self._clean_company(company_ld)
                if needs_location and city_ld:
                    job["location"] = city_ld.strip()
                if needs_country and country_ld:
                    cc = country_ld.strip()
                    if cc.upper() == "CH":
                        cc = "Switzerland"
                    job["country"] = cc

            except Exception:
                continue

        return jobs

    # -------------------------
    # Shared utilities
    # -------------------------
    def _clean_company(self, company: str):
        """Clean and normalize company names"""
        if not company:
            return "Unknown"

        company = re.sub(r"\s+", " ", str(company)).strip()
        company = re.sub(
            r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?\s*$",
            "",
            company,
            flags=re.IGNORECASE,
        )
        company = re.sub(r"\s*\(.*?\)\s*", "", company)
        company = re.sub(r"\s*(hiring now|we're hiring).*", "", company, flags=re.IGNORECASE)
        return company.strip() if company.strip() else "Unknown"

    def detect_technologies(self, title: str):
        tech = []
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

    def save_to_csv(self, filename="treasury_jobs.csv"):
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        df = pd.DataFrame(self.jobs)

        # Add technology detection
        df["technologies"] = df["title"].apply(lambda x: ", ".join(self.detect_technologies(x)))

        # Merge if exists
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, df], ignore_index=True)

            # Deduplicate by URL first, then by company/title/location
            if "url" in combined_df.columns:
                combined_df.drop_duplicates(subset=["url"], keep="last", inplace=True)
            combined_df.drop_duplicates(subset=["company", "title", "location"], keep="last", inplace=True)

            combined_df.to_csv(filename, index=False)
            print(f"✅ Updated {filename} | Total rows: {len(combined_df)}")
        else:
            df.to_csv(filename, index=False)
            print(f"✅ Created {filename} | Rows: {len(df)}")

    def close(self):
        self.driver.quit()
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
        print(f"📊 Total jobs collected: {len(scraper.jobs)}")
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
