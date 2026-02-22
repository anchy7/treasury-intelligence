"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions (or locally)

Output: treasury_jobs.csv
"""

from __future__ import annotations

import os
import re
import time
import json
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
        chrome_options.add_argument("--headless=new")  # newer headless
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )

        # If your runner has a correct chromedriver in PATH this is enough.
        # In GitHub Actions, ensure you remove /usr/bin/chromedriver (old) and install matching driver.
        self.driver = webdriver.Chrome(options=chrome_options)

        self.jobs: list[dict] = []
        print("✅ Web scraper ready\n")

    # ---------------------------------------------------------------------
    # StepStone
    # ---------------------------------------------------------------------
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
                keyword_clean = keyword.replace(" ", "-").lower()
                url = f"https://www.stepstone.de/jobs/{keyword_clean}?location={quote_plus(location)}"

                print(f"   URL: {url}")
                self.driver.get(url)
                time.sleep(4)

                # Scroll once to trigger lazy loading
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                job_cards = soup.find_all("article", {"data-at": "job-item"})
                if not job_cards:
                    job_cards = soup.find_all("article", class_=re.compile("job", re.IGNORECASE))

                print(f"   Found {len(job_cards)} job cards")

                added = 0
                for card in job_cards[:15]:
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

                        if title != "Unknown" and company != "Unknown":
                            self.jobs.append(
                                {
                                    "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                                    "source": "StepStone.de",
                                    "company": company,
                                    "title": title,
                                    "location": job_location,
                                    "url": job_url,
                                    "country": "Germany",
                                }
                            )
                            added += 1
                            print(f"   ✅ [{added}] {company} - {title[:60]}")
                    except Exception as e:
                        print(f"   ⚠️  Error parsing StepStone card: {str(e)[:120]}")
                        continue

                time.sleep(1.5)

            except Exception as e:
                print(f"   ❌ Error scraping StepStone: {e}")

        stepstone_count = len([j for j in self.jobs if j["source"] == "StepStone.de"])
        print(f"\n✅ StepStone Total: {stepstone_count} jobs\n")

    # ---------------------------------------------------------------------
    # Jobs.ch (Switzerland) - FIXED: avoid "whole card text" titles + enrich company
    # ---------------------------------------------------------------------
    def scrape_jobs_ch(self):
        """Scrape Jobs.ch for Swiss treasury jobs (robust title + company extraction)"""
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

                # Only accept real job detail links: /en/jobs/<digits>/...
                job_links = soup.find_all("a", href=re.compile(r"^/en/jobs/\d+/?"))
                if not job_links:
                    job_links = soup.find_all("a", href=re.compile(r"^/jobs/\d+/?"))

                # De-duplicate by href
                seen_hrefs = set()
                unique_links = []
                for a in job_links:
                    href = a.get("href")
                    if not href:
                        continue
                    if href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)
                    unique_links.append(a)

                print(f"   Found {len(unique_links)} job links")

                batch = []
                added = 0
                for a in unique_links[:12]:
                    href = a.get("href", "")
                    job_url = href if href.startswith("http") else f"https://www.jobs.ch{href}"

                    # Title: aria-label / heading inside link (never full card text)
                    title = self._extract_title_jobs_ch_from_link(a)
                    if self._looks_like_not_a_job_title(title):
                        continue

                    # Company/location from list tile if present (often missing)
                    company = self._extract_company_jobs_ch_from_link_context(a)
                    location = self._extract_location_jobs_ch_from_link_context(a) or "Switzerland"

                    batch.append(
                        {
                            "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                            "source": "Jobs.ch",
                            "company": self._clean_company(company) if company else "Unknown",
                            "title": title,
                            "location": location,
                            "url": job_url,
                            "country": "Switzerland",
                        }
                    )
                    added += 1
                    print(f"   ✅ [{added}] {batch[-1]['company']} - {title[:60]}")

                # Enrich missing company/location from detail pages using JSON-LD
                batch = self._enrich_jobs_ch_from_detail_pages(batch, max_pages=12)

                self.jobs.extend(batch)
                time.sleep(1.5)

            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        jobsch_count = len([j for j in self.jobs if j["source"] == "Jobs.ch"])
        print(f"\n✅ Jobs.ch Total: {jobsch_count} jobs\n")

    def _looks_like_not_a_job_title(self, title: str) -> bool:
        if not title:
            return True
        t = title.strip().lower()
        if t in {"unknown", "zürich", "zurich", "switzerland"}:
            return True
        bad_phrases = [
            "job offers",
            "job offer",
            "join our team",
            "create job alert",
            "subscribe",
            "sign in",
            "log in",
        ]
        return any(p in t for p in bad_phrases) or len(t) < 8

    def _extract_title_jobs_ch_from_link(self, a_tag) -> str:
        """Prefer aria-label then a nested heading; never full card text."""
        aria = a_tag.get("aria-label")
        if aria and len(aria.strip()) > 5:
            return re.sub(r"\s+", " ", aria).strip()

        h = a_tag.find(["h1", "h2", "h3", "strong"])
        if h:
            txt = h.get_text(" ", strip=True)
            if txt:
                return txt

        txt = a_tag.get_text("\n", strip=True)
        if txt:
            first = txt.split("\n")[0].strip()
            if first and len(first) > 5:
                return first

        return "Unknown"

    def _extract_company_jobs_ch_from_link_context(self, a_tag) -> str:
        """Best-effort from list view; if missing, will be filled from detail page JSON-LD."""
        container = a_tag.find_parent(["article", "li", "div"]) or a_tag.parent
        if not container:
            return "Unknown"

        sels = [
            "[data-cy*=company]",
            "[data-testid*=company]",
            "span[class*=company]",
            "div[class*=company]",
            "span[class*=employer]",
            "div[class*=employer]",
            "a[href*='/en/companies/']",
            "a[href*='/companies/']",
        ]
        for sel in sels:
            el = container.select_one(sel)
            if el:
                txt = self._clean_company(el.get_text(" ", strip=True))
                if txt and txt.lower() != "unknown":
                    return txt

        return "Unknown"

    def _extract_location_jobs_ch_from_link_context(self, a_tag) -> str | None:
        container = a_tag.find_parent(["article", "li", "div"]) or a_tag.parent
        if not container:
            return None

        sels = [
            "[data-cy*=location]",
            "[data-testid*=location]",
            "span[class*=location]",
            "div[class*=location]",
            "span[class*=city]",
            "div[class*=city]",
        ]
        for sel in sels:
            el = container.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                txt = re.sub(r"\s+", " ", txt).strip()
                if txt:
                    return txt
        return None

    def _enrich_jobs_ch_from_detail_pages(self, batch: list[dict], max_pages: int = 12) -> list[dict]:
        """
        For Jobs.ch, company is often not available on the list page.
        This function opens job detail pages and tries to extract:
        - company: JSON-LD hiringOrganization.name (best)
        - location: jobLocation.address.addressLocality / addressRegion / addressCountry
        """
        enriched = []
        to_process = batch[:max_pages]

        for job in to_process:
            try:
                if job.get("company") != "Unknown" and job.get("location") not in {"Switzerland", "Unknown"}:
                    enriched.append(job)
                    continue

                self.driver.get(job["url"])
                time.sleep(2.5)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # JSON-LD extraction (most reliable)
                json_ld_blocks = soup.find_all("script", {"type": "application/ld+json"})
                found_company = None
                found_location = None

                for script in json_ld_blocks:
                    txt = script.get_text(strip=True)
                    if not txt:
                        continue
                    try:
                        data = json.loads(txt)
                    except Exception:
                        continue

                    # Sometimes it's a list of items
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict):
                            continue

                        # Identify a JobPosting
                        t = item.get("@type") or item.get("type")
                        if isinstance(t, list):
                            is_job = any(x.lower() == "jobposting" for x in t if isinstance(x, str))
                        else:
                            is_job = isinstance(t, str) and t.lower() == "jobposting"

                        if not is_job:
                            continue

                        # Company
                        hiring = item.get("hiringOrganization") or {}
                        if isinstance(hiring, dict):
                            name = hiring.get("name")
                            if isinstance(name, str) and name.strip():
                                found_company = name.strip()

                        # Location
                        job_loc = item.get("jobLocation")
                        # Can be dict or list
                        loc_items = job_loc if isinstance(job_loc, list) else ([job_loc] if isinstance(job_loc, dict) else [])
                        for li in loc_items:
                            if not isinstance(li, dict):
                                continue
                            addr = li.get("address") or {}
                            if isinstance(addr, dict):
                                city = addr.get("addressLocality")
                                region = addr.get("addressRegion")
                                country = addr.get("addressCountry")
                                parts = [p for p in [city, region, country] if isinstance(p, str) and p.strip()]
                                if parts:
                                    found_location = ", ".join(parts)
                                    break

                if found_company and job.get("company") == "Unknown":
                    job["company"] = self._clean_company(found_company)

                if found_location and (job.get("location") in {"Switzerland", "Unknown"}):
                    job["location"] = found_location

                enriched.append(job)

            except Exception as e:
                print(f"   ⚠️  Enrich failed for Jobs.ch detail page: {str(e)[:120]}")
                enriched.append(job)

        # Add any not processed due to max_pages
        if len(batch) > len(to_process):
            enriched.extend(batch[len(to_process):])

        return enriched

    # ---------------------------------------------------------------------
    # Common helpers
    # ---------------------------------------------------------------------
    def _clean_company(self, company: str) -> str:
        if not company:
            return "Unknown"

        company = re.sub(r"\s+", " ", company).strip()

        # Remove common suffixes (light normalization)
        company = re.sub(r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$", "", company, flags=re.IGNORECASE)

        # Remove bracketed stuff
        company = re.sub(r"\s*\(.*?\)\s*", " ", company)
        company = re.sub(r"\s+", " ", company).strip()

        return company if company else "Unknown"

    def detect_technologies(self, title: str) -> list[str]:
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

    def save_to_csv(self, filename: str = "treasury_jobs.csv"):
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        df = pd.DataFrame(self.jobs)

        # Add technology detection
        print("\n🔍 Detecting technologies...")
        df["technologies"] = df["title"].apply(lambda x: ", ".join(self.detect_technologies(x)))

        # Ensure columns exist
        for col in ["date_scraped", "source", "company", "title", "location", "url", "country", "technologies"]:
            if col not in df.columns:
                df[col] = ""

        # Merge with existing file
        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)
            print(f"   Current database: {len(existing_df)} jobs")

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            combined_df.drop_duplicates(
                subset=["source", "company", "title", "location"],
                keep="last",
                inplace=True,
            )
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
