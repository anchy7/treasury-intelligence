"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions (or locally)
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
import time
import re
import os


class TreasuryWebScraper:
    def __init__(self):
        print("🚀 Initializing web scraper...")

        chrome_options = Options()
        # Use the newer headless mode if available (works well on Linux runners)
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # A realistic UA (doesn't need to be Windows-specific)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        )

        # Selenium Manager (no webdriver-manager)
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
                # Use stable query params + URL encoding (important for München)
                url = (
                    "https://www.stepstone.de/jobs?"
                    f"what={quote_plus(keyword)}&where={quote_plus(location)}"
                )
                print(f"   URL: {url}")

                self.driver.get(url)

                # Wait for the page to load meaningful HTML
                self.wait.until(lambda d: len(d.page_source) > 5000)

                # Best effort: wait for job cards (may not appear if blocked/consent)
                try:
                    self.wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "article[data-at='job-item']")
                        )
                    )
                except Exception:
                    pass

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                job_cards = soup.select("article[data-at='job-item']")
                if not job_cards:
                    job_cards = soup.find_all("article", class_=re.compile("job", re.IGNORECASE))

                print(f"   Found {len(job_cards)} job cards")

                added = 0
                for card in job_cards[:20]:  # a bit more, then we cap prints
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
                        job_location = (
                            location_elem.get_text(strip=True) if location_elem else location
                        )

                        link_elem = card.find("a", href=True)
                        job_url = ""
                        if link_elem:
                            href = link_elem["href"]
                            job_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                        # Only add if minimum data exists
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
                        print(f"   ⚠️  Error parsing job card: {str(e)[:100]}")
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
        """Scrape Jobs.ch for Swiss treasury jobs (link-based extraction)"""
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
                self.wait.until(lambda d: len(d.page_source) > 5000)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # Jobs.ch patterns can change; we capture common vacancy links
                links = soup.find_all(
                    "a",
                    href=re.compile(r"(/en/vacancies/|/vacancies/|/en/jobs/|/jobs/)", re.IGNORECASE),
                )

                print(f"   Found {len(links)} job links")

                added = 0
                seen = set()

                for link in links:
                    try:
                        href = link.get("href", "")
                        if not href:
                            continue

                        # Only keep likely job detail links; avoid nav/footer noise
                        if not re.search(r"(vacanc|job)", href, re.IGNORECASE):
                            continue

                        title = link.get_text(strip=True)
                        if not title or len(title) < 6:
                            continue

                        job_url = href if href.startswith("http") else f"https://www.jobs.ch{href}"

                        key = ("Jobs.ch", job_url)
                        if key in seen:
                            continue
                        seen.add(key)

                        parent = link.find_parent(["article", "div", "li", "section"]) or link.parent
                        text = parent.get_text(" ", strip=True) if parent else ""

                        # Best-effort company extraction
                        company = "Unknown"
                        m = re.search(r"\bat\s+(.+?)(?:\s+in\s+|•|\||$)", text, re.IGNORECASE)
                        if m:
                            company = self._clean_company(m.group(1).strip())

                        # Best-effort location extraction
                        job_location = "Switzerland"
                        loc_elem = (
                            parent.find("span", class_=re.compile("location", re.IGNORECASE))
                            if parent
                            else None
                        )
                        if loc_elem:
                            job_location = loc_elem.get_text(strip=True)
                        else:
                            # fallback city guess
                            swiss_cities = [
                                "Zurich",
                                "Zürich",
                                "Basel",
                                "Geneva",
                                "Genève",
                                "Lausanne",
                                "Bern",
                                "Zug",
                                "Lucerne",
                                "Luzern",
                            ]
                            for city in swiss_cities:
                                if city.lower() in text.lower():
                                    job_location = city
                                    break

                        self.jobs.append(
                            {
                                "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                                "source": "Jobs.ch",
                                "company": company,
                                "title": title,
                                "location": job_location,
                                "url": job_url,
                            }
                        )

                        added += 1
                        if added <= 15:
                            print(f"   ✅ [{added}] {company} - {title[:55]}")

                        # Keep it reasonable per keyword
                        if added >= 25:
                            break

                    except Exception as e:
                        print(f"   ⚠️  Error parsing link: {str(e)[:100]}")
                        continue

                print(f"   Added {added} Jobs.ch jobs")
                time.sleep(1)

            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")

        jobsch_count = len([j for j in self.jobs if j["source"] == "Jobs.ch"])
        print(f"\n✅ Jobs.ch Total: {jobsch_count} jobs\n")

    # ---------------------------
    # HELPERS
    # ---------------------------
    def _clean_company(self, company: str) -> str:
        """Clean and normalize company names"""
        company = re.sub(r"\s+", " ", (company or "")).strip()
        company = re.sub(
            r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?\s*$", "", company, flags=re.IGNORECASE
        )
        company = re.sub(r"\s*\(.*?\)\s*", "", company)
        company = re.sub(r"\s*(hiring now|we\'re hiring).*", "", company, flags=re.IGNORECASE)
        return company.strip() if company else "Unknown"

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
        """Save scraped jobs to CSV (merge + dedupe)."""
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save!")
            return

        df = pd.DataFrame(self.jobs)

        print("\n🔍 Detecting technologies...")
        df["technologies"] = df["title"].apply(lambda x: ", ".join(self.detect_technologies(x)))

        # Make sure url exists; it's our best unique key
        if "url" not in df.columns:
            df["url"] = ""

        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)
            print(f"   Current database: {len(existing_df)} jobs")

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            # Dedupe by URL (most reliable across sources)
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
