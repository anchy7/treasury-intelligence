"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions
"""

from selenium import webdriver
#from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
#from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import quote_plus
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
import os

class TreasuryWebScraper:
    def __init__(self):
        print("🚀 Initializing web scraper...")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        #service = Service(ChromeDriverManager().install())
        #self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.jobs = []
        print("✅ Web scraper ready\n")
    
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
            url = f"https://www.stepstone.de/jobs?what={quote_plus(keyword)}&where={quote_plus(location)}"
            print(f"   URL: {url}")

            self.driver.get(url)

            # Wait for page to load something meaningful
            WebDriverWait(self.driver, 15).until(
                lambda d: len(d.page_source) > 5000
            )

            # Optional: try to wait specifically for job items (may fail if blocked/consent)
            try:
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-at='job-item']"))
                )
            except:
                pass

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            job_cards = soup.select("article[data-at='job-item']")
            if not job_cards:
                job_cards = soup.find_all("article", class_=re.compile("job", re.IGNORECASE))

            print(f"   Found {len(job_cards)} job cards")

            for i, card in enumerate(job_cards[:15], 1):
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

                    if title != "Unknown" and company != "Unknown":
                        self.jobs.append({
                            "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                            "source": "StepStone.de",
                            "company": company,
                            "title": title,
                            "location": job_location,
                            "url": job_url,
                        })
                        print(f"   ✅ [{i}] {company} - {title[:50]}")

                except Exception as e:
                    print(f"   ⚠️  Error parsing job card: {str(e)[:80]}")
                    continue

            time.sleep(1)

        except Exception as e:
            print(f"   ❌ Error scraping StepStone: {e}")

    stepstone_count = len([j for j in self.jobs if j["source"] == "StepStone.de"])
    print(f"\n✅ StepStone Total: {stepstone_count} jobs\n")

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

            WebDriverWait(self.driver, 15).until(
                lambda d: len(d.page_source) > 5000
            )

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Grab job links (more stable than guessing card structure)
            links = soup.find_all("a", href=re.compile(r"(/en/vacancies/|/vacancies/|/en/jobs/)"))
            print(f"   Found {len(links)} job links")

            added = 0
            seen = set()

            for link in links:
                try:
                    title = link.get_text(strip=True)
                    if not title or len(title) < 6:
                        continue

                    href = link.get("href", "")
                    if not href:
                        continue

                    job_url = href if href.startswith("http") else f"https://www.jobs.ch{href}"

                    # Dedup inside one run
                    key = (title, job_url)
                    if key in seen:
                        continue
                    seen.add(key)

                    # Try to get company/location from nearby text (best-effort)
                    parent = link.find_parent(["article", "div", "li", "section"]) or link.parent
                    text = parent.get_text(" ", strip=True) if parent else ""

                    company = "Unknown"
                    m = re.search(r"\bat\s+(.+?)(?:\s+in\s+|•|\||$)", text, re.IGNORECASE)
                    if m:
                        company = self._clean_company(m.group(1).strip())

                    job_location = "Switzerland"
                    loc_elem = parent.find("span", class_=re.compile("location", re.IGNORECASE)) if parent else None
                    if loc_elem:
                        job_location = loc_elem.get_text(strip=True)

                    self.jobs.append({
                        "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                        "source": "Jobs.ch",
                        "company": company,
                        "title": title,
                        "location": job_location,
                        "url": job_url,
                    })
                    added += 1

                    if added <= 12:
                        print(f"   ✅ [{added}] {company} - {title[:50]}")

                    if added >= 30:  # keep it reasonable
                        break

                except Exception as e:
                    print(f"   ⚠️  Error parsing link: {str(e)[:80]}")
                    continue

            print(f"   Added {added} Jobs.ch jobs")

            time.sleep(1)

        except Exception as e:
            print(f"   ❌ Error scraping Jobs.ch: {e}")

    jobsch_count = len([j for j in self.jobs if j["source"] == "Jobs.ch"])
    print(f"\n✅ Jobs.ch Total: {jobsch_count} jobs\n")

    
    def _clean_company(self, company):
        """Clean and normalize company names"""
        # Remove extra whitespace
        company = re.sub(r'\s+', ' ', company).strip()
        
        # Remove common suffixes for normalization
        company = re.sub(r'\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$', '', company, flags=re.IGNORECASE)
        
        # Remove "Hiring now" and similar
        company = re.sub(r'\s*\(.*?\)\s*', '', company)
        company = re.sub(r'\s*(hiring now|we\'re hiring).*', '', company, flags=re.IGNORECASE)
        
        return company.strip()
    
    def detect_technologies(self, title):
        """Detect technologies mentioned in job title"""
        tech = []
        text = title.lower()
        
        if re.search(r's[/]?4\s?hana|s4hana', text):
            tech.append('SAP S/4HANA')
        if 'kyriba' in text:
            tech.append('Kyriba')
        if re.search(r'\bpython\b', text):
            tech.append('Python')
        if re.search(r'\bapi\b', text):
            tech.append('API')
        if 'swift' in text:
            tech.append('SWIFT')
        if 'power bi' in text or 'powerbi' in text:
            tech.append('Power BI')
        
        return tech
    
    def save_to_csv(self, filename="treasury_jobs.csv"):
        """Save scraped jobs to CSV"""
        print("=" * 60)
        print("💾 SAVING DATA")
        print("=" * 60)
        
        if not self.jobs:
            print("⚠️  No jobs to save!")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(self.jobs)
        
        # Add technology detection
        print("\n🔍 Detecting technologies...")
        df['technologies'] = df['title'].apply(
            lambda x: ', '.join(self.detect_technologies(x))
        )
        
        # Merge with existing data if file exists
        if os.path.exists(filename):
            print(f"\n📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)
            print(f"   Current database: {len(existing_df)} jobs")
            
            # Combine
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates
            before_count = len(combined_df)
            combined_df.drop_duplicates(
                subset=['company', 'title', 'location'],
                keep='last',
                inplace=True
            )
            after_count = len(combined_df)
            
            removed = before_count - after_count
            
            # Save
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
        """Close browser"""
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
        # Scrape StepStone (Germany)
        scraper.scrape_stepstone_de()
        
        # Scrape Jobs.ch (Switzerland)
        scraper.scrape_jobs_ch()
        
        # Save everything
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
