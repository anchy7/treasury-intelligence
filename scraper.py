"""
Web Scraper for StepStone.de and Jobs.ch
Runs daily via GitHub Actions
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
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
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
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
            ("Liquidity", "Frankfurt")
        ]
        
        for keyword, location in searches:
            print(f"\n🔍 Searching: '{keyword}' in {location}")
            
            try:
                # Build URL
                keyword_clean = keyword.replace(" ", "-").lower()
                url = f"https://www.stepstone.de/jobs/{keyword_clean}?location={location}"
                
                print(f"   URL: {url}")
                self.driver.get(url)
                time.sleep(4)
                
                # Scroll to load more jobs
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Parse HTML
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Find job cards - StepStone uses data-at="job-item"
                job_cards = soup.find_all('article', {'data-at': 'job-item'})
                
                if not job_cards:
                    # Fallback: look for any article with "job" in class
                    job_cards = soup.find_all('article', class_=re.compile('job', re.IGNORECASE))
                
                print(f"   Found {len(job_cards)} job cards")
                
                for i, card in enumerate(job_cards[:15], 1):  # Limit to 15 per search
                    try:
                        # Extract job title
                        title_elem = card.find('h2') or card.find('a', {'data-at': 'job-item-title'})
                        if not title_elem:
                            title_elem = card.find('a', class_=re.compile('title', re.IGNORECASE))
                        
                        title = title_elem.get_text(strip=True) if title_elem else "Unknown"
                        
                        # Extract company name
                        company_elem = card.find('span', {'data-at': 'job-item-company-name'})
                        if not company_elem:
                            company_elem = card.find('span', class_=re.compile('company', re.IGNORECASE))
                        
                        company = company_elem.get_text(strip=True) if company_elem else "Unknown"
                        company = self._clean_company(company)
                        
                        # Extract location
                        location_elem = card.find('span', {'data-at': 'job-item-location'})
                        if not location_elem:
                            location_elem = card.find('span', class_=re.compile('location', re.IGNORECASE))
                        
                        job_location = location_elem.get_text(strip=True) if location_elem else location
                        
                        # Extract URL
                        link_elem = card.find('a', href=True)
                        job_url = ""
                        if link_elem:
                            href = link_elem['href']
                            job_url = href if href.startswith('http') else f"https://www.stepstone.de{href}"
                        
                        # Only add if we have minimum data
                        if title != "Unknown" and company != "Unknown":
                            self.jobs.append({
                                'date_scraped': datetime.now().strftime('%Y-%m-%d'),
                                'source': 'StepStone.de',
                                'company': company,
                                'title': title,
                                'location': job_location,
                                'url': job_url
                            })
                            print(f"   ✅ [{i}] {company} - {title[:50]}")
                        
                    except Exception as e:
                        print(f"   ⚠️  Error parsing job card: {str(e)[:50]}")
                        continue
                
                time.sleep(2)  # Be nice to the server
                
            except Exception as e:
                print(f"   ❌ Error scraping StepStone: {e}")
        
        stepstone_count = len([j for j in self.jobs if j['source'] == 'StepStone.de'])
        print(f"\n✅ StepStone Total: {stepstone_count} jobs\n")
    
    def scrape_jobs_ch(self):
        """Scrape Jobs.ch for Swiss treasury jobs"""
        print("=" * 60)
        print("📊 SCRAPING JOBS.CH (Switzerland)")
        print("=" * 60)
        
        searches = [
            "Treasury",
            "Cash Manager",
            "Liquidität"
        ]
        
        for keyword in searches:
            print(f"\n🔍 Searching: '{keyword}'")
            
            try:
                # Jobs.ch URL format
                keyword_encoded = keyword.replace(" ", "%20")
                url = f"https://www.jobs.ch/en/vacancies/?term={keyword_encoded}"
                
                print(f"   URL: {url}")
                self.driver.get(url)
                time.sleep(4)
                
                # Scroll
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Parse HTML
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Find job listings - Jobs.ch uses various structures
                job_cards = soup.find_all('article', class_=re.compile('vacancy|job', re.IGNORECASE))
                
                if not job_cards:
                    # Try alternative: divs with job data
                    job_cards = soup.find_all('div', {'data-cy': re.compile('job|vacancy')})
                
                if not job_cards:
                    # Try finding all links with job URLs
                    job_links = soup.find_all('a', href=re.compile(r'/en/jobs/'))
                    # Create pseudo-cards from links
                    job_cards = [link.find_parent(['article', 'div', 'li']) for link in job_links]
                    job_cards = [card for card in job_cards if card]
                
                print(f"   Found {len(job_cards)} job cards")
                
                for i, card in enumerate(job_cards[:12], 1):  # Limit to 12 per search
                    try:
                        # Extract title
                        title_elem = card.find('h2') or card.find('h3')
                        if not title_elem:
                            title_elem = card.find('a', class_=re.compile('title', re.IGNORECASE))
                        
                        title = title_elem.get_text(strip=True) if title_elem else "Unknown"
                        
                        # Extract company
                        company_elem = card.find('span', class_=re.compile('company', re.IGNORECASE))
                        if not company_elem:
                            # Try looking for company in nearby text
                            text = card.get_text()
                            company_match = re.search(r'(?:at|bei)\s+([A-Z][A-Za-z\s&]+)', text)
                            company = company_match.group(1).strip() if company_match else "Unknown"
                        else:
                            company = company_elem.get_text(strip=True)
                        
                        company = self._clean_company(company)
                        
                        # Location - Jobs.ch is always Switzerland, but try to get city
                        location_elem = card.find('span', class_=re.compile('location', re.IGNORECASE))
                        if location_elem:
                            job_location = location_elem.get_text(strip=True)
                        else:
                            # Look for Swiss cities in text
                            text = card.get_text()
                            swiss_cities = ['Zurich', 'Basel', 'Geneva', 'Lausanne', 'Bern', 'Zug', 'Lucerne']
                            found_city = next((city for city in swiss_cities if city.lower() in text.lower()), None)
                            job_location = found_city if found_city else "Switzerland"
                        
                        # Extract URL
                        link_elem = card.find('a', href=re.compile(r'/en/jobs/'))
                        job_url = ""
                        if link_elem:
                            href = link_elem['href']
                            job_url = href if href.startswith('http') else f"https://www.jobs.ch{href}"
                        
                        # Only add if we have minimum data
                        if title != "Unknown" and len(title) > 5:
                            self.jobs.append({
                                'date_scraped': datetime.now().strftime('%Y-%m-%d'),
                                'source': 'Jobs.ch',
                                'company': company,
                                'title': title,
                                'location': job_location,
                                'url': job_url
                            })
                            print(f"   ✅ [{i}] {company} - {title[:50]}")
                        
                    except Exception as e:
                        print(f"   ⚠️  Error parsing job card: {str(e)[:50]}")
                        continue
                
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error scraping Jobs.ch: {e}")
        
        jobsch_count = len([j for j in self.jobs if j['source'] == 'Jobs.ch'])
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
