"""
Email Job Alert Parser
Parses LinkedIn job alert emails from Gmail
"""

import os
import base64
import re
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

class EmailJobParser:
    def __init__(self):
        self.service = self._get_gmail_service()
        self.jobs = []
    
    def _get_gmail_service(self):
        """Authenticate with Gmail API"""
        print("🔐 Authenticating with Gmail...")
        
        creds = None
        
        # Decode credentials from environment (GitHub Actions)
        if os.environ.get('GMAIL_CREDENTIALS'):
            print("   Using credentials from environment")
            creds_data = base64.b64decode(os.environ['GMAIL_CREDENTIALS'])
            with open('credentials.json', 'wb') as f:
                f.write(creds_data)
        
        if os.environ.get('GMAIL_TOKEN'):
            print("   Using token from environment")
            token_data = base64.b64decode(os.environ['GMAIL_TOKEN'])
            with open('token.json', 'wb') as f:
                f.write(token_data)
        
        # Load token if exists
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
        # If no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists('credentials.json'):
                    print("❌ credentials.json not found!")
                    print("   Run locally first to authenticate")
                    return None
                    
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save token
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        
        print("✅ Gmail authentication successful\n")
        return build('gmail', 'v1', credentials=creds)
    
    def get_linkedin_emails(self, days_back=7):
        """Fetch LinkedIn job alert emails"""
        print("=" * 60)
        print("📧 FETCHING LINKEDIN JOB ALERT EMAILS")
        print("=" * 60)
        
        if not self.service:
            print("❌ Gmail service not initialized")
            return []
        
        date_str = (datetime.now() - timedelta(days=days_back)).strftime('%Y/%m/%d')
        
        # Gmail query for LinkedIn job alerts
        query = f'from:(jobalerts-noreply@linkedin.com) after:{date_str} subject:(new jobs)'
        
        print(f"\n🔍 Searching for emails...")
        print(f"   From: jobalerts-noreply@linkedin.com")
        print(f"   Date range: Last {days_back} days")
        
        try:
            results = self.service.users().messages().list(
                userId='me',
                q=query,
                maxResults=50
            ).execute()
            
            messages = results.get('messages', [])
            print(f"\n✅ Found {len(messages)} LinkedIn job alert emails\n")
            
            return messages
            
        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            return []
    
    def parse_linkedin_email(self, msg_id):
        """Parse individual LinkedIn email"""
        try:
            # Get full message
            message = self.service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()
            
            # Get subject
            headers = message['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
            
            # Get email body
            body = self._get_email_body(message)
            
            if not body:
                return []
            
            # Parse HTML
            soup = BeautifulSoup(body, 'html.parser')
            
            jobs = []
            
            # LinkedIn emails contain job links
            # Pattern: linkedin.com/jobs/view/XXXXXXX
            job_links = soup.find_all('a', href=re.compile(r'linkedin\.com/jobs/view/\d+'))
            
            print(f"   Found {len(job_links)} job links in email")
            
            for link in job_links:
                try:
                    url = link.get('href', '')
                    
                    # Clean URL (remove tracking)
                    url = url.split('?')[0]
                    
                    # Find parent container with job info
                    parent = link.find_parent(['div', 'td', 'table', 'tr'])
                    
                    if not parent:
                        parent = link.find_parent()
                    
                    # Extract title (usually in the link text)
                    title = link.get_text(strip=True)
                    
                    # If title is empty or too short, look for nearby heading
                    if not title or len(title) < 10:
                        heading = parent.find(['h2', 'h3', 'h4', 'strong'])
                        if heading:
                            title = heading.get_text(strip=True)
                    
                    # Extract company name
                    # LinkedIn emails usually have company after "at"
                    company = "Unknown"
                    parent_text = parent.get_text()
                    
                    # Try pattern: "at CompanyName"
                    company_match = re.search(r'\bat\s+([A-Z][A-Za-z0-9\s&\.-]+?)(?:\s+in|\s+•|\n|$)', parent_text)
                    if company_match:
                        company = company_match.group(1).strip()
                    
                    # Extract location
                    # Pattern: "in City, Country" or just city name
                    location = "Germany"
                    location_match = re.search(
                        r'\bin\s+(Munich|Frankfurt|Berlin|Hamburg|Stuttgart|Düsseldorf|Zurich|Basel|Geneva|Vienna|Cologne|Dortmund|[A-Z][a-z]+)',
                        parent_text,
                        re.IGNORECASE
                    )
                    if location_match:
                        location = location_match.group(1)
                    
                    # Only add if we have valid data
                    if title != "Unknown" and len(title) > 10 and company != "Unknown":
                        jobs.append({
                            'title': title,
                            'company': self._clean_company(company),
                            'location': location,
                            'url': url,
                            'source': 'LinkedIn',
                            'date_scraped': datetime.now().strftime('%Y-%m-%d')
                        })
                
                except Exception as e:
                    print(f"   ⚠️  Error parsing job link: {str(e)[:50]}")
                    continue
            
            return jobs
            
        except Exception as e:
            print(f"   ❌ Error parsing email: {e}")
            return []
    
    def _get_email_body(self, message):
        """Extract HTML body from email"""
        try:
            # Check for parts (multipart email)
            if 'parts' in message['payload']:
                for part in message['payload']['parts']:
                    if part['mimeType'] == 'text/html':
                        if 'data' in part['body']:
                            data = part['body']['data']
                            return base64.urlsafe_b64decode(data).decode('utf-8')
            
            # Fallback: direct body
            if 'body' in message['payload'] and 'data' in message['payload']['body']:
                data = message['payload']['body']['data']
                return base64.urlsafe_b64decode(data).decode('utf-8')
            
        except Exception as e:
            print(f"   ⚠️  Error extracting body: {e}")
        
        return ""
    
    def _clean_company(self, company):
        """Clean company name"""
        company = re.sub(r'\s+', ' ', company).strip()
        company = re.sub(r'\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$', '', company, flags=re.IGNORECASE)
        return company.strip()
    
    def detect_technologies(self, title):
        """Detect technologies in title"""
        tech = []
        text = title.lower()
        
        if re.search(r's[/]?4\s?hana', text):
            tech.append('SAP S/4HANA')
        if 'kyriba' in text:
            tech.append('Kyriba')
        if 'python' in text:
            tech.append('Python')
        if 'api' in text:
            tech.append('API')
        if 'swift' in text:
            tech.append('SWIFT')
        
        return tech
    
    def process_all_emails(self, days_back=7):
        """Main processing function"""
        print("\n" + "=" * 60)
        print("📧 EMAIL JOB ALERT PARSER")
        print("=" * 60)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")
        
        # Get emails
        messages = self.get_linkedin_emails(days_back)
        
        if not messages:
            print("⚠️  No LinkedIn emails found")
            print("   This is normal if:")
            print("   1. You just set up job alerts (wait 24 hours)")
            print("   2. No new jobs match your criteria")
            return
        
        # Parse each email
        print("🔍 Parsing emails...\n")
        
        for i, msg in enumerate(messages, 1):
            print(f"📧 Email {i}/{len(messages)}")
            jobs = self.parse_linkedin_email(msg['id'])
            
            if jobs:
                self.jobs.extend(jobs)
                print(f"   ✅ Extracted {len(jobs)} jobs")
            else:
                print(f"   ⚠️  No jobs extracted")
        
        # Remove duplicates
        if self.jobs:
            df = pd.DataFrame(self.jobs)
            before = len(df)
            df.drop_duplicates(subset=['title', 'company', 'location'], keep='first', inplace=True)
            after = len(df)
            
            # Add technology detection
            df['technologies'] = df['title'].apply(
                lambda x: ', '.join(self.detect_technologies(x))
            )
            
            self.jobs = df.to_dict('records')
            
            print(f"\n📊 Statistics:")
            print(f"   Total extracted: {before}")
            print(f"   Duplicates removed: {before - after}")
            print(f"   Unique jobs: {after}")
        else:
            print("\n⚠️  No jobs extracted from emails")
    
    def save_to_csv(self, filename="treasury_jobs.csv"):
        """Save to CSV and merge with existing data"""
        print("\n" + "=" * 60)
        print("💾 SAVING EMAIL DATA")
        print("=" * 60)
        
        if not self.jobs:
            print("\n⚠️  No jobs to save from emails")
            return
        
        df = pd.DataFrame(self.jobs)
        print(f"\n📧 LinkedIn email jobs: {len(df)}")
        
        # Merge with existing data
        if os.path.exists(filename):
            print(f"📂 Found existing file: {filename}")
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
            
            print(f"\n📊 Final Statistics:")
            print(f"   LinkedIn jobs: {len(df)}")
            print(f"   Duplicates removed: {removed}")
            print(f"   Total in database: {after_count}")
        else:
            df.to_csv(filename, index=False)
            print(f"\n✅ Created new file with {len(df)} LinkedIn jobs")
        
        print("\n✅ Save complete!")

def main():
    parser = EmailJobParser()
    
    # Process emails from last 7 days
    parser.process_all_emails(days_back=7)
    
    # Save to CSV (merges with web-scraped data)
    parser.save_to_csv("treasury_jobs.csv")
    
    print("\n" + "=" * 60)
    print("✅ EMAIL PARSING COMPLETE!")
    print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
