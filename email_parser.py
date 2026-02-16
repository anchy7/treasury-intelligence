"""
Email Job Alert Parser
Parses LinkedIn job alert emails from Gmail and extracts job title/company/location/country.
Designed for GitHub Actions (env secrets) and local runs.
"""

import os
import base64
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from bs4 import BeautifulSoup
import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

LINKEDIN_SENDER = "jobalerts-noreply@linkedin.com"

# LinkedIn often uses /comm/jobs/view/<id> in emails
JOB_URL_RE = re.compile(r"https?://(www\.)?linkedin\.com/(comm/)?jobs/view/\d+", re.IGNORECASE)

# Lines / labels we should ignore when trying to interpret company/location
NOISE_PHRASES = {
    "this company is actively hiring",
    "promoted",
    "hiring now",
    "actively hiring",
    "see more jobs",
    "view job",
    "easy apply",
    "apply",
    "school alumni",
    "connections",
}

COUNTRY_HINTS = {
    "germany": "Germany",
    "switzerland": "Switzerland",
    "austria": "Austria",
    "deutschland": "Germany",
    "schweiz": "Switzerland",
    "österreich": "Austria",
}


class EmailJobParser:
    def __init__(self):
        self.service = self._get_gmail_service()
        self.jobs: List[Dict] = []

    def _get_gmail_service(self):
        """Authenticate with Gmail API (GitHub Actions secrets or local flow)."""
        print("🔐 Authenticating with Gmail...")

        creds = None

        # 1) GitHub Actions: write credentials.json/token.json from env vars if provided
        if os.environ.get("GMAIL_CREDENTIALS"):
            print("   Using credentials from environment (GMAIL_CREDENTIALS)")
            creds_data = base64.b64decode(os.environ["GMAIL_CREDENTIALS"])
            with open("credentials.json", "wb") as f:
                f.write(creds_data)

        if os.environ.get("GMAIL_TOKEN"):
            print("   Using token from environment (GMAIL_TOKEN)")
            token_data = base64.b64decode(os.environ["GMAIL_TOKEN"])
            with open("token.json", "wb") as f:
                f.write(token_data)

        # 2) Load token if exists
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)

        # 3) Refresh or run local flow if needed
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("   Refreshing expired token...")
                creds.refresh(Request())
            else:
                if not os.path.exists("credentials.json"):
                    print("❌ credentials.json not found!")
                    print("   If running locally: download OAuth client JSON and name it credentials.json")
                    print("   If running in GitHub Actions: set secret GMAIL_CREDENTIALS (base64 of credentials.json)")
                    return None

                print("   Running local OAuth flow (first-time setup)...")
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)

            # Save token for next time
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        print("✅ Gmail authentication successful\n")
        return build("gmail", "v1", credentials=creds)

    def get_linkedin_emails(self, days_back: int = 7):
        """Fetch LinkedIn job alert emails (no subject filter)."""
        print("=" * 60)
        print("📧 FETCHING LINKEDIN JOB ALERT EMAILS")
        print("=" * 60)

        if not self.service:
            print("❌ Gmail service not initialized")
            return []

        # Gmail "after:" accepts unix timestamp seconds in many cases; but date YYYY/MM/DD is OK too.
        date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y/%m/%d")
        query = f'from:({LINKEDIN_SENDER}) after:{date_str}'

        print(f"\n🔍 Searching Gmail...")
        print(f"   Query: {query}")

        try:
            results = self.service.users().messages().list(
                userId="me",
                q=query,
                maxResults=100
            ).execute()

            messages = results.get("messages", [])
            print(f"\n✅ Found {len(messages)} LinkedIn emails\n")
            return messages

        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            return []

    def _get_header(self, message: dict, header_name: str) -> str:
        headers = message.get("payload", {}).get("headers", [])
        return next((h["value"] for h in headers if h.get("name", "").lower() == header_name.lower()), "")

    def _get_email_body_html(self, message: dict) -> str:
        """Extract HTML body from Gmail message payload."""
        try:
            payload = message.get("payload", {})

            def decode_part(part) -> Optional[str]:
                body = part.get("body", {})
                data = body.get("data")
                if not data:
                    return None
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")

            # multipart
            if "parts" in payload:
                stack = list(payload["parts"])
                while stack:
                    part = stack.pop(0)
                    if part.get("mimeType") == "text/html":
                        html = decode_part(part)
                        if html:
                            return html
                    # nested multipart
                    if "parts" in part:
                        stack.extend(part["parts"])

            # single-part fallback
            if payload.get("mimeType") == "text/html":
                html = decode_part(payload)
                if html:
                    return html

            # sometimes html is inside body even if mimetype not explicit
            body = payload.get("body", {})
            if body.get("data"):
                return base64.urlsafe_b64decode(body["data"]).decode("utf-8", errors="replace")

        except Exception as e:
            print(f"   ⚠️  Error extracting body: {e}")

        return ""

    def _infer_country_from_subject(self, subject: str) -> Optional[str]:
        s = (subject or "").lower()
        # typical: "... Treasury Manager in Germany ..."
        for key, country in COUNTRY_HINTS.items():
            if f"in {key}" in s or key in s:
                return country
        return None

    def _clean_company(self, company: str) -> str:
        company = re.sub(r"\s+", " ", (company or "")).strip()
        company = re.sub(r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$", "", company, flags=re.IGNORECASE).strip()
        # remove noise suffixes
        if company.lower() in NOISE_PHRASES:
            return ""
        return company

    def _is_noise_line(self, line: str) -> bool:
        l = (line or "").strip().lower()
        if not l:
            return True
        if l in NOISE_PHRASES:
            return True
        # generic numeric-only / alumni lines
        if re.fullmatch(r"\d+\s+.*", l) and ("alumni" in l or "connections" in l):
            return True
        return False

    def _parse_company_and_location_from_line(self, line: str) -> Tuple[str, str, str]:
        """
        LinkedIn line often: "LBBW · Stuttgart, Baden-Württemberg, Germany"
        or: "Keystone Recruitment · EMEA" (no country)
        Returns (company, location, country)
        """
        raw = (line or "").strip()
        parts = [p.strip() for p in raw.split("·") if p.strip()]

        company = ""
        location = ""
        country = ""

        if len(parts) >= 1:
            company = self._clean_company(parts[0])

        if len(parts) >= 2:
            location = parts[1].strip()

            # If location looks like "... , Country", take last token as country
            if "," in location:
                last = location.split(",")[-1].strip()
                # basic sanity: avoid tiny tokens
                if len(last) >= 3:
                    country = last

        return company, location, country

    def _extract_jobs_from_html(self, html: str, fallback_country: Optional[str]) -> List[Dict]:
        """
        Extract job cards from LinkedIn email HTML by:
        - finding all job view links
        - for each unique URL, selecting the richest parent block text
        - parsing:
            line1 = title
            line2 = company · location, country
        """
        soup = BeautifulSoup(html, "html.parser")

        # collect candidates grouped by URL
        candidates: Dict[str, List[str]] = {}

        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = JOB_URL_RE.search(href)
            if not m:
                continue

            url = m.group(0).split("?")[0]  # remove tracking

            # choose a reasonable container
            container = a.find_parent(["tr", "td", "div", "table"]) or a.parent
            if not container:
                continue

            text = container.get_text("\n", strip=True)
            if not text or len(text) < 10:
                continue

            candidates.setdefault(url, []).append(text)

        jobs: List[Dict] = []

        for url, texts in candidates.items():
            # pick the "best" block = most lines & longest text
            best = max(texts, key=lambda t: (t.count("\n"), len(t)))
            lines = [ln.strip() for ln in best.split("\n") if ln.strip()]

            # remove obvious noise lines
            lines = [ln for ln in lines if not self._is_noise_line(ln)]

            if not lines:
                continue

            # title = first non-noise line
            title = lines[0].strip()

            # company/location usually in the next line
            company = ""
            location = ""
            country = ""

            if len(lines) >= 2:
                c, loc, ctry = self._parse_company_and_location_from_line(lines[1])
                company, location, country = c, loc, ctry

            # if company still empty, try scanning next few lines
            if not company:
                for ln in lines[1:5]:
                    c, loc, ctry = self._parse_company_and_location_from_line(ln)
                    if c:
                        company = c
                        if not location:
                            location = loc
                        if not country:
                            country = ctry
                        break

            # country fallback
            if not country and fallback_country:
                country = fallback_country

            # location fallback if missing
            if not location:
                location = country or "Unknown"

            # title sanity
            if len(title) < 5:
                continue

            jobs.append(
                {
                    "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                    "source": "LinkedIn",
                    "company": company or "Unknown",
                    "title": title,
                    "location": location,
                    "country": country or "Unknown",
                    "url": url,
                }
            )

        # de-dupe within one email
        if jobs:
            tmp = pd.DataFrame(jobs)
            tmp.drop_duplicates(subset=["title", "company", "location", "url"], inplace=True)
            jobs = tmp.to_dict("records")

        return jobs

    def detect_technologies(self, title: str) -> List[str]:
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

    def process_all_emails(self, days_back: int = 7):
        print("\n" + "=" * 60)
        print("📧 EMAIL JOB ALERT PARSER")
        print("=" * 60)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")

        messages = self.get_linkedin_emails(days_back)
        if not messages:
            print("⚠️  No LinkedIn emails found")
            return

        print("🔍 Parsing emails...\n")

        for i, msg in enumerate(messages, 1):
            try:
                message = self.service.users().messages().get(
                    userId="me",
                    id=msg["id"],
                    format="full",
                ).execute()

                subject = self._get_header(message, "Subject")
                sender = self._get_header(message, "From")
                date_hdr = self._get_header(message, "Date")

                print(f"📧 Email {i}/{len(messages)} (id={msg['id']})")
                print(f"   ✉️  Subject: {subject}")
                print(f"   👤 From: {sender}")
                print(f"   🗓️  Date: {date_hdr}")

                fallback_country = self._infer_country_from_subject(subject)

                html = self._get_email_body_html(message)
                if not html:
                    print("   ⚠️  No HTML body found")
                    continue

                extracted = self._extract_jobs_from_html(html, fallback_country=fallback_country)
                print(f"   🔗 Extracted {len(extracted)} jobs")

                self.jobs.extend(extracted)

            except Exception as e:
                print(f"   ❌ Error parsing email: {e}")

        if not self.jobs:
            print("\n⚠️  No jobs extracted from LinkedIn emails")
            return

        df = pd.DataFrame(self.jobs)

        # Technology detection
        df["technologies"] = df["title"].apply(lambda x: ", ".join(self.detect_technologies(str(x))))

        # remove duplicates across all emails
        before = len(df)
        df.drop_duplicates(subset=["source", "company", "title", "location", "url"], keep="first", inplace=True)
        after = len(df)

        self.jobs = df.to_dict("records")

        print(f"\n📊 Statistics:")
        print(f"   Extracted: {before}")
        print(f"   Duplicates removed: {before - after}")
        print(f"   Unique jobs: {after}")

    def save_to_csv(self, filename: str = "treasury_jobs.csv"):
        print("\n" + "=" * 60)
        print("💾 SAVING EMAIL DATA")
        print("=" * 60)

        if not self.jobs:
            print("⚠️  No jobs to save from emails")
            return

        df = pd.DataFrame(self.jobs)

        # Ensure columns exist consistently
        for col in ["date_scraped", "source", "company", "title", "location", "country", "url", "technologies"]:
            if col not in df.columns:
                df[col] = ""

        if os.path.exists(filename):
            print(f"📂 Found existing file: {filename}")
            existing = pd.read_csv(filename)

            combined = pd.concat([existing, df], ignore_index=True)

            # keep last occurrence
            combined.drop_duplicates(subset=["source", "company", "title", "location", "url"], keep="last", inplace=True)

            combined.to_csv(filename, index=False)
            print(f"✅ Updated {filename}: now {len(combined)} total rows")
        else:
            df.to_csv(filename, index=False)
            print(f"✅ Created {filename} with {len(df)} rows")


def main():
    parser = EmailJobParser()
    parser.process_all_emails(days_back=7)
    parser.save_to_csv("treasury_jobs.csv")

    print("\n" + "=" * 60)
    print("✅ EMAIL PARSING COMPLETE!")
    print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
