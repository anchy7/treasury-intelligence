"""
Email Job Alert Parser (LinkedIn)
Parses LinkedIn job alert emails from Gmail (all emails from sender, no subject filtering)
"""

import os
import base64
import re
import quopri
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

LINKEDIN_SENDER = "jobalerts-noreply@linkedin.com"

# Accept both:
# - https://www.linkedin.com/jobs/view/123...
# - https://www.linkedin.com/comm/jobs/view/123...
JOB_URL_RE = re.compile(r"https?://(?:www\.)?linkedin\.com/(?:comm/)?jobs/view/\d+", re.IGNORECASE)

# Lines that often split job blocks in text/plain
SEPARATOR_RE = re.compile(r"^-{10,}\s*$")


class EmailJobParser:
    def __init__(self):
        self.service = self._get_gmail_service()
        self.jobs: List[Dict] = []

    def _get_gmail_service(self):
        """Authenticate with Gmail API"""
        print("🔐 Authenticating with Gmail...")

        creds = None

        # Optional: decode credentials/token from env (GitHub Actions)
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

        # Load token if exists
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)

        # If no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists("credentials.json"):
                    print("❌ credentials.json not found!")
                    print("   Provide it via repo/env secrets or run locally once to create token.json")
                    return None

                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)

            with open("token.json", "w") as token:
                token.write(creds.to_json())

        print("✅ Gmail authentication successful\n")
        return build("gmail", "v1", credentials=creds)

    def get_linkedin_emails(self, days_back: int = 7, max_results: int = 200) -> List[Dict]:
        """Fetch all LinkedIn job alert emails from sender (no subject filtering)."""
        print("=" * 60)
        print("📧 FETCHING LINKEDIN JOB ALERT EMAILS")
        print("=" * 60)

        if not self.service:
            print("❌ Gmail service not initialized")
            return []

        # Use newer_than to avoid date-format edge cases
        query = f"from:({LINKEDIN_SENDER}) newer_than:{days_back}d"

        print("\n🔍 Searching for emails...")
        print(f"   Query: {query}")

        messages: List[Dict] = []
        page_token = None

        try:
            while True:
                resp = (
                    self.service.users()
                    .messages()
                    .list(
                        userId="me",
                        q=query,
                        maxResults=min(100, max_results - len(messages)),
                        pageToken=page_token,
                    )
                    .execute()
                )

                messages.extend(resp.get("messages", []))
                page_token = resp.get("nextPageToken")

                if not page_token or len(messages) >= max_results:
                    break

            print(f"\n✅ Found {len(messages)} LinkedIn emails in last {days_back} days\n")
            return messages

        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            return []

    def parse_linkedin_email(self, msg_id: str) -> List[Dict]:
        """Parse a single LinkedIn email into job dicts."""
        try:
            message = (
                self.service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )

            headers = message.get("payload", {}).get("headers", [])
            subject = self._get_header(headers, "Subject") or ""
            from_ = self._get_header(headers, "From") or ""
            date_ = self._get_header(headers, "Date") or ""

            print(f"   ✉️  Subject: {subject}")
            print(f"   👤 From: {from_}")
            print(f"   🗓️  Date: {date_}")

            html_body, text_body = self._get_bodies(message)

            # 1) Try extracting URLs from text blocks (most reliable for your sample format)
            jobs_from_text = self._extract_jobs_from_text(text_body)

            # 2) Also extract URLs from HTML anchors (as fallback / additional coverage)
            jobs_from_html = self._extract_jobs_from_html(html_body)

            # Merge + de-duplicate (by url)
            combined = {j["url"]: j for j in (jobs_from_text + jobs_from_html)}.values()
            jobs = list(combined)

            print(f"   🔗 Extracted {len(jobs)} job(s)")

            # Enrich/normalize
            out = []
            for j in jobs:
                j["source"] = "LinkedIn"
                j["date_scraped"] = datetime.now().strftime("%Y-%m-%d")
                j["company"] = self._clean_company(j.get("company", "Unknown"))
                j["technologies"] = ", ".join(self.detect_technologies((j.get("title") or "") + " " + (j.get("company") or "")))
                out.append(j)

            return out

        except Exception as e:
            print(f"   ❌ Error parsing email: {e}")
            return []

    def _get_bodies(self, message) -> Tuple[str, str]:
        """Extract decoded HTML + text/plain bodies from Gmail message."""
        html = ""
        text = ""

        def decode_part_data(data_b64: str) -> str:
            raw = base64.urlsafe_b64decode(data_b64.encode("utf-8"))
            # Some parts are quoted-printable-like; quopri is safe to apply
            raw = quopri.decodestring(raw)
            try:
                return raw.decode("utf-8", errors="replace")
            except Exception:
                return raw.decode(errors="replace")

        payload = message.get("payload", {})

        # Recursive walk of MIME parts
        stack = [payload]
        while stack:
            part = stack.pop()
            mime = part.get("mimeType", "")
            body = part.get("body", {})
            data = body.get("data")

            if data:
                decoded = decode_part_data(data)
                if mime == "text/html" and not html:
                    html = decoded
                elif mime == "text/plain" and not text:
                    text = decoded

            for sub in part.get("parts", []) or []:
                stack.append(sub)

        return html or "", text or ""

    def _extract_jobs_from_text(self, text: str) -> List[Dict]:
        """Parse jobs from the text/plain format using 'View job:' blocks."""
        if not text:
            return []

        lines = [ln.strip() for ln in text.splitlines()]
        jobs: List[Dict] = []

        # Build blocks separated by dashed lines
        blocks: List[List[str]] = []
        current: List[str] = []
        for ln in lines:
            if SEPARATOR_RE.match(ln):
                if current:
                    blocks.append(current)
                    current = []
            else:
                # keep even empty lines to preserve proximity, but we'll ignore later
                current.append(ln)
        if current:
            blocks.append(current)

        # For each block, find "View job:" lines and infer title/company/location above it
        for block in blocks:
            for i, ln in enumerate(block):
                if ln.lower().startswith("view job:"):
                    # extract URL
                    m = JOB_URL_RE.search(ln)
                    if not m:
                        # sometimes URL wraps to next line(s)
                        joined = " ".join(block[i : min(i + 3, len(block))])
                        m = JOB_URL_RE.search(joined)
                    if not m:
                        continue

                    url = self._clean_url(m.group(0))

                    # look backwards for meaningful lines (title/company/location)
                    back = []
                    j = i - 1
                    while j >= 0 and len(back) < 6:
                        cand = block[j].strip()
                        if cand and not cand.lower().startswith("your job alert has been created"):
                            back.append(cand)
                        j -= 1
                    back = list(reversed(back))

                    title = back[-3] if len(back) >= 3 else (back[-1] if back else "Unknown")
                    company = back[-2] if len(back) >= 2 else "Unknown"
                    location = back[-1] if back else "Unknown"

                    # filter out noise lines that can appear between company/location
                    noise = {"apply with resume & profile", "view job:"}
                    if location.lower() in noise:
                        location = "Unknown"

                    jobs.append(
                        {
                            "title": title,
                            "company": company,
                            "location": location,
                            "url": url,
                        }
                    )

        # If blocks approach fails, do a simple URL-only extraction
        if not jobs:
            urls = set(JOB_URL_RE.findall(text))
            for u in urls:
                jobs.append({"title": "Unknown", "company": "Unknown", "location": "Unknown", "url": self._clean_url(u)})

        return jobs

    def _extract_jobs_from_html(self, html: str) -> List[Dict]:
        """Extract job URLs from HTML anchors (fallback)."""
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        jobs: List[Dict] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = JOB_URL_RE.search(href)
            if not m:
                continue
            url = self._clean_url(m.group(0))
            title = a.get_text(strip=True) or "Unknown"
            jobs.append({"title": title, "company": "Unknown", "location": "Unknown", "url": url})
        return jobs

    def _clean_url(self, url: str) -> str:
        """Strip tracking params."""
        return url.split("?")[0].strip()

    def _get_header(self, headers: List[Dict], name: str) -> Optional[str]:
        for h in headers:
            if h.get("name", "").lower() == name.lower():
                return h.get("value")
        return None

    def _clean_company(self, company: str) -> str:
        company = re.sub(r"\s+", " ", str(company)).strip()
        company = re.sub(r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$", "", company, flags=re.IGNORECASE)
        return company.strip()

    def detect_technologies(self, text: str) -> List[str]:
        tech = []
        t = (text or "").lower()

        if re.search(r"s[/]?4\s?hana|s4hana", t):
            tech.append("SAP S/4HANA")
        if "kyriba" in t:
            tech.append("Kyriba")
        if "python" in t:
            tech.append("Python")
        if re.search(r"\bapi\b", t):
            tech.append("API")
        if "swift" in t:
            tech.append("SWIFT")
        return tech

    def process_all_emails(self, days_back: int = 7):
        print("\n" + "=" * 60)
        print("📧 EMAIL JOB ALERT PARSER")
        print("=" * 60)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")

        messages = self.get_linkedin_emails(days_back)

        if not messages:
            print("⚠️  No LinkedIn emails found.")
            return

        print("🔍 Parsing emails...\n")
        for i, msg in enumerate(messages, 1):
            print(f"📧 Email {i}/{len(messages)} (id={msg['id']})")
            jobs = self.parse_linkedin_email(msg["id"])
            if jobs:
                self.jobs.extend(jobs)
            else:
                print("   ⚠️  No jobs extracted")

        # De-duplicate by URL
        if self.jobs:
            df = pd.DataFrame(self.jobs)
            before = len(df)
            df.drop_duplicates(subset=["url"], keep="first", inplace=True)
            after = len(df)
            self.jobs = df.to_dict("records")

            print(f"\n📊 Statistics:")
            print(f"   Total extracted: {before}")
            print(f"   Duplicates removed: {before - after}")
            print(f"   Unique jobs: {after}")
        else:
            print("\n⚠️  No jobs extracted from emails")

    def save_to_csv(self, filename: str = "treasury_jobs.csv"):
        print("\n" + "=" * 60)
        print("💾 SAVING EMAIL DATA")
        print("=" * 60)

        if not self.jobs:
            print("\n⚠️  No jobs to save from emails")
            return

        df = pd.DataFrame(self.jobs)
        print(f"\n📧 LinkedIn email jobs: {len(df)}")

        if os.path.exists(filename):
            print(f"📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            combined_df.drop_duplicates(subset=["company", "title", "location", "url"], keep="last", inplace=True)
            after_count = len(combined_df)

            combined_df.to_csv(filename, index=False)

            print(f"\n📊 Final Statistics:")
            print(f"   Duplicates removed: {before_count - after_count}")
            print(f"   Total in database: {after_count}")
        else:
            df.to_csv(filename, index=False)
            print(f"\n✅ Created new file with {len(df)} LinkedIn jobs")

        print("\n✅ Save complete!")


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
