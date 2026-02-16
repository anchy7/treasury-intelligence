"""
Email Job Alert Parser
Parses LinkedIn job alert emails from Gmail (ALL emails from sender, no subject filtering)
"""

import os
import base64
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from bs4 import BeautifulSoup
import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

LINKEDIN_SENDERS = [
    "jobalerts-noreply@linkedin.com",
    # keep these as optional fallbacks; comment out if you only want the one sender
    # "jobs-noreply@linkedin.com",
]


class EmailJobParser:
    def __init__(self):
        self.service = self._get_gmail_service()
        self.jobs: List[Dict] = []

    # -----------------------------
    # AUTH
    # -----------------------------
    def _get_gmail_service(self):
        """Authenticate with Gmail API. Prefer env vars (GitHub). Fallback to local auth."""
        print("🔐 Authenticating with Gmail...")

        creds: Optional[Credentials] = None

        # If GitHub Actions provides base64-encoded files, write them
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
            try:
                creds = Credentials.from_authorized_user_file("token.json", SCOPES)
            except Exception as e:
                print(f"⚠️  Could not read token.json: {e}")
                creds = None

        # If no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("   Refreshing expired token...")
                creds.refresh(Request())
            else:
                # Local-only flow
                if not os.path.exists("credentials.json"):
                    print("❌ credentials.json not found!")
                    print("   In GitHub: you should provide GMAIL_TOKEN (and optionally GMAIL_CREDENTIALS).")
                    print("   Locally: place credentials.json in project root to authenticate.")
                    return None

                print("   Running local OAuth flow...")
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)

            # Save token
            with open("token.json", "w", encoding="utf-8") as token:
                token.write(creds.to_json())

        print("✅ Gmail authentication successful\n")
        return build("gmail", "v1", credentials=creds)

    # -----------------------------
    # SEARCH EMAILS
    # -----------------------------
    def get_linkedin_emails(self, days_back=7, max_results=50):
        """Fetch ALL LinkedIn job alert emails from specified sender(s), no subject filtering."""
        print("=" * 60)
        print("📧 FETCHING LINKEDIN JOB ALERT EMAILS")
        print("=" * 60)

        if not self.service:
            print("❌ Gmail service not initialized")
            return []

        # Build sender OR query
        sender_query = " OR ".join([f"from:({s})" for s in LINKEDIN_SENDERS])
        query = f"in:anywhere ({sender_query}) newer_than:{days_back}d"

        print(f"\n🔍 Gmail query: {query}")
        print(f"   Date range: last {days_back} days")
        print(f"   Max results: {max_results}")

        try:
            results = self.service.users().messages().list(
                userId="me",
                q=query,
                maxResults=max_results,
            ).execute()

            messages = results.get("messages", [])
            print(f"\n✅ Found {len(messages)} LinkedIn emails\n")

            # Optional: print first few message IDs
            for m in messages[:5]:
                print(f"   • msg id: {m.get('id')}")

            return messages

        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            return []

    # -----------------------------
    # PARSING
    # -----------------------------
    def parse_linkedin_email(self, msg_id: str):
        """Parse individual LinkedIn email and extract job postings."""
        try:
            message = self.service.users().messages().get(
                userId="me",
                id=msg_id,
                format="full",
            ).execute()

            subject = self._get_header(message, "Subject") or ""
            from_header = self._get_header(message, "From") or ""
            date_header = self._get_header(message, "Date") or ""

            print(f"   ✉️  Subject: {subject[:90]}")
            print(f"   👤 From: {from_header[:90]}")
            if date_header:
                print(f"   🗓️  Date: {date_header[:90]}")

            body_html = self._get_email_body_html(message)
            if not body_html:
                print("   ⚠️  No HTML body found")
                return []

            soup = BeautifulSoup(body_html, "html.parser")

            # LinkedIn job links often look like linkedin.com/jobs/view/<id>
            job_links = soup.find_all("a", href=re.compile(r"linkedin\.com/jobs/view/\d+"))
            print(f"   🔗 Found {len(job_links)} job links")

            jobs = []

            for link in job_links:
                try:
                    url = link.get("href", "")
                    url = url.split("?")[0]  # remove tracking

                    # title guess
                    title = link.get_text(strip=True) or "Unknown"

                    # find a surrounding container
                    parent = link.find_parent(["tr", "td", "table", "div"]) or link.parent
                    parent_text = parent.get_text(" ", strip=True) if parent else ""

                    # company extraction: LinkedIn alerts often contain "at Company"
                    company = self._extract_company(parent_text)

                    # location extraction: best-effort
                    location = self._extract_location(parent_text) or "Unknown"

                    if title != "Unknown" and len(title) > 5:
                        jobs.append(
                            {
                                "title": title,
                                "company": self._clean_company(company) if company else "Unknown",
                                "location": location,
                                "url": url,
                                "source": "LinkedIn",
                                "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                            }
                        )
                except Exception as e:
                    print(f"   ⚠️  Error parsing job link: {str(e)[:120]}")
                    continue

            # Filter out unknown company entries (optional: keep them if you prefer)
            cleaned = []
            for j in jobs:
                if j["company"] != "Unknown":
                    cleaned.append(j)

            if len(cleaned) != len(jobs):
                print(f"   🧹 Filtered {len(jobs) - len(cleaned)} jobs with unknown company")

            return cleaned

        except Exception as e:
            print(f"   ❌ Error parsing email {msg_id}: {e}")
            return []

    def _get_header(self, message, name: str) -> Optional[str]:
        headers = message.get("payload", {}).get("headers", [])
        for h in headers:
            if h.get("name", "").lower() == name.lower():
                return h.get("value")
        return None

    def _get_email_body_html(self, message) -> str:
        """
        Extract HTML body from Gmail message payload.
        Handles nested multipart structures.
        """
        payload = message.get("payload", {})
        return self._find_part_recursive(payload)

    def _find_part_recursive(self, part: dict) -> str:
        # If this part is HTML and contains data, decode it
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data")

        if mime == "text/html" and data:
            try:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
            except Exception:
                return ""

        # If multipart, walk children
        for p in part.get("parts", []) or []:
            found = self._find_part_recursive(p)
            if found:
                return found

        # Some emails store body directly without parts
        if data:
            try:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
            except Exception:
                return ""

        return ""

    # -----------------------------
    # EXTRACTION HELPERS
    # -----------------------------
    def _extract_company(self, text: str) -> Optional[str]:
        if not text:
            return None

        # Try common patterns
        # "at Company" / "bei Company"
        m = re.search(r"\b(at|bei)\s+([A-Z][A-Za-z0-9\s&\.\-]+?)(?:\s+(in|auf|•|\||-)|$)", text)
        if m:
            return m.group(2).strip()

        # Sometimes "Company • Location"
        m2 = re.search(r"^([A-Z][A-Za-z0-9\s&\.\-]{2,})\s+•\s+", text)
        if m2:
            return m2.group(1).strip()

        return None

    def _extract_location(self, text: str) -> Optional[str]:
        if not text:
            return None

        # simple heuristics; extend as needed
        # City, Country or City (case-insensitive)
        cities = [
            "Zurich", "Zürich", "Basel", "Geneva", "Genf", "Bern", "Zug", "Lausanne",
            "Munich", "München", "Frankfurt", "Berlin", "Hamburg", "Stuttgart",
            "Düsseldorf", "Cologne", "Köln", "Vienna", "Wien"
        ]
        for c in cities:
            if re.search(rf"\b{re.escape(c)}\b", text, flags=re.IGNORECASE):
                return c

        # fallback: "in X"
        m = re.search(r"\bin\s+([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-\s]{2,})", text)
        if m:
            return m.group(1).strip()

        return None

    def _clean_company(self, company: str) -> str:
        if not company:
            return "Unknown"
        company = re.sub(r"\s+", " ", company).strip()
        company = re.sub(r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$", "", company, flags=re.IGNORECASE)
        return company.strip() or "Unknown"

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

        return tech

    # -----------------------------
    # PIPELINE
    # -----------------------------
    def process_all_emails(self, days_back=7, max_results=50):
        print("\n" + "=" * 60)
        print("📧 EMAIL JOB ALERT PARSER")
        print("=" * 60)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")

        messages = self.get_linkedin_emails(days_back=days_back, max_results=max_results)

        if not messages:
            print("⚠️  No LinkedIn emails found with current query.")
            print("   Tips:")
            print("   - Increase days_back (e.g., 14)")
            print("   - Verify sender in Gmail UI and adjust LINKEDIN_SENDERS")
            return

        print("\n🔍 Parsing emails...\n")

        for i, msg in enumerate(messages, 1):
            print(f"📧 Email {i}/{len(messages)} (id={msg['id']})")
            jobs = self.parse_linkedin_email(msg["id"])

            if jobs:
                self.jobs.extend(jobs)
                print(f"   ✅ Extracted {len(jobs)} jobs\n")
            else:
                print(f"   ⚠️  No jobs extracted\n")

        # De-duplicate
        if self.jobs:
            df = pd.DataFrame(self.jobs)
            before = len(df)
            df.drop_duplicates(subset=["title", "company", "location"], keep="first", inplace=True)

            # Add technology detection
            df["technologies"] = df["title"].apply(lambda x: ", ".join(self.detect_technologies(x)))

            after = len(df)
            self.jobs = df.to_dict("records")

            print(f"\n📊 Statistics:")
            print(f"   Total extracted: {before}")
            print(f"   Duplicates removed: {before - after}")
            print(f"   Unique jobs: {after}")
        else:
            print("\n⚠️  No jobs extracted from emails")

    def save_to_csv(self, filename="treasury_jobs.csv"):
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
            print(f"   Current database: {len(existing_df)} jobs")

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            combined_df.drop_duplicates(subset=["company", "title", "location"], keep="last", inplace=True)
            after_count = len(combined_df)
            removed = before_count - after_count

            combined_df.to_csv(filename, index=False)

            print(f"\n📊 Final Statistics:")
            print(f"   LinkedIn jobs added: {len(df)}")
            print(f"   Duplicates removed: {removed}")
            print(f"   Total in database: {after_count}")
        else:
            df.to_csv(filename, index=False)
            print(f"\n✅ Created new file with {len(df)} LinkedIn jobs")

        print("\n✅ Save complete!")


def main():
    parser = EmailJobParser()

    # Increase days_back if you want to be extra safe
    parser.process_all_emails(days_back=7, max_results=50)

    # Merge into the same CSV your scraper uses
    parser.save_to_csv("treasury_jobs.csv")

    print("\n" + "=" * 60)
    print("✅ EMAIL PARSING COMPLETE!")
    print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
