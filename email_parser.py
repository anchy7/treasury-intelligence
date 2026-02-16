"""
Email Job Alert Parser
Parses LinkedIn job alert emails from Gmail (no subject filtering)
Extracts Title / Company / Location from plain-text job cards
Adds Country (from subject fallback to location heuristics)
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
import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
LINKEDIN_SENDER = "jobalerts-noreply@linkedin.com"


class EmailJobParser:
    def __init__(self):
        self.service = self._get_gmail_service()
        self.jobs: List[Dict] = []

    # ----------------------------
    # Auth
    # ----------------------------
    def _get_gmail_service(self):
        print("🔐 Authenticating with Gmail...")

        creds = None

        # GitHub Actions env-based secrets (base64)
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

        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists("credentials.json"):
                    print("❌ credentials.json not found!")
                    print("   Provide GMAIL_CREDENTIALS/GMAIL_TOKEN in env (CI), or run locally once.")
                    return None

                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)

            with open("token.json", "w") as token:
                token.write(creds.to_json())

        print("✅ Gmail authentication successful\n")
        return build("gmail", "v1", credentials=creds)

    # ----------------------------
    # Gmail fetch
    # ----------------------------
    def get_linkedin_emails(self, days_back: int = 7, max_results: int = 50):
        """Fetch ALL LinkedIn Job Alerts emails (no subject filter)."""
        print("=" * 60)
        print("📧 FETCHING LINKEDIN JOB ALERT EMAILS")
        print("=" * 60)

        if not self.service:
            print("❌ Gmail service not initialized")
            return []

        query = f'from:({LINKEDIN_SENDER}) newer_than:{days_back}d'
        print(f"\n🔍 Searching Gmail...")
        print(f"   Query: {query}")

        try:
            results = (
                self.service.users()
                .messages()
                .list(userId="me", q=query, maxResults=max_results)
                .execute()
            )
            messages = results.get("messages", [])
            print(f"\n✅ Found {len(messages)} LinkedIn emails\n")
            return messages
        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            return []

    # ----------------------------
    # Parsing helpers
    # ----------------------------
    def _decode_part_data(self, data_b64url: str) -> str:
        raw = base64.urlsafe_b64decode(data_b64url.encode("utf-8"))
        try:
            raw = quopri.decodestring(raw)
        except Exception:
            pass
        text = raw.decode("utf-8", errors="replace")
        text = text.replace("=\r\n", "").replace("=\n", "")
        return text

    def _extract_parts(self, payload: dict) -> List[dict]:
        parts = []
        if not payload:
            return parts
        if payload.get("parts"):
            for p in payload["parts"]:
                parts.extend(self._extract_parts(p))
        else:
            parts.append(payload)
        return parts

    def _get_email_bodies(self, message: dict) -> Tuple[str, str]:
        payload = message.get("payload", {})
        parts = self._extract_parts(payload)

        plain = ""
        html = ""

        if payload.get("body", {}).get("data") and not parts:
            plain = self._decode_part_data(payload["body"]["data"])
            return plain, html

        for part in parts:
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data")
            if not data:
                continue
            decoded = self._decode_part_data(data)
            if mime == "text/plain" and not plain:
                plain = decoded
            elif mime == "text/html" and not html:
                html = decoded

        return plain, html

    def _clean_company(self, company: str) -> str:
        company = re.sub(r"\s+", " ", (company or "")).strip()
        company = re.sub(r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$", "", company, flags=re.I)
        return company.strip()

    def _clean_url(self, url: str) -> str:
        url = (url or "").strip()
        return url.split("?")[0]

    def _infer_country(self, subject: str, location: str) -> str:
        """
        1) Try subject like "... in Germany ..." or "... in Switzerland ..."
        2) Fallback: location heuristic
        """
        subj = subject or ""
        m = re.search(r"\bin\s+(Germany|Switzerland|Austria|Deutschland|Schweiz|Österreich)\b", subj, flags=re.I)
        if m:
            c = m.group(1).lower()
            return {
                "germany": "Germany",
                "deutschland": "Germany",
                "switzerland": "Switzerland",
                "schweiz": "Switzerland",
                "austria": "Austria",
                "österreich": "Austria",
            }.get(c, m.group(1))

        loc = (location or "").lower()
        swiss_markers = ["zurich", "zürich", "basel", "geneva", "genève", "lausanne", "bern", "zug", "lucerne", "luzern"]
        if any(x in loc for x in swiss_markers):
            return "Switzerland"

        germany_markers = ["frankfurt", "munich", "münchen", "berlin", "hamburg", "stuttgart", "düsseldorf", "cologne", "köln"]
        if any(x in loc for x in germany_markers):
            return "Germany"

        return "Unknown"

    def _extract_jobs_from_plaintext(self, text: str, subject: str) -> List[Dict]:
        """
        Strong rule for LinkedIn plain text cards:
        For each "View job: <url>" line:
            nearest non-empty line above = location
            next non-empty above = company
            next non-empty above = title
        """
        if not text:
            return []

        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = text.split("\n")

        view_pat = re.compile(r"View job:\s*(https?://\S+)", re.IGNORECASE)

        jobs: List[Dict] = []

        for idx, line in enumerate(lines):
            m = view_pat.search(line)
            if not m:
                continue

            url = self._clean_url(m.group(1))

            # Walk upwards to collect 3 non-empty lines
            collected: List[str] = []
            for j in range(idx - 1, -1, -1):
                candidate = lines[j].strip()
                if not candidate:
                    continue
                # Stop if we hit another card's "View job:" (rare but safe)
                if candidate.lower().startswith("view job:"):
                    break
                collected.append(candidate)
                if len(collected) == 3:
                    break

            # collected = [location, company, title] in reverse order? Actually collected is bottom-up:
            # first appended = location, second = company, third = title
            location = collected[0] if len(collected) >= 1 else "Unknown"
            company = collected[1] if len(collected) >= 2 else "Unknown"
            title = collected[2] if len(collected) >= 3 else "Unknown"

            company = self._clean_company(company)
            country = self._infer_country(subject, location)

            # sanity
            if title == "Unknown" or company == "Unknown":
                continue

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "country": country,
                    "url": url,
                    "source": "LinkedIn (Email)",
                    "date_scraped": datetime.now().strftime("%Y-%m-%d"),
                }
            )

        return jobs

    def detect_technologies(self, title: str) -> List[str]:
        tech = []
        text = (title or "").lower()

        if re.search(r"s[/]?4\s?hana|s4hana", text):
            tech.append("SAP S/4HANA")
        if "kyriba" in text:
            tech.append("Kyriba")
        if "python" in text:
            tech.append("Python")
        if "api" in text:
            tech.append("API")
        if "swift" in text:
            tech.append("SWIFT")
        return tech

    # ----------------------------
    # Main email parse
    # ----------------------------
    def parse_linkedin_email(self, msg_id: str) -> List[Dict]:
        try:
            message = (
                self.service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )

            headers = message.get("payload", {}).get("headers", [])
            subject = next((h["value"] for h in headers if h["name"].lower() == "subject"), "")
            sender = next((h["value"] for h in headers if h["name"].lower() == "from"), "")
            date_h = next((h["value"] for h in headers if h["name"].lower() == "date"), "")

            print(f"   ✉️  Subject: {subject}")
            print(f"   👤 From: {sender}")
            print(f"   🗓️  Date: {date_h}")

            plain, _html = self._get_email_bodies(message)

            jobs = self._extract_jobs_from_plaintext(plain, subject)
            print(f"   ✅ Extracted {len(jobs)} jobs (plain text cards)")
            return jobs

        except Exception as e:
            print(f"   ❌ Error parsing email: {e}")
            return []

    def process_all_emails(self, days_back: int = 7):
        print("\n" + "=" * 60)
        print("📧 EMAIL JOB ALERT PARSER")
        print("=" * 60)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")

        messages = self.get_linkedin_emails(days_back=days_back)

        if not messages:
            print("⚠️  No LinkedIn emails found")
            return

        print("🔍 Parsing emails...\n")

        for i, msg in enumerate(messages, 1):
            msg_id = msg["id"]
            print(f"📧 Email {i}/{len(messages)} (id={msg_id})")

            jobs = self.parse_linkedin_email(msg_id)
            if jobs:
                for j in jobs:
                    j["technologies"] = ", ".join(self.detect_technologies(j.get("title", "")))
                self.jobs.extend(jobs)
                print(f"   ➕ Added {len(jobs)} jobs")
            else:
                print("   ⚠️  No jobs extracted")

        if self.jobs:
            df = pd.DataFrame(self.jobs)
            before = len(df)
            df.drop_duplicates(subset=["title", "company", "location", "url"], keep="first", inplace=True)
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

        # Ensure columns exist
        for col in ["country", "technologies"]:
            if col not in df.columns:
                df[col] = ""

        if os.path.exists(filename):
            print(f"📂 Found existing file: {filename}")
            existing_df = pd.read_csv(filename)

            # Add missing columns to existing if needed
            for col in df.columns:
                if col not in existing_df.columns:
                    existing_df[col] = ""

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            before_count = len(combined_df)
            combined_df.drop_duplicates(subset=["company", "title", "location"], keep="last", inplace=True)
            after_count = len(combined_df)
            removed = before_count - after_count

            combined_df.to_csv(filename, index=False)

            print(f"\n📊 Final Statistics:")
            print(f"   Added (email): {len(df)}")
            print(f"   Duplicates removed: {removed}")
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
