"""
Email Job Alert Parser
Parses LinkedIn job alert emails from Gmail (no subject filtering)
Extracts Title / Company / Location from plain-text job cards
"""

import os
import base64
import re
import quopri
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

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
        """Authenticate with Gmail API (supports env-based secrets)."""
        print("🔐 Authenticating with Gmail...")

        creds = None

        # Optional: decode credentials from env (GitHub Actions secrets)
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

        # If no valid credentials, authenticate interactively (local only)
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

            # Save token for reuse
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

        # Gmail query notes:
        # - after:YYYY/MM/DD works, but "newer_than:7d" is simple and robust
        query = f'from:({LINKEDIN_SENDER}) newer_than:{days_back}d'

        print(f"\n🔍 Searching Gmail...")
        print(f"   Query: {query}")
        print(f"   Max results: {max_results}")

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
        """Decode Gmail API base64url body and also handle quoted-printable soft breaks."""
        raw = base64.urlsafe_b64decode(data_b64url.encode("utf-8"))
        # Many LinkedIn bodies are quoted-printable → decode it safely
        try:
            raw = quopri.decodestring(raw)
        except Exception:
            pass
        # Remove common quoted-printable soft line breaks just in case
        text = raw.decode("utf-8", errors="replace")
        text = text.replace("=\r\n", "").replace("=\n", "")
        return text

    def _extract_parts(self, payload: dict) -> List[dict]:
        """Flatten Gmail message parts (recursive)."""
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
        """
        Return (plain_text, html_text).
        Prefer parsing plain text for job cards.
        """
        payload = message.get("payload", {})
        parts = self._extract_parts(payload)

        plain = ""
        html = ""

        # Sometimes body is directly in payload without parts
        if payload.get("body", {}).get("data") and not parts:
            # assume plain
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
        company = re.sub(r"\s+", " ", company).strip()
        company = re.sub(r"\s+(GmbH|AG|SE|KGaA|Ltd|Inc|Corp|SA)\.?$", "", company, flags=re.I)
        return company.strip()

    def _clean_url(self, url: str) -> str:
        url = url.strip()
        # cut tracking params
        url = url.split("?")[0]
        return url

    def _is_noise_line(self, line: str) -> bool:
        l = line.strip()
        if not l:
            return True
        noise_phrases = [
            "Your job alert has been created",
            "You’ll receive notifications",
            "You=E2=80=99ll receive notifications",
            "Apply with resume",
            "Apply with resume & profile",
            "school alumni",
            "---------------------------------------------------------",
        ]
        if any(p.lower() in l.lower() for p in noise_phrases):
            return True
        # lines that are just separators or repeated hyphens
        if re.fullmatch(r"[-=_]{6,}", l):
            return True
        return False

    def _extract_jobs_from_plaintext(self, text: str) -> List[Dict]:
        """
        LinkedIn plain text format typically:
            <Title>
            <Company>
            <Location>
            View job: https://www.linkedin.com/comm/jobs/view/123...
        We locate each "View job:" and read 3 meaningful lines above it.
        """
        if not text:
            return []

        # Normalize line endings
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        # Find all view job occurrences and their positions
        pattern = re.compile(r"View job:\s*(https?://\S+)", re.IGNORECASE)
        matches = list(pattern.finditer(text))

        jobs: List[Dict] = []
        if not matches:
            return jobs

        lines = text.split("\n")

        # Build an index from character offset → line number
        # (cheap approach: track cumulative lengths)
        cum = 0
        line_start_offsets = []
        for ln in lines:
            line_start_offsets.append(cum)
            cum += len(ln) + 1  # + newline

        def offset_to_line_idx(offset: int) -> int:
            # last start offset <= offset
            lo, hi = 0, len(line_start_offsets) - 1
            while lo <= hi:
                mid = (lo + hi) // 2
                if line_start_offsets[mid] <= offset:
                    lo = mid + 1
                else:
                    hi = mid - 1
            return max(0, hi)

        for m in matches:
            url = self._clean_url(m.group(1))
            line_idx = offset_to_line_idx(m.start())

            # Look upwards for candidate lines
            candidates = []
            lookback = 12  # enough to skip "Apply..." or blank lines
            for i in range(line_idx - 1, max(-1, line_idx - lookback), -1):
                l = lines[i].strip()
                if self._is_noise_line(l):
                    continue
                # Stop if we bump into another "View job:" block or major header
                if l.lower().startswith("view job:"):
                    break
                candidates.append(l)
                if len(candidates) >= 3:
                    break

            # candidates are collected bottom-up; reverse to get Title, Company, Location
            candidates = list(reversed(candidates))

            title = candidates[0] if len(candidates) >= 1 else "Unknown"
            company = candidates[1] if len(candidates) >= 2 else "Unknown"
            location = candidates[2] if len(candidates) >= 3 else "Unknown"

            company = self._clean_company(company)

            # Basic sanity checks (avoid capturing random headers)
            if title == "Unknown" or company == "Unknown":
                # still keep it if you want, but usually better to skip
                continue

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
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
        """Parse a single LinkedIn email using plain text body first."""
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

            plain, html = self._get_email_bodies(message)

            # 1) Parse plain text (best for the sample you shared)
            jobs = self._extract_jobs_from_plaintext(plain)

            print(f"   🔗 Extracted {len(jobs)} jobs from plain text")

            # 2) (Optional) If you want, you could add an HTML fallback here later.
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
                # add tech detection
                for j in jobs:
                    j["technologies"] = ", ".join(self.detect_technologies(j.get("title", "")))
                self.jobs.extend(jobs)
                print(f"   ✅ Added {len(jobs)} jobs")
            else:
                print("   ⚠️  No jobs extracted")

        # De-dup
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
