import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def download_statements(days=32):
    """
    Connect to Gmail via IMAP, search for emails with "Statement" in the subject
    from the last `days` days (defaults to 32 days, approx. one billing cycle),
    and download PDF attachments.
    """
    print(f"Starting ingestion: Looking back {days} days.")

    # We load standard library 'os' locally to read environment variables
    # but strictly use pathlib for all path operations as requested.
    import os

    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_APP_PASSWORD")

    if not email_user or not email_pass:
        print("Error: EMAIL_USER or EMAIL_APP_PASSWORD not set in environment.")
        return 0

    # Ensure data/raw/ exists
    # Assuming this script is in src/ingestion.py, the project root is parent.parent
    project_root = Path(__file__).parent.parent
    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Connect to IMAP
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(email_user, email_pass)
    except Exception as e:
        print(f"Failed to connect to IMAP or login: {e}")
        return 0

    # Select INBOX
    mail.select("INBOX")

    # Calculate date for IMAP search (Format: DD-Mon-YYYY)
    search_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")

    # Search criteria: emails since `search_date` with subject "Statement"
    search_criteria = f'(SINCE "{search_date}" SUBJECT "Statement")'

    status, messages = mail.search(None, search_criteria)

    if status != "OK":
        print("Error searching for emails.")
        mail.logout()
        return 0

    email_ids = messages[0].split()
    print(f"Found {len(email_ids)} emails matching the criteria.")

    new_files_downloaded = 0

    for email_id in email_ids:
        status, msg_data = mail.fetch(email_id, "(RFC822)")
        if status != "OK":
            continue

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])

                # Get email date
                date_tuple = email.utils.parsedate_tz(msg["Date"])
                if date_tuple:
                    # convert to datetime to format it
                    email_datetime = parsedate_to_datetime(msg["Date"])
                    email_date_str = email_datetime.strftime("%Y-%m-%d")
                else:
                    email_date_str = "UnknownDate"

                # Check for attachments
                for part in msg.walk():
                    if part.get_content_maintype() == "multipart":
                        continue
                    if part.get("Content-Disposition") is None:
                        continue

                    filename = part.get_filename()
                    if filename:
                        # Decode the filename if it's encoded
                        decoded_name, charset = decode_header(filename)[0]
                        if isinstance(decoded_name, bytes):
                            filename = decoded_name.decode(charset or "utf-8")

                        # We only want PDFs
                        if filename.lower().endswith('.pdf'):
                            # Construct new filename
                            new_filename = f"{email_date_str}_{filename}"
                            filepath = raw_dir / new_filename

                            if filepath.exists():
                                print(f"File already exists, skipping: {new_filename}")
                            else:
                                print(f"Downloading: {new_filename}")
                                with filepath.open("wb") as f:
                                    f.write(part.get_payload(decode=True))
                                new_files_downloaded += 1

    mail.close()
    mail.logout()

    print(f"Successfully downloaded {new_files_downloaded} new statements.")
    return new_files_downloaded

if __name__ == "__main__":
    download_statements()
