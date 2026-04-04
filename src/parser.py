import json
import re
import traceback
import pandas as pd
import pdfplumber
from pathlib import Path
from fpdf import FPDF
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Schema Definition
# Date | Description | Amount | Transaction_Type | Source_Card
COLUMNS = ["Date", "Description", "Amount", "Transaction_Type", "Source_Card"]

class DecryptionError(Exception):
    """Raised when a PDF fails to decrypt due to an incorrect or missing password."""
    pass

def get_pdf_passwords():
    """Reads passwords from data/passwords.json, creating a template if it doesn't exist."""
    pwd_file = Path("data/passwords.json")

    if not pwd_file.exists():
        template = {
            "Amazon": "ENTER_PASSWORD_HERE",
            "Sapphiro": "ENTER_PASSWORD_HERE",
            "Millenia": "ENTER_PASSWORD_HERE",
            "Rubyx": "ENTER_PASSWORD_HERE",
            "BPCL": "ENTER_PASSWORD_HERE",
            "NEUCARD": "ENTER_PASSWORD_HERE",
            "Statement": "ENTER_PASSWORD_HERE"
        }
        pwd_file.parent.mkdir(parents=True, exist_ok=True)
        with pwd_file.open("w") as f:
            json.dump(template, f, indent=4)
        print(f"Created password template at {pwd_file}. Please update it with real passwords.")
        return template

    try:
        with pwd_file.open("r") as f:
            json_data = json.load(f)
            return {k.strip().lower(): v.strip() for k, v in json_data.items()}
    except json.JSONDecodeError:
        print(f"Warning: Could not parse {pwd_file}. Ensure it is valid JSON.")
        return {}

def extract_text_from_pdf(pdf_path, password=None):
    """Extracts text from the first few pages of a PDF to help identify bank and card info."""
    text = ""
    try:
        with pdfplumber.open(pdf_path, password=password) as pdf:
            # Check up to first 2 pages
            for i in range(min(2, len(pdf.pages))):
                text += pdf.pages[i].extract_text() or ""
    except Exception as e:
        err_str = traceback.format_exc()
        if "PDFPasswordIncorrect" in err_str:
            raise DecryptionError(f"Password incorrect or missing for {pdf_path}")
        else:
            print(f"Error reading text from {pdf_path}:\n{err_str}")
            raise
    return text

def identify_bank_and_card(text):
    """
    Identifies the bank and extracts the last 4 digits of the card/account number.
    Returns (bank_name, card_suffix).
    """
    text_upper = text.upper()
    bank_name = "Generic"
    if "HDFC" in text_upper:
        bank_name = "HDFC"
    elif "ICICI" in text_upper:
        bank_name = "ICICI"

    # Attempt to find account/card number pattern: looks for Account No, Card No, etc., followed by digits
    # Simple regex to find 4 digits that might represent an account/card suffix
    card_suffix = bank_name
    match = re.search(r'(?:A/C|ACCOUNT|CARD).*?(\d{4})\b', text_upper)
    if match:
        card_suffix = f"{bank_name} - {match.group(1)}"

    return bank_name, card_suffix

def clean_amount(amount_str):
    """Strips non-numeric characters (except decimals) and converts to float."""
    if pd.isna(amount_str) or not str(amount_str).strip():
        return 0.0

    # Remove commas and currency symbols
    cleaned = re.sub(r'[^\d.]', '', str(amount_str))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def parse_hdfc_table(rows, source_card):
    """
    Parses HDFC tables.
    Assumes columns: Date | Description | Debit | Credit
    We merge Debit/Credit into Amount and Transaction_Type.
    """
    parsed_data = []
    headers = [str(h).strip().lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'description' in h or 'particulars' in h), 1)
    debit_idx = next((i for i, h in enumerate(headers) if 'debit' in h or 'withdrawal' in h), 2)
    credit_idx = next((i for i, h in enumerate(headers) if 'credit' in h or 'deposit' in h), 3)

    for row in rows[1:]:
        if not row or row[date_idx] is None or not str(row[date_idx]).strip():
            continue

        date = str(row[date_idx]).strip()
        desc = str(row[desc_idx]).strip() if len(row) > desc_idx and row[desc_idx] else ""

        debit_val = str(row[debit_idx]).strip() if len(row) > debit_idx and row[debit_idx] else ""
        credit_val = str(row[credit_idx]).strip() if len(row) > credit_idx and row[credit_idx] else ""

        amount = 0.0
        txn_type = ""

        # If there's a value in debit, it's a debit
        if debit_val and any(c.isdigit() for c in debit_val):
            amount = clean_amount(debit_val)
            txn_type = "Debit"
        # If there's a value in credit, it's a credit
        elif credit_val and any(c.isdigit() for c in credit_val):
            amount = clean_amount(credit_val)
            txn_type = "Credit"

        try:
            date_parsed = pd.to_datetime(date, dayfirst=True).strftime('%Y-%m-%d')
        except Exception:
            date_parsed = date

        if amount > 0:
            parsed_data.append({
                "Date": date_parsed,
                "Description": desc,
                "Amount": amount,
                "Transaction_Type": txn_type,
                "Source_Card": source_card
            })

    return parsed_data

def parse_icici_table(rows, source_card):
    """
    Parses ICICI tables.
    Assumes columns: Date | Description | Amount | Dr/Cr
    We split Amount and Dr/Cr indicator into Amount and Transaction_Type.
    """
    parsed_data = []
    headers = [str(h).strip().lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'description' in h or 'particulars' in h), 1)
    amt_idx = next((i for i, h in enumerate(headers) if 'amount' in h), 2)
    type_idx = next((i for i, h in enumerate(headers) if 'dr/cr' in h or 'type' in h), 3)

    for row in rows[1:]:
        if not row or row[date_idx] is None or not str(row[date_idx]).strip():
            continue

        date = str(row[date_idx]).strip()
        desc = str(row[desc_idx]).strip() if len(row) > desc_idx and row[desc_idx] else ""
        amt_str = str(row[amt_idx]).strip() if len(row) > amt_idx and row[amt_idx] else ""
        type_str = str(row[type_idx]).strip().upper() if len(row) > type_idx and row[type_idx] else ""

        amount = clean_amount(amt_str)

        txn_type = "Debit" # Default
        if "CR" in type_str:
            txn_type = "Credit"
        elif "DR" in type_str:
            txn_type = "Debit"

        try:
            date_parsed = pd.to_datetime(date, dayfirst=True).strftime('%Y-%m-%d')
        except Exception:
            date_parsed = date

        if amount > 0:
            parsed_data.append({
                "Date": date_parsed,
                "Description": desc,
                "Amount": amount,
                "Transaction_Type": txn_type,
                "Source_Card": source_card
            })

    return parsed_data

def parse_generic_table(rows, source_card):
    """
    Parses Generic tables.
    Attempts to dynamically find Date, Description, Amount columns.
    Assumes Debit/Credit logic might be present in a single Amount column (e.g. positive/negative or just generic).
    """
    parsed_data = []
    if not rows or len(rows) < 2:
        return parsed_data

    headers = [str(h).strip().lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'description' in h or 'particulars' in h), 1)

    # Check if we have debit/credit columns
    debit_idx = next((i for i, h in enumerate(headers) if 'debit' in h or 'withdrawal' in h), -1)
    credit_idx = next((i for i, h in enumerate(headers) if 'credit' in h or 'deposit' in h), -1)

    # Or just a single amount column
    amt_idx = next((i for i, h in enumerate(headers) if 'amount' in h), -1)

    for row in rows[1:]:
        if not row or row[date_idx] is None or not str(row[date_idx]).strip():
            continue

        date = str(row[date_idx]).strip()
        desc = str(row[desc_idx]).strip() if len(row) > desc_idx and row[desc_idx] else ""

        amount = 0.0
        txn_type = "Debit"

        if debit_idx != -1 and credit_idx != -1:
            # HDFC style
            debit_val = str(row[debit_idx]).strip() if len(row) > debit_idx and row[debit_idx] else ""
            credit_val = str(row[credit_idx]).strip() if len(row) > credit_idx and row[credit_idx] else ""
            if debit_val and any(c.isdigit() for c in debit_val):
                amount = clean_amount(debit_val)
                txn_type = "Debit"
            elif credit_val and any(c.isdigit() for c in credit_val):
                amount = clean_amount(credit_val)
                txn_type = "Credit"
        elif amt_idx != -1:
            # Simple amount, try to check sign or just assume Debit
            amt_str = str(row[amt_idx]).strip() if len(row) > amt_idx and row[amt_idx] else ""
            if amt_str.startswith('-'):
                txn_type = "Debit" # Sometimes negative is debit
            amount = clean_amount(amt_str)

        try:
            date_parsed = pd.to_datetime(date, dayfirst=True).strftime('%Y-%m-%d')
        except Exception:
            date_parsed = date

        if amount > 0:
            parsed_data.append({
                "Date": date_parsed,
                "Description": desc,
                "Amount": amount,
                "Transaction_Type": txn_type,
                "Source_Card": source_card
            })

    return parsed_data

def process_pdf(pdf_path, passwords):
    """Processes a single PDF file and returns a list of row dicts."""
    pdf_path = Path(pdf_path)
    filename = pdf_path.name
    print(f"Processing {pdf_path}...")

    # First, try to read text to identify bank
    bank_name = "Generic"
    source_card = "Generic"

    password_to_use = None
    pdf_text = ""
    is_encrypted = False
    matched_key = None

    try:
        # Try without password first
        pdf_text = extract_text_from_pdf(pdf_path)
    except DecryptionError:
        is_encrypted = True

    if is_encrypted:
        initial_password_failed = False
        # Find an initial matching password
        for key, pwd in passwords.items():
            if pwd != "ENTER_PASSWORD_HERE" and key in filename.lower():
                password_to_use = pwd
                matched_key = key
                break

        if password_to_use:
            print(f"[DEBUG] Attempting password key '{matched_key}' for file '{filename}'.")
            try:
                pdf_text = extract_text_from_pdf(pdf_path, password=password_to_use)
                print(f"[SUCCESS] Decrypted '{filename}' using key '{matched_key}'.")
            except DecryptionError:
                initial_password_failed = True
                print(f"[INFO] Initial password failed for {filename}. Trying all other known passwords...")
        else:
            print(f"[INFO] No pattern match for {filename}. Trying all known passwords...")
            initial_password_failed = True

        if initial_password_failed:
            success = False
            # Iterate through all unique passwords, skipping the one we just tried and placeholders
            tried_passwords = {password_to_use} if password_to_use else set()
            for key, pwd in passwords.items():
                if pwd == "ENTER_PASSWORD_HERE" or pwd in tried_passwords:
                    continue

                try:
                    pdf_text = extract_text_from_pdf(pdf_path, password=pwd)
                    password_to_use = pwd
                    matched_key = key
                    success = True
                    print(f"[SUCCESS] Decrypted '{filename}' using key '{matched_key}'.")
                    break
                except DecryptionError:
                    tried_passwords.add(pwd)

            if not success:
                print(f"[FATAL] No valid password found for {filename} in passwords.json. Bank Snippet: '{filename[:20]}' - Please verify bank type.")
                return []

    if pdf_text:
        bank_name, ident_source_card = identify_bank_and_card(pdf_text)
        if not matched_key:
            source_card = ident_source_card

    if matched_key:
        # If a password successfully unlocked the PDF, assign its corresponding key to Source_Card
        source_card = matched_key

    parsed_rows = []
    found_any_table = False

    try:
        with pdfplumber.open(pdf_path, password=password_to_use) as pdf:
            for page in pdf.pages:
                try:
                    # Extract tables
                    tables = page.extract_tables()
                    if not tables:
                        tables = page.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text"})

                    if tables:
                        found_any_table = True

                    for table in tables:
                        if bank_name == "HDFC":
                            rows = parse_hdfc_table(table, source_card)
                        elif bank_name == "ICICI":
                            rows = parse_icici_table(table, source_card)
                        else:
                            rows = parse_generic_table(table, source_card)

                        parsed_rows.extend(rows)
                except Exception as e:
                    print(f"Failed to process page in {filename}:\n{traceback.format_exc()}")

            if not found_any_table:
                print(f"[WARNING] {filename} appears to be an image. Skipping for now (requires OCR).")

    except DecryptionError:
        print(f"Failed to process {filename} due to password incorrect after fallback.")
    except Exception as e:
        print(f"Failed to process {filename}. Error:\n{traceback.format_exc()}")

    return parsed_rows

def parse_all_pdfs(raw_dir="data/raw"):
    """
    Iterates through all PDFs in raw_dir, parses them, and returns a unified DataFrame.
    Filters out files not matching YYYY-MM-DD prefix or older than 32 days.
    """
    from datetime import datetime, timedelta

    raw_dir_path = Path(raw_dir)
    if not raw_dir_path.exists():
        print(f"Directory {raw_dir_path} does not exist.")
        return pd.DataFrame(columns=COLUMNS)

    passwords = get_pdf_passwords()
    all_data = []

    cutoff_date = datetime.now() - timedelta(days=32)

    for file_path in raw_dir_path.iterdir():
        if not file_path.name.lower().endswith('.pdf'):
            continue
        filename = file_path.name

        # Extract YYYY-MM-DD from the beginning of the filename
        match = re.match(r'^(\d{4}-\d{2}-\d{2})', filename)
        if not match:
            print(f"[SKIP] Missing date prefix in filename: {filename}")
            continue

        file_date_str = match.group(1)
        try:
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
        except ValueError:
            print(f"[SKIP] Invalid date prefix format in filename: {filename}")
            continue

        if file_date < cutoff_date:
            print(f"[SKIP] File outside 32-day window: {filename}")
            continue

        rows = process_pdf(file_path, passwords)
        all_data.extend(rows)

    df = pd.DataFrame(all_data, columns=COLUMNS)
    return df

def create_table_pdf(filename, title, headers, data):
    """Helper to create a PDF with a table using fpdf2."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 16)
    pdf.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("helvetica", size=10)

    # Calculate column widths
    col_widths = [pdf.get_string_width(h) + 20 for h in headers]
    for row in data:
        for i, item in enumerate(row):
            w = pdf.get_string_width(str(item)) + 10
            if w > col_widths[i]:
                col_widths[i] = w

    line_height = pdf.font_size * 2.5

    # Header
    pdf.set_font("helvetica", "B", 10)
    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], line_height, header, border=1, align="C")
    pdf.ln(line_height)

    # Data
    pdf.set_font("helvetica", size=10)
    for row in data:
        for i, item in enumerate(row):
            pdf.cell(col_widths[i], line_height, str(item), border=1)
        pdf.ln(line_height)

    pdf.output(filename)

def generate_mock_pdfs(raw_dir="data/raw"):
    """Generates mock PDFs for testing."""
    from datetime import datetime
    raw_dir_path = Path(raw_dir)
    raw_dir_path.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")

    # HDFC Mock
    hdfc_headers = ["Date", "Particulars", "Chq/Ref No.", "Value Date", "Withdrawal Amount", "Deposit Amount", "Closing Balance"]
    hdfc_data = [
        ["01/04/23", "To Amazon", "123456", "01/04/23", "1,500.00", "", "10000.00"],
        ["05/04/23", "Salary", "", "05/04/23", "", "50,000.00", "60000.00"],
        ["10/04/23", "Grocery", "789012", "10/04/23", "200.50", "", "59799.50"]
    ]
    create_table_pdf(str(raw_dir_path / f"{today_str}_mock_hdfc_statement.pdf"), "HDFC Bank Statement A/C 9999", hdfc_headers, hdfc_data)

    # ICICI Mock
    icici_headers = ["S No.", "Value Date", "Transaction Date", "Cheque Number", "Transaction Remarks", "Withdrawal Amount (INR )", "Deposit Amount (INR )", "Balance (INR )"]
    # For ICICI we were planning Amount and Dr/Cr but standard might be Withdrawal/Deposit as well.
    # Let's mock our ICICI assumed schema: Date | Description | Amount | Dr/Cr
    icici_mock_headers = ["Date", "Description", "Amount", "Dr/Cr"]
    icici_mock_data = [
        ["15-05-2023", "Netflix Subscription", "799.00", "DR"],
        ["16-05-2023", "UPI Transfer", "2000.00", "CR"],
        ["20-05-2023", "Swiggy", "450.00", "DR"]
    ]
    create_table_pdf(str(raw_dir_path / f"{today_str}_mock_icici_statement.pdf"), "ICICI Bank Account 8888", icici_mock_headers, icici_mock_data)

    # Generic Mock
    generic_headers = ["Date", "Description", "Amount", "Balance"]
    generic_data = [
        ["2023-06-01", "Rent", "-15000.00", "5000.00"],
        ["2023-06-02", "Refund", "500.00", "5500.00"],
        ["2023-06-05", "ATM", "-2000.00", "3500.00"]
    ]
    create_table_pdf(str(raw_dir_path / f"{today_str}_mock_generic_statement.pdf"), "SomeBank Statement", generic_headers, generic_data)

if __name__ == "__main__":
    RAW_DIR = "data/raw"
    PROCESSED_DIR = Path("data/processed")

    print("Generating mock PDFs...")
    generate_mock_pdfs(RAW_DIR)

    print("Parsing PDFs...")
    df = parse_all_pdfs(RAW_DIR)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / "raw_transactions.csv"
    df.to_csv(out_path, index=False)
    print(f"Data saved to {out_path}")
    print("\nParsed Data Sample:")
    print(df.head(10))
