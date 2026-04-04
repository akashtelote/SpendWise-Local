import os
import json
import re
import pandas as pd
import pdfplumber
from fpdf import FPDF
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Schema Definition
# Date | Description | Amount | Transaction_Type | Source_Card
COLUMNS = ["Date", "Description", "Amount", "Transaction_Type", "Source_Card"]

def get_pdf_passwords():
    """Reads PDF_PASSWORDS from .env and parses it into a dictionary."""
    passwords_str = os.getenv("PDF_PASSWORDS", "{}")
    try:
        return json.loads(passwords_str)
    except json.JSONDecodeError:
        print("Warning: Could not parse PDF_PASSWORDS from .env. Ensure it is valid JSON.")
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
        print(f"Error reading text from {pdf_path}: {e}")
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
    print(f"Processing {pdf_path}...")

    # First, try to read text to identify bank
    bank_name = "Generic"
    source_card = "Generic"

    # We might need to try multiple passwords
    password_to_use = None
    pdf_text = ""
    is_encrypted = False

    try:
        # Try without password first
        pdf_text = extract_text_from_pdf(pdf_path)
    except Exception as e:
        # If it fails, try with passwords
        is_encrypted = True
        pass

    if not pdf_text and is_encrypted:
        # Try passwords
        for pwd in passwords.values():
            pdf_text = extract_text_from_pdf(pdf_path, pwd)
            if pdf_text:
                password_to_use = pwd
                break

    if pdf_text:
        bank_name, source_card = identify_bank_and_card(pdf_text)

    # Only assign bank password if we know it's encrypted and haven't found it yet
    if is_encrypted and not password_to_use and bank_name in passwords:
        password_to_use = passwords[bank_name]

    parsed_rows = []

    try:
        with pdfplumber.open(pdf_path, password=password_to_use) as pdf:
            for page in pdf.pages:
                # Extract tables
                tables = page.extract_tables()
                for table in tables:
                    if bank_name == "HDFC":
                        rows = parse_hdfc_table(table, source_card)
                    elif bank_name == "ICICI":
                        rows = parse_icici_table(table, source_card)
                    else:
                        rows = parse_generic_table(table, source_card)

                    parsed_rows.extend(rows)
    except Exception as e:
        print(f"Failed to process {pdf_path}. Error: {e}")

    return parsed_rows

def parse_all_pdfs(raw_dir="data/raw"):
    """
    Iterates through all PDFs in raw_dir, parses them, and returns a unified DataFrame.
    """
    if not os.path.exists(raw_dir):
        print(f"Directory {raw_dir} does not exist.")
        return pd.DataFrame(columns=COLUMNS)

    passwords = get_pdf_passwords()
    all_data = []

    for filename in os.listdir(raw_dir):
        if filename.lower().endswith('.pdf'):
            filepath = os.path.join(raw_dir, filename)
            rows = process_pdf(filepath, passwords)
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
    os.makedirs(raw_dir, exist_ok=True)

    # HDFC Mock
    hdfc_headers = ["Date", "Particulars", "Chq/Ref No.", "Value Date", "Withdrawal Amount", "Deposit Amount", "Closing Balance"]
    hdfc_data = [
        ["01/04/23", "To Amazon", "123456", "01/04/23", "1,500.00", "", "10000.00"],
        ["05/04/23", "Salary", "", "05/04/23", "", "50,000.00", "60000.00"],
        ["10/04/23", "Grocery", "789012", "10/04/23", "200.50", "", "59799.50"]
    ]
    create_table_pdf(os.path.join(raw_dir, "mock_hdfc_statement.pdf"), "HDFC Bank Statement A/C 9999", hdfc_headers, hdfc_data)

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
    create_table_pdf(os.path.join(raw_dir, "mock_icici_statement.pdf"), "ICICI Bank Account 8888", icici_mock_headers, icici_mock_data)

    # Generic Mock
    generic_headers = ["Date", "Description", "Amount", "Balance"]
    generic_data = [
        ["2023-06-01", "Rent", "-15000.00", "5000.00"],
        ["2023-06-02", "Refund", "500.00", "5500.00"],
        ["2023-06-05", "ATM", "-2000.00", "3500.00"]
    ]
    create_table_pdf(os.path.join(raw_dir, "mock_generic_statement.pdf"), "SomeBank Statement", generic_headers, generic_data)

if __name__ == "__main__":
    RAW_DIR = "data/raw"
    PROCESSED_DIR = "data/processed"

    print("Generating mock PDFs...")
    generate_mock_pdfs(RAW_DIR)

    print("Parsing PDFs...")
    df = parse_all_pdfs(RAW_DIR)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    out_path = os.path.join(PROCESSED_DIR, "raw_transactions.csv")
    df.to_csv(out_path, index=False)
    print(f"Data saved to {out_path}")
    print("\nParsed Data Sample:")
    print(df.head(10))
