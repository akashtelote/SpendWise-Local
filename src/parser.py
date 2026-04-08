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

def is_valid_transaction(row):
    """
    Validates a parsed row.
    Extracts date if it matches the regex \\d{2}[/-]\\d{2}[/-]\\d{2,4}.
    If the field contains non-date characters like 'C', 'Rs.', or 'Limit', discard the row.
    Returns (is_valid, extracted_date, reason)
    """
    date_val = str(row.get("Date", "")).strip()

    # Check for invalid characters/words in the date string
    invalid_keywords = ['c', 'rs.', 'limit']
    date_lower = date_val.lower()
    for keyword in invalid_keywords:
        if keyword in date_lower:
            return (False, None, f"Keyword '{keyword.upper()}' found in date")

    # Resilient regex check for date format (search anywhere in string)
    match = re.search(r'\d{2,4}[/-]\d{2}[/-]\d{2,4}', date_val)
    if not match:
        return (False, None, "Date format mismatch")

    return (True, match.group(0), "")

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

def identify_bank_and_card(text, filename=""):
    """
    Identifies the bank and extracts the last 4 digits of the card/account number.
    Returns (bank_name, card_suffix).
    """
    text_upper = text.upper()
    filename_upper = filename.upper()

    bank_name = "Generic"
    if "HDFC" in text_upper:
        bank_name = "HDFC"
    elif "ICICI" in text_upper:
        if "263570044" in text_upper or "SAVINGS A/C" in text_upper:
            bank_name = "ICICI Savings"
        else:
            bank_name = "ICICI CC"
    elif "SBI CARD" in text_upper or "BPCL" in filename_upper:
        bank_name = "SBI"

    # Attempt to find account/card number pattern: looks for Account No, Card No, etc., followed by digits
    # Simple regex to find 4 digits that might represent an account/card suffix
    card_suffix = bank_name
    match = re.search(r'(?:A/C|ACCOUNT|CARD).*?(\d{4})\b', text_upper)
    if match:
        card_suffix = f"{bank_name} - {match.group(1)}"

    return bank_name, card_suffix

def clean_amount(amount_str):
    """Strips non-numeric characters (except decimals), detects 'CR' for Credit, and converts to float."""
    if pd.isna(amount_str) or not str(amount_str).strip():
        return 0.0, False

    amt_str = str(amount_str).strip().upper()
    is_credit = "CR" in amt_str or amt_str.endswith(" CR") or " CR" in amt_str

    # Explicitly remove the rupee symbol, then remove other non-numeric characters
    amt_str = amt_str.replace('₹', '').replace('CR', '').replace(',', '')
    cleaned = re.sub(r'[^\d.]', '', amt_str)
    try:
        return float(cleaned), is_credit
    except ValueError:
        return 0.0, is_credit

def parse_hdfc_table(rows, source_card):
    """
    Parses HDFC tables.
    Assumes columns: Date | Description | Debit | Credit
    We merge Debit/Credit into Amount and Transaction_Type.
    """
    parsed_data = []
    headers = [" ".join(str(h).replace('\n', ' ').split()).lower() if h is not None else "" for h in rows[0]]

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
            amount, is_credit = clean_amount(debit_val)
            txn_type = "Credit" if is_credit else "Debit"
        # If there's a value in credit, it's a credit
        elif credit_val and any(c.isdigit() for c in credit_val):
            amount, is_credit = clean_amount(credit_val)
            txn_type = "Credit"

        try:
            date_parsed = pd.to_datetime(date, dayfirst=True).strftime('%Y-%m-%d')
        except Exception:
            date_parsed = date

        # HDFC Refinement: Ignore rows that are just GST entries (e.g., 'SGST-VPS') if they lack a clear merchant description
        desc_lower = desc.lower()
        if desc_lower in ["sgst-vps", "igst-vps", "cgst-vps", "sgst", "igst", "cgst"]:
            continue

        if amount > 0:
            parsed_data.append({
                "Date": date_parsed,
                "Description": desc,
                "Amount": amount,
                "Transaction_Type": txn_type,
                "Source_Card": source_card
            })

    return parsed_data

def parse_icici_cc_table(rows, source_card):
    """
    Parses ICICI CC tables.
    """
    parsed_data = []

    # Header normalization
    headers = [" ".join(str(h).replace('\n', ' ').split()).lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'description' in h or 'particulars' in h or 'transaction details' in h), 1)

    # Refined Column Mapping: prioritize 'amount (int)' or 'amount(rs.)' over just 'amount'
    amt_idx = -1
    for i, h in enumerate(headers):
        if h == 'amount (int)' or h == 'amount(rs.)':
            amt_idx = i
            break
    if amt_idx == -1:
        amt_idx = next((i for i, h in enumerate(headers) if 'amount' in h), 2)

    type_idx = next((i for i, h in enumerate(headers) if 'dr/cr' in h or 'type' in h), -1)

    def validate_row(row_data):
        for cell in row_data:
            if cell is None:
                continue
            match = re.search(r'\d{2}/\d{2}/\d{4}', str(cell))
            if match:
                return match.group(0)
        return None

    for row in rows[1:]:
        if not row:
            continue

        extracted_date = validate_row(row)
        if not extracted_date:
            continue

        date = extracted_date
        desc = str(row[desc_idx]).strip() if len(row) > desc_idx and row[desc_idx] else ""
        amt_str = str(row[amt_idx]).strip() if len(row) > amt_idx and row[amt_idx] else ""
        type_str = str(row[type_idx]).strip().upper() if type_idx != -1 and len(row) > type_idx and row[type_idx] else ""

        amount, is_credit = clean_amount(amt_str)

        txn_type = "Debit" # Default
        if "CR" in type_str or is_credit:
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

def parse_icici_savings_table(rows, source_card):
    """
    Parses ICICI Savings tables.
    """
    parsed_data = []
    headers = [" ".join(str(h).replace('\n', ' ').split()).lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'particulars' in h or 'description' in h), 1)
    withdraw_idx = next((i for i, h in enumerate(headers) if 'withdrawals' in h or 'withdrawal' in h or 'debit' in h), 2)
    deposit_idx = next((i for i, h in enumerate(headers) if 'deposits' in h or 'deposit' in h or 'credit' in h), 3)

    for row in rows[1:]:
        if not row or row[date_idx] is None or not str(row[date_idx]).strip():
            continue

        date = str(row[date_idx]).strip()
        desc = str(row[desc_idx]).strip() if len(row) > desc_idx and row[desc_idx] else ""
        withdraw_str = str(row[withdraw_idx]).strip() if len(row) > withdraw_idx and row[withdraw_idx] else ""
        deposit_str = str(row[deposit_idx]).strip() if len(row) > deposit_idx and row[deposit_idx] else ""

        amount = 0.0
        txn_type = ""

        if withdraw_str and any(c.isdigit() for c in withdraw_str):
            amount, is_credit = clean_amount(withdraw_str)
            txn_type = "Credit" if is_credit else "Debit"
        elif deposit_str and any(c.isdigit() for c in deposit_str):
            amount, is_credit = clean_amount(deposit_str)
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

def parse_sbi_table(rows, source_card):
    """
    Parses SBI tables.
    """
    parsed_data = []
    headers = [" ".join(str(h).replace('\n', ' ').split()).lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'transaction details' in h or 'description' in h), 1)

    amt_idx = -1
    for i, h in enumerate(headers):
        if h == 'amount (int)' or h == 'amount(rs.)':
            amt_idx = i
            break
    if amt_idx == -1:
        amt_idx = next((i for i, h in enumerate(headers) if 'amount' in h), -1)

    if amt_idx == -1:
        # Fallback if we couldn't find 'amount' directly
        amt_idx = len(headers) - 1

    cleanup_keywords = ["previous balance", "total outstanding", "minimum amount due", "account summary"]

    for row in rows[1:]:
        if not row or row[date_idx] is None or not str(row[date_idx]).strip():
            continue

        date = str(row[date_idx]).strip()
        desc = str(row[desc_idx]).strip() if len(row) > desc_idx and row[desc_idx] else ""

        # Cleanup filter for SBI
        desc_lower = desc.lower()
        if any(kw in desc_lower for kw in cleanup_keywords):
            # If we hit an account summary section, often it marks the end of transactions in the table block
            if "account summary" in desc_lower:
                break
            continue

        amt_str = str(row[amt_idx]).strip() if len(row) > amt_idx and row[amt_idx] else ""

        amount, is_credit = clean_amount(amt_str)
        txn_type = "Credit" if is_credit else "Debit"

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

    headers = [" ".join(str(h).replace('\n', ' ').split()).lower() if h is not None else "" for h in rows[0]]

    date_idx = next((i for i, h in enumerate(headers) if 'date' in h), 0)
    desc_idx = next((i for i, h in enumerate(headers) if 'description' in h or 'particulars' in h), 1)

    # Check if we have debit/credit columns
    debit_idx = next((i for i, h in enumerate(headers) if 'debit' in h or 'withdrawal' in h), -1)
    credit_idx = next((i for i, h in enumerate(headers) if 'credit' in h or 'deposit' in h), -1)

    # Or just a single amount column
    amt_idx = -1
    for i, h in enumerate(headers):
        if h == 'amount (int)' or h == 'amount(rs.)':
            amt_idx = i
            break
    if amt_idx == -1:
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
                amount, is_credit = clean_amount(debit_val)
                txn_type = "Credit" if is_credit else "Debit"
            elif credit_val and any(c.isdigit() for c in credit_val):
                amount, is_credit = clean_amount(credit_val)
                txn_type = "Credit"
        elif amt_idx != -1:
            # Simple amount, try to check sign or just assume Debit
            amt_str = str(row[amt_idx]).strip() if len(row) > amt_idx and row[amt_idx] else ""
            amount, is_credit = clean_amount(amt_str)
            if amt_str.startswith('-'):
                txn_type = "Debit" # Sometimes negative is debit
            elif is_credit:
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
        bank_name, ident_source_card = identify_bank_and_card(pdf_text, filename)
        if not matched_key:
            source_card = ident_source_card

    if matched_key:
        # If a password successfully unlocked the PDF, assign its corresponding key to Source_Card
        source_card = matched_key

    parsed_rows = []
    found_any_table = False

    try:
        with pdfplumber.open(pdf_path, password=password_to_use) as pdf:
            total_pages = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                print(f"[DEBUG] Scanning Page {i+1}/{total_pages} of {filename}....")
                rows_on_page = 0
                try:
                    # Extract tables
                    tables = []

                    if bank_name == "ICICI CC" and i == 0:
                        # Find "CREDIT SUMMARY" to crop page 1
                        words = page.extract_words()
                        credit_summary_y = None
                        for w in words:
                            if w['text'] == "CREDIT" or w['text'] == "SUMMARY":
                                # Very basic check - we can check a combination
                                pass

                        # Better: search text for "CREDIT SUMMARY" and find bounding box of the whole block or words
                        for w_idx, w in enumerate(words):
                            if w['text'].upper() == "CREDIT" and w_idx + 1 < len(words) and words[w_idx+1]['text'].upper() == "SUMMARY":
                                credit_summary_y = w['bottom']
                                break

                        if credit_summary_y:
                            cropped_page = page.crop((0, credit_summary_y, page.width, page.height))
                            tables = cropped_page.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 5})
                        else:
                            # Fallback if not found
                            tables = page.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 5})

                    elif bank_name == "SBI":
                        tables = page.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text"})
                    else:
                        tables = page.extract_tables()

                    if not tables:
                        if i == 0 and bank_name != "ICICI CC":
                            # Page 1 specific fallbacks for non-ICICI CC
                            tables = page.extract_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines", "snap_tolerance": 3})
                            if not tables:
                                tables = page.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text"})
                            if not tables:
                                bottom_half = page.crop((0, page.height / 2, page.width, page.height))
                                tables = bottom_half.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text"})
                        elif bank_name != "ICICI CC":
                            # Standard fallback for other pages
                            tables = page.extract_tables(table_settings={"vertical_strategy": "text", "horizontal_strategy": "text"})

                    if tables:
                        found_any_table = True

                    junk_keywords = [
                        "illustration", "late payment charges", "method of payment",
                        "minimum amount due", "scenario a", "scenario a/b", "gst entry"
                    ]

                    global_junk_keywords = ["illustration", "assume", "scenario"]

                    for table in tables:
                        if not table or not table[0]:
                            continue

                        # Global Table Content Filter
                        table_content = " ".join(str(cell).lower() for row in table for cell in row if cell is not None)
                        if any(junk in table_content for junk in global_junk_keywords):
                            print(f"[DEBUG] Skipping junk table on page {i+1} containing global junk keywords in content.")
                            continue

                        # Normalize header row to match 'Transaction Details' and other split headers robustly
                        header_row = [" ".join(str(h).replace('\n', ' ').split()).lower() if h is not None else "" for h in table[0]]
                        if any(any(junk in header_cell for junk in junk_keywords) for header_cell in header_row):
                            print(f"[DEBUG] Skipping junk table on page {i+1} containing keywords in header.")
                            continue

                        # Specific profile checks
                        if bank_name == "SBI":
                            if not any("transaction details" in header_cell for header_cell in header_row):
                                continue

                        if bank_name == "HDFC":
                            rows = parse_hdfc_table(table, source_card)
                        elif bank_name == "ICICI CC":
                            rows = parse_icici_cc_table(table, source_card)
                        elif bank_name == "ICICI Savings":
                            rows = parse_icici_savings_table(table, source_card)
                        elif bank_name == "SBI":
                            rows = parse_sbi_table(table, source_card)
                        else:
                            rows = parse_generic_table(table, source_card)

                        # Global Row Validator: Discard row if Description contains 'LIMIT', 'BALANCE', 'TOTAL', 'OUTSTANDING', 'DUE', or 'SUMMARY'
                        valid_rows = []
                        global_row_junk_keywords = ['limit', 'balance', 'total', 'outstanding', 'due', 'summary']
                        for row in rows:
                            desc_lower = str(row.get("Description", "")).lower()
                            is_valid, extracted_date, reason = is_valid_transaction(row)

                            amount = row.get("Amount", 0.0)

                            if not is_valid:
                                if i == 0: print(f"[DEBUG] Page {i+1}: Rejected Row {row} | Reason: {reason}")
                            elif amount <= 0:
                                if i == 0: print(f"[DEBUG] Page {i+1}: Rejected Row {row} | Reason: Malformed Amount Data")
                            elif any(junk in desc_lower for junk in global_row_junk_keywords):
                                if i == 0: print(f"[DEBUG] Page {i+1}: Rejected Row {row} | Reason: Global keyword filter match")
                            else:
                                row["Date"] = extracted_date
                                valid_rows.append(row)

                        parsed_rows.extend(valid_rows)
                        rows_on_page += len(valid_rows)

                    # Universal Regex Fallback
                    if rows_on_page == 0:
                        page_text = page.extract_text()
                        if page_text:
                            if i == 0:
                                print(f"[DEBUG] Raw Text Sample (First 500 chars) of Page 1: {page_text[:500]}")
                            regex = r'(\d{2}/\d{2}/\d{2,4})\s+(.*?)\s+([\d,]+\.\d{2}(?:\s*CR)?)'
                            for match in re.finditer(regex, page_text, re.DOTALL):
                                date_str = match.group(1)
                                raw_desc = match.group(2).strip()
                                # Post-processing: Remove newlines and long numeric reference strings (8+ digits)
                                raw_desc = raw_desc.replace('\n', ' ')
                                desc = re.sub(r'\d{8,}', '', raw_desc).strip()
                                amt_str = match.group(3)

                                amount, is_credit = clean_amount(amt_str)
                                txn_type = "Credit" if "CR" in amt_str.upper() else "Debit"

                                try:
                                    date_parsed = pd.to_datetime(date_str, dayfirst=True).strftime('%Y-%m-%d')
                                except Exception:
                                    date_parsed = date_str

                                # Apply Global Row Validator to regex fallback
                                desc_lower = desc.lower()
                                global_row_junk_keywords = ['limit', 'balance', 'total', 'outstanding', 'due', 'summary']

                                row_dict = {
                                    "Date": date_parsed,
                                    "Description": desc,
                                    "Amount": amount,
                                    "Transaction_Type": txn_type,
                                    "Source_Card": source_card
                                }

                                is_valid, extracted_date, reason = is_valid_transaction(row_dict)

                                if not is_valid:
                                    if i == 0: print(f"[DEBUG] Page {i+1}: Rejected Row {row_dict} | Reason: {reason}")
                                elif amount <= 0:
                                    if i == 0: print(f"[DEBUG] Page {i+1}: Rejected Row {row_dict} | Reason: Malformed Amount Data")
                                elif any(junk in desc_lower for junk in global_row_junk_keywords):
                                    if i == 0: print(f"[DEBUG] Page {i+1}: Rejected Row {row_dict} | Reason: Global keyword filter match")
                                else:
                                    row_dict["Date"] = extracted_date
                                    parsed_rows.append(row_dict)
                                    rows_on_page += 1
                                found_any_table = True

                    print(f"[DEBUG] Extracted {rows_on_page} rows from Page {i+1}..")
                except Exception as e:
                    print(f"Failed to process page {i+1} in {filename}:\n{traceback.format_exc()}")

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
    import time
    from datetime import datetime, timedelta

    start_time = time.time()
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

    total_time = time.time() - start_time
    print(f"[SUMMARY] Parser completed in {total_time:.2f} seconds. Total rows: {len(df)}.")

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
