import json
import hashlib
import sqlite3
import pandas as pd
from pathlib import Path

MAPPING_FILE = Path("data/category_mapping.json")
DB_PATH = Path("data/processed/expenses.db")

def ensure_mapping_file():
    """Generates a default category mapping JSON if it does not exist."""
    MAPPING_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not MAPPING_FILE.exists():
        default_mapping = {
            "zomato": "Food & Dining",
            "swiggy": "Food & Dining",
            "amazon": "Shopping",
            "flipkart": "Shopping",
            "uber": "Travel",
            "ola": "Travel",
            "airtel": "Utilities",
            "jio": "Utilities",
            "netflix": "Entertainment",
            "spotify": "Entertainment",
            "insurance": "Insurance",
            "rent": "Rent"
        }
        with open(MAPPING_FILE, 'w') as f:
            json.dump(default_mapping, f, indent=4)
        print(f"Created default category mapping at {MAPPING_FILE}")

def load_mapping():
    """Loads the category mapping from the JSON file."""
    ensure_mapping_file()
    with open(MAPPING_FILE, 'r') as f:
        mapping = json.load(f)
    # Ensure all keys are lowercase for case-insensitive matching
    return {k.lower(): v for k, v in mapping.items()}

def categorize_transaction(description, mapping):
    """Categorizes a single description based on the mapping."""
    if not description or pd.isna(description):
        return "Uncategorized"

    desc_lower = str(description).lower()
    for keyword, category in mapping.items():
        if keyword in desc_lower:
            return category

    return "Uncategorized"

def generate_hash(row):
    """Generates a SHA-256 hash for a row based on specific columns."""
    date = str(row.get('date', ''))
    desc = str(row.get('description', ''))
    amount = str(row.get('amount', ''))
    card = str(row.get('source_card', ''))

    hash_str = f"{date}{desc}{amount}{card}"
    return hashlib.sha256(hash_str.encode('utf-8')).hexdigest()

def process_and_store(df: pd.DataFrame):
    """Processes the DataFrame and stores it in the SQLite database."""
    print(f"[DATABASE] Received {len(df)} transactions for processing.")

    if df.empty:
        print("[WARNING] No data to process. Database update skipped.")
        return

    # 1. Rename columns to lowercase
    df.columns = [col.lower() for col in df.columns]

    print("[DEBUG] First 3 rows being sent to DB:")
    print(df[['date', 'description', 'amount']].head(3))

    # 2. Categorization
    mapping = load_mapping()
    df['category'] = df['description'].apply(lambda x: categorize_transaction(x, mapping))

    # 3. Hash Generation
    df['transaction_id'] = df.apply(generate_hash, axis=1)

    # 4. Database Upsert
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to SQLite
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            date TEXT,
            description TEXT,
            amount REAL,
            category TEXT,
            source_card TEXT,
            transaction_type TEXT
        )
    ''')

    # 3.5 Schema Alignment - Force Numeric Amount
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)

    # Upsert logic: INSERT OR IGNORE
    # We'll use executemany for efficiency
    records = df[['transaction_id', 'date', 'description', 'amount', 'category', 'source_card', 'transaction_type']].to_records(index=False)

    # Capture total_changes before execution
    initial_changes = conn.total_changes

    query = '''
        INSERT OR IGNORE INTO transactions (transaction_id, date, description, amount, category, source_card, transaction_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''

    cursor.executemany(query, records)
    conn.commit()

    # Calculate actual inserted rows based on total_changes diff
    inserted_rows = conn.total_changes - initial_changes

    if inserted_rows == 0 and len(df) > 0:
        print(f"[DEBUG] All {len(df)} transactions already exist in the database (Hash Match)..")

    print(f"[DATABASE] Successfully added {inserted_rows} new transactions to the database.")

    conn.close()

if __name__ == "__main__":
    # For testing purposes if run standalone
    raw_path = Path("data/processed/raw_transactions.csv")
    if raw_path.exists():
        print(f"Testing process_and_store with {raw_path}")
        df = pd.read_csv(raw_path)
        process_and_store(df)
    else:
        print(f"No {raw_path} found to test.")
