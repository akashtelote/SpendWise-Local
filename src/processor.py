import os
import json
import hashlib
import sqlite3
import pandas as pd

MAPPING_FILE = "data/category_mapping.json"
DB_PATH = "data/processed/expenses.db"

def ensure_mapping_file():
    """Generates a default category mapping JSON if it does not exist."""
    os.makedirs(os.path.dirname(MAPPING_FILE), exist_ok=True)
    if not os.path.exists(MAPPING_FILE):
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
            "spotify": "Entertainment"
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

def categorize_description(description, mapping):
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
    date = str(row.get('Date', ''))
    desc = str(row.get('Description', ''))
    amount = str(row.get('Amount', ''))
    card = str(row.get('Source_Card', ''))

    hash_str = f"{date}{desc}{amount}{card}"
    return hashlib.sha256(hash_str.encode('utf-8')).hexdigest()

def process_and_store(df: pd.DataFrame):
    """Processes the DataFrame and stores it in the SQLite database."""
    if df.empty:
        print("DataFrame is empty. Nothing to process.")
        return

    # 1. Categorization
    mapping = load_mapping()
    df['Category'] = df['Description'].apply(lambda x: categorize_description(x, mapping))

    # 2. Hash Generation
    df['transaction_id'] = df.apply(generate_hash, axis=1)

    # 3. Database Upsert
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Connect to SQLite
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            Date TEXT,
            Description TEXT,
            Amount REAL,
            Transaction_Type TEXT,
            Source_Card TEXT,
            Category TEXT
        )
    ''')

    # Upsert logic: INSERT OR IGNORE
    # We'll use executemany for efficiency
    records = df[['transaction_id', 'Date', 'Description', 'Amount', 'Transaction_Type', 'Source_Card', 'Category']].to_records(index=False)

    query = '''
        INSERT OR IGNORE INTO transactions (transaction_id, Date, Description, Amount, Transaction_Type, Source_Card, Category)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''

    cursor.executemany(query, records)
    conn.commit()

    print(f"Processed and stored {len(records)} records. Inserted new records, ignored duplicates.")

    conn.close()

if __name__ == "__main__":
    # For testing purposes if run standalone
    # Let's see if we have raw_transactions.csv
    raw_path = "data/processed/raw_transactions.csv"
    if os.path.exists(raw_path):
        print(f"Testing process_and_store with {raw_path}")
        df = pd.read_csv(raw_path)
        process_and_store(df)
    else:
        print(f"No {raw_path} found to test.")
