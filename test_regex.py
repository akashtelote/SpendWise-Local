import re
from src.parser import is_valid_transaction

test_rows = [
    {"Date": "2023-05-15", "Desc": "Valid YYYY-MM-DD"},
    {"Date": "15-05-2023", "Desc": "Valid DD-MM-YYYY"},
    {"Date": "15/05/2023", "Desc": "Valid DD/MM/YYYY"},
    {"Date": "15/05/23", "Desc": "Valid DD/MM/YY"},
    {"Date": "C15/05/23", "Desc": "Invalid C prefix"},
    {"Date": "Rs. 100", "Desc": "Invalid Rs"},
    {"Date": "Limit: 100", "Desc": "Invalid Limit"},
]

for row in test_rows:
    print(f"{row['Desc']} ({row['Date']}): {is_valid_transaction(row)}")

