import re

text = """
15/05/2023    Netflix Subscription    799.00
16/05/2023 UPI Transfer  2,000.00 CR
20/05/2023    Swiggy Order #12345 450.00
12/12/2024   Random String here    1,234.56CR
"""

regex = r'(\d{2}/\d{2}/\d{4})\s+(.*?)\s+([\d,]+\.\d{2}(?:\s*CR)?)'
for line in text.split('\n'):
    match = re.search(regex, line)
    if match:
        date = match.group(1)
        desc = match.group(2).strip()
        amt_str = match.group(3)
        print(f"Date: {date}, Desc: '{desc}', Amt: '{amt_str}'")
