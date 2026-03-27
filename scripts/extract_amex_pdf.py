import pdfplumber
import re

def extract_transactions(pdf_path):
    transactions = []

    current_user = "Unknown"
    current_last4 = "Unknown"

    # Detect section headers like:
    # "ADRIANA NEVAREZ CARRERA - 1234"
    section_pattern = re.compile(r'(LUIS|ADRIANA)[^\d]*(\d{4})', re.IGNORECASE)

    # Detect transactions
    txn_pattern = re.compile(
        r'(\d{1,2}/\d{1,2})\s+(.*?)\s+(-?\$?[\d,]+\.\d{2})'
    )

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = text.split("\n")

            for line in lines:
                # --- Detect section (card user + last4) ---
                section_match = section_pattern.search(line)
                if section_match:
                    name = section_match.group(1).lower()

                    if "luis" in name:
                        current_user = "Luis"
                    elif "adriana" in name:
                        current_user = "Adriana"

                    current_last4 = section_match.group(2)
                    continue

                # --- Detect transaction ---
                txn_match = txn_pattern.match(line)
                if txn_match:
                    date = txn_match.group(1)
                    description = txn_match.group(2).strip()
                    amount = txn_match.group(3).replace("$", "").replace(",", "")

                    transactions.append({
                        "date": date,
                        "description": description,
                        "amount": float(amount),
                        "card_user": current_user,
                        "card_last4": current_last4
                    })

    return transactions


if __name__ == "__main__":
    pdf_path = "../raw/2025-Taxes/Amex Blue/Amex Blue January to December 2025.pdf"

    txns = extract_transactions(pdf_path)

    for t in txns[:10]:
        print(t)

    print(f"\nTotal transactions: {len(txns)}")
