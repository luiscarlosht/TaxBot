import pandas as pd
import os
import glob
import re

# Configuration for paths
BASE_DIR = "TaxBot"
RAW_DIR = os.path.join(BASE_DIR, "raw/2025-Taxes")
OUTPUT_FILE = os.path.join(BASE_DIR, "processing/master_transactions_2025.csv")

def normalize_amex(df, account_name):
    """Standardize Amex CSV format."""
    df.columns = [c.strip() for c in df.columns]
    output = pd.DataFrame()
    output['date'] = pd.to_datetime(df['Date'])
    output['description'] = df['Description']
    # Amex amounts are positive for spend, negative for payments usually
    output['amount'] = pd.to_numeric(df['Amount'])
    output['account'] = account_name
    return output

def normalize_wellsfargo(df, account_name):
    """Standardize Wells Fargo CSV format (Date, Amount, *, *, Description)."""
    output = pd.DataFrame()
    output['date'] = pd.to_datetime(df.iloc[:, 0])
    output['amount'] = pd.to_numeric(df.iloc[:, 1])
    # Description is usually in the last column or 5th
    desc_col = 4 if len(df.columns) > 4 else 2
    output['description'] = df.iloc[:, desc_col]
    output['account'] = account_name
    return output

def identify_transfers(df):
    """Flag internal transfers to avoid double counting expenses."""
    transfer_keywords = ['PAYMENT', 'CREDIT CARD', 'TRANSFER', 'AUTOPAY', 'WF CARD']
    df['is_transfer'] = df['description'].str.upper().apply(
        lambda x: any(k in x for k in transfer_keywords)
    )
    return df

def main():
    print(f"🚀 Starting Phase 1: Normalization in {BASE_DIR}")
    all_txns = []

    # 1. Process Amex Blue
    blue_path = os.path.join(RAW_DIR, "Amex Blue/*.csv")
    for f in glob.glob(blue_path):
        print(f"Reading {f}...")
        all_txns.append(normalize_amex(pd.read_csv(f), "Amex Blue"))

    # 2. Process Amex One
    one_path = os.path.join(RAW_DIR, "Amex One/*.csv")
    for f in glob.glob(one_path):
        print(f"Reading {f}...")
        all_txns.append(normalize_amex(pd.read_csv(f), "Amex One"))

    # 3. Process Wells Fargo ATM
    wf_atm_path = os.path.join(RAW_DIR, "Wellsfargo ATM/*.csv")
    for f in glob.glob(wf_atm_path):
        print(f"Reading {f}...")
        # Wells Fargo CSVs often don't have headers
        all_txns.append(normalize_wellsfargo(pd.read_csv(f, header=None), "Wells Fargo ATM"))

    # 4. Process Wells Fargo Credit Card
    wf_cc_path = os.path.join(RAW_DIR, "Wellsfargo Credit Card/*.csv")
    for f in glob.glob(wf_cc_path):
        print(f"Reading {f}...")
        # Simple CSV usually has headers or is very short
        try:
            df = pd.read_csv(f)
            if not df.empty:
                all_txns.append(normalize_wellsfargo(df, "Wells Fargo CC"))
        except:
            pass

    if not all_txns:
        print("❌ No transactions found. Check your 'raw' folder paths.")
        return

    # Consolidate
    master_df = pd.concat(all_txns, ignore_index=True)
    master_df = identify_transfers(master_df)
    
    # Sort and Save
    master_df = master_df.sort_values(by='date')
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    master_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n✅ Phase 1 Complete!")
    print(f"Total Transactions: {len(master_df)}")
    print(f"Master Ledger: {OUTPUT_FILE}")
    print(f"Potential Transfers Flagged: {master_df['is_transfer'].sum()}")

if __name__ == "__main__":
    main()
