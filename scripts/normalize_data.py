import pandas as pd
import os
import glob
from dateutil import parser

# Configuration
BASE_DIR = "TaxBot"
RAW_DIR = os.path.join(BASE_DIR, "raw/2025-Taxes")
OUTPUT_FILE = os.path.join(BASE_DIR, "processing/master_transactions_2025.csv")

def get_metadata(file_path):
    """
    Mapping rules to determine owner and account_name.
    """
    path_lower = file_path.lower()
    
    # Default values
    owner = "Unknown"
    account_name = os.path.basename(os.path.dirname(file_path))

    # Mapping Logic based on your folder structure
    if "amex blue" in path_lower:
        owner = "Adriana"
        account_name = "Amex Blue - Adriana"
    elif "amex one" in path_lower:
        owner = "Luis"
        account_name = "Amex One - Luis"
    elif "wellsfargo atm" in path_lower:
        owner = "Luis"
        account_name = "Wells Fargo Checking - Luis"
    elif "wellsfargo credit card" in path_lower:
        owner = "Luis"
        account_name = "Wells Fargo CC - Luis"
    
    return owner, account_name

def normalize_generic(df, file_path, is_wellsfargo=False):
    """Standardize data into the new schema."""
    owner, account_name = get_metadata(file_path)
    output = pd.DataFrame()
    
    try:
        if is_wellsfargo:
            # Wells Fargo: Date, Amount, *, *, Description
            output['date'] = pd.to_datetime(df.iloc[:, 0])
            output['amount'] = pd.to_numeric(df.iloc[:, 1])
            desc_col = 4 if len(df.columns) > 4 else 2
            output['description'] = df.iloc[:, desc_col]
        else:
            # Amex / Standard CSV
            df.columns = [c.strip() for c in df.columns]
            output['date'] = pd.to_datetime(df['Date'])
            output['amount'] = pd.to_numeric(df['Amount'])
            output['description'] = df['Description']

        # New Schema Fields
        output['account_name'] = account_name
        output['owner'] = owner
        output['source_file'] = os.path.basename(file_path)
    except Exception as e:
        print(f"Error normalizing {file_path}: {e}")
        return pd.DataFrame()
    
    return output

def main():
    print(f"🚀 Updating Phase 1: Normalization with Owner Tracking")
    all_txns = []
    
    # Patterns to search
    search_patterns = [
        (os.path.join(RAW_DIR, "Amex Blue/*.csv"), False),
        (os.path.join(RAW_DIR, "Amex One/*.csv"), False),
        (os.path.join(RAW_DIR, "Wellsfargo ATM/*.csv"), True),
        (os.path.join(RAW_DIR, "Wellsfargo Credit Card/*.csv"), True),
    ]

    for pattern, is_wf in search_patterns:
        for f in glob.glob(pattern):
            print(f"Processing {f}...")
            try:
                # Wells Fargo ATM CSV often has no header
                header = None if "ATM" in f else 'infer'
                df = pd.read_csv(f, header=header)
                if not df.empty:
                    norm_df = normalize_generic(df, f, is_wf)
                    if not norm_df.empty:
                        all_txns.append(norm_df)
            except Exception as e:
                print(f"Skipping {f}: {e}")

    if all_txns:
        master_df = pd.concat(all_txns, ignore_index=True)
        # Standardize date to YYYY-MM-DD
        master_df['date'] = pd.to_datetime(master_df['date']).dt.strftime('%Y-%m-%d')
        master_df = master_df.sort_values(by='date')
        
        # Deduplication (Date + Amount + Description)
        initial_len = len(master_df)
        master_df = master_df.drop_duplicates(subset=['date', 'amount', 'description'])
        final_len = len(master_df)

        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        master_df.to_csv(OUTPUT_FILE, index=False)
        
        print(f"\n✅ Phase 1 Complete: {OUTPUT_FILE}")
        print(f"Transactions: {final_len} (Removed {initial_len - final_len} duplicates)")
        print("\nSummary by Owner:")
        print(master_df.groupby(['owner', 'account_name']).size())
    else:
        print("❌ No transactions found. Check your folder structure.")

if __name__ == "__main__":
    main()
