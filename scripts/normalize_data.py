import pandas as pd
import os
import glob

def normalize_amex(df, account_name):
    # Amex CSVs usually have Date, Description, Amount
    df = df.copy()
    # Find the right columns based on typical Amex format
    df.columns = [c.strip() for c in df.columns]
    # Standardize
    output = pd.DataFrame()
    output['date'] = pd.to_datetime(df['Date'])
    output['description'] = df['Description']
    output['amount'] = df['Amount']
    output['account'] = account_name
    return output

def normalize_wellsfargo(df, account_name):
    # Wells Fargo usually: Date, Amount, *, *, Description
    # We will handle the specific column mapping here
    output = pd.DataFrame()
    # Simple mapping for V1, assuming standard CSV export
    output['date'] = pd.to_datetime(df.iloc[:, 0])
    output['amount'] = df.iloc[:, 1]
    output['description'] = df.iloc[:, 4] if len(df.columns) > 4 else df.iloc[:, 2]
    output['account'] = account_name
    return output

def main():
    raw_path = "tax_2026/raw/2025-Taxes"
    all_txns = []

    # Process Amex Blue CSV
    blue_csv = glob.glob(f"{raw_path}/Amex Blue/*.csv")
    if blue_csv:
        df = pd.read_csv(blue_csv[0])
        all_txns.append(normalize_amex(df, "Amex Blue"))

    # Process Amex One CSV
    one_csv = glob.glob(f"{raw_path}/Amex One/*.csv")
    if one_csv:
        df = pd.read_csv(one_csv[0])
        all_txns.append(normalize_amex(df, "Amex One"))

    # Process Wells Fargo CSVs
    wf_atm = glob.glob(f"{raw_path}/Wellsfargo ATM/*.csv")
    if wf_atm:
        df = pd.read_csv(wf_atm[0], header=None)
        all_txns.append(normalize_wellsfargo(df, "Wells Fargo ATM"))

    if all_txns:
        master_df = pd.concat(all_txns, ignore_index=True)
        master_df = master_df.sort_values(by='date')
        output_path = "tax_2026/processing/master_transactions_2025.csv"
        master_df.to_csv(output_path, index=False)
        print(f"Successfully created master transaction list: {output_path}")
        print(f"Total transactions: {len(master_df)}")

if __name__ == "__main__":
    main()
