import pypdf
import json
import os
import glob
import re

def extract_w2_data(pdf_path):
    reader = pypdf.PdfReader(pdf_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text()
    
    # Simple regex extraction for common W-2 boxes
    data = {
        "file": os.path.basename(pdf_path),
        "wages_tips_other": re.findall(r"Wages, tips, other compensation\s+([\d,.]+)", text),
        "fed_tax_withheld": re.findall(r"Federal income tax withheld\s+([\d,.]+)", text),
        "ss_wages": re.findall(r"Social security wages\s+([\d,.]+)", text),
        "medicare_wages": re.findall(r"Medicare wages and tips\s+([\d,.]+)", text)
    }
    return data

def main():
    w2_path = "tax_2026/raw/2025-Taxes/W2/*.pdf"
    summary = []
    for f in glob.glob(w2_path):
        print(f"Processing {f}...")
        try:
            summary.append(extract_w2_data(f))
        except Exception as e:
            print(f"Error processing {f}: {e}")
    
    output_path = "tax_2026/processing/w2_summary.json"
    with open(output_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"W-2 Summary saved to {output_path}")

if __name__ == "__main__":
    main()
