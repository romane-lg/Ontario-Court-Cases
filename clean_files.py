import os
import pandas as pd
import re

def make_filename_from_case_title(case_title):
    """Convert case title to valid filename."""
    name = case_title.lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    name = name.strip('_')
    return name + ".txt"

def clean_and_align_data():
    """
    Clean, align, and rename court case files.
    Ensures df and cleaned files are perfectly aligned.
    """
    
    clean_folder = "/Users/iphonex/Downloads/Court-Cases-Text-Analytics/Ontario-Court-Cases/court_case_texts_cleaned"
    csv_path = "canlii_final_report_20.csv"
    
    # Load the dataframe
    df = pd.read_csv(csv_path)
    
    # Remove problematic cases (badly scraped)
    files_to_remove = [
        "case_33.txt", "case_103.txt", "case_223.txt",
        "case_224.txt", "case_225.txt", "case_226.txt",
        "case_227.txt", "case_228.txt", "case_229.txt", "case_230.txt"
    ]
    
    additional_files = [
        f'case_{i}.txt' for i in range(231, 343)
    ] + ['case_379.txt', 'case_388.txt', 'case_483.txt']
    
    files_to_remove.extend(additional_files)
    indices_to_remove = [33, 103, 223] + list(range(224, 231)) + list(range(231, 343)) + [379, 388, 483]
    df = df[~df.index.isin(indices_to_remove)].reset_index(drop=True)
    
    # Create file identifiers from case titles
    df['file_identifier'] = df['Case_Title'].apply(make_filename_from_case_title)
    
    # Remove any old case_*.txt files that might exist
    for f in os.listdir(clean_folder):
        if f.startswith("case_") and f.endswith(".txt"):
            os.remove(os.path.join(clean_folder, f))
            print(f"Removed old file: {f}")
    
    # Rename existing files to match new naming scheme
    for idx, row in df.iterrows():
        old_filename = f"case_{idx}.txt"
        new_filename = row['file_identifier']
        old_path = os.path.join(clean_folder, old_filename)
        new_path = os.path.join(clean_folder, new_filename)
        
        if os.path.exists(old_path) and old_path != new_path:
            os.rename(old_path, new_path)
            print(f"Renamed: {old_filename} → {new_filename}")
    
    # Verify alignment: only keep rows where files actually exist
    existing_files = {f.strip().lower() for f in os.listdir(clean_folder) if f.endswith(".txt")}
    df['file_identifier_norm'] = df['file_identifier'].str.strip().str.lower()
    df = df[df['file_identifier_norm'].isin(existing_files)].reset_index(drop=True)
    
    # Read texts in same order as df
    texts = []
    for fname in df['file_identifier']:
        file_path = os.path.join(clean_folder, fname)
        if os.path.exists(file_path):
            with open(file_path, encoding='utf-8') as f:
                texts.append(f.read())
    
    # Save cleaned dataframe
    df.to_csv('df_aligned.csv', index=False)
    
    print(f"\n✓ Alignment complete:")
    print(f"  - Rows in df: {len(df)}")
    print(f"  - Text files: {len(texts)}")
    print(f"  - Files saved to: df_aligned.csv")
    
    return df, texts

if __name__ == "__main__":
    df, texts = clean_and_align_data()