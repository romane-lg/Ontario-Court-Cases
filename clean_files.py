import os
import zipfile
import re

# Paths
zip_path = "/Users/iphonex/Downloads/Court-Cases-Text-Analytics/Ontario-Court-Cases/court_case_texts_v3.zip"
extract_path = "/Users/iphonex/Downloads/Court-Cases-Text-Analytics/Ontario-Court-Cases/data_raw"
clean_path = "/Users/iphonex/Downloads/Court-Cases-Text-Analytics/Ontario-Court-Cases/data_clean"

# Fresh folders
import shutil
if os.path.exists(extract_path):
    shutil.rmtree(extract_path)
if os.path.exists(clean_path):
    shutil.rmtree(clean_path)
os.makedirs(extract_path)
os.makedirs(clean_path)

# Extract
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)
print("Extraction done.")

# Cleaning function
def clean_text(text):
    if not text:
        return text

    # Remove paragraph numbers like:
    # [
    # 12
    # ]
    # This handles numbers on their own line inside brackets
    text = re.sub(r'\[\s*\n*\s*\d+\s*\n*\s*\]', '', text)

    # Replace non-breaking spaces
    text = text.replace("\xa0", " ")

    # Lowercase
    text = text.lower()

    # Remove spacing before punctuation
    text = re.sub(r'\s+([.,;:])', r'\1', text)

    # Normalize multiple spaces to one
    text = re.sub(r' +', ' ', text)

    # Strip leading/trailing spaces
    text = text.strip()

    return text

# Process files
file_count = 0
for root, dirs, files in os.walk(extract_path):
    for filename in files:
        if filename.endswith(".txt"):
            raw_path = os.path.join(root, filename)
            with open(raw_path, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()

            cleaned_text = clean_text(text)

            if len(cleaned_text) > 0:
                clean_file_path = os.path.join(clean_path, filename)
                with open(clean_file_path, 'w', encoding='utf-8') as f:
                    f.write(cleaned_text)
                file_count += 1
            else:
                print("WARNING: Empty cleaned file:", filename)

print(f"Cleaning done. Total files cleaned: {file_count}")


