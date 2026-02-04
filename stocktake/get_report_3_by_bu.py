import pandas as pd
import pathlib
from datetime import datetime
import json
import shutil
import os
from tqdm import tqdm


bu = 'CFR'
start_date = datetime(2025, 11, 17)
end_date = datetime(2025, 11, 18)

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path = filepath / 'Apps' / 'Report_Statistics4.xlsx'

# Load data
df = pd.read_excel(path, sheet_name='Report_Statistics4')
# Filter columns
keep_columns = ['ID', 'Email', 'StartTime', 'BU', 'StoreCode', 'CNTDATE', 'Report3']
df = df[keep_columns]
df = df[df['Report3'].notnull()]
df = df[df['BU'] == bu]
df['StartTime'] = pd.to_datetime(df['StartTime'], errors='coerce')
df = df[df['StartTime'] >= start_date]
df = df[df['StartTime'] <= end_date]

# Extract filenames from JSON
def extract_filenames(json_str):
    try:
        data = json.loads(json_str)
        return [item['name'] for item in data if 'name' in item]
    except Exception:
        return []

df['filename_list'] = df['Report3'].apply(extract_filenames)

# Explode filenames into separate rows
df = df.explode('filename_list').rename(columns={'filename_list': 'original_filename'})

# Add file type
df['file_type'] = df['original_filename'].str.extract(r'(\.[^.]+)$', expand=False)

# Build path_target
#base_path = pathlib.Path('D:/report/report3')
df['path_target'] = df.apply(lambda row: filepath / 'Shared' / 'Performance' / 'report' / 'report3' / row['BU'] / f"Report3_{row['BU']}_{row['StoreCode']}_{row['CNTDATE'].strftime('%Y%m%d')}{row['file_type']}", axis=1)

# Extract just the filename from the path
df['filename'] = df['path_target'].apply(lambda x: x.name)

# Count duplicates and add suffix
df['dup_count'] = df.groupby('filename').cumcount()

def add_suffix(row):
    if row['dup_count'] == 0:
        return row['filename']
    else:
        stem = pathlib.Path(row['filename']).stem
        suffix = pathlib.Path(row['filename']).suffix
        return f"{stem}_{row['dup_count']}{suffix}"

df['unique_filename'] = df.apply(add_suffix, axis=1)
df['file_type'] = df['file_type'].str.lower()
df = df[df['file_type'].isin(['.xlsx', '.xls'])]
df['destination'] = user_path / 'OneDrive - Central Group' / 'Apps' / 'Microsoft Forms' / 'Upload Report' / 'Report 3' / df['original_filename']
df['path_target'] = df.apply(lambda row: filepath / 'Shared' / 'Performance' / 'report' / 'report3' / row['BU'] / f"Report3_{row['BU']}_{row['StoreCode']}_{row['CNTDATE'].strftime('%Y%m%d')}_{row['dup_count']}{row['file_type']}", axis=1)

# Copy files from 'file_destination' to 'path_file_target'
for index, row in tqdm(df.iterrows(), total=len(df), desc="Copying files"):
    try:
        if pathlib.Path(row['destination']).exists():
            target_path = pathlib.Path(row['path_target'])
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(row['destination'], target_path)
            tqdm.write(f"Copied {row['destination']} to {target_path}") 
        else:
            tqdm.write(f"Source file {row['destination']} does not exist.")  # ใช้ tqdm.write แทนการ print จะได้ไม่มี progress bar หลายบรรทัด
    except Exception as e:
        tqdm.write(f"Error copying file {row['destination']} to {row['path_target']}: {e}")