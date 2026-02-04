import pandas as pd
import pathlib
import shutil
import json
from tqdm import tqdm


folder1 = 'stk1'
folder2 = 'stk2'
folder3 = 'var1'
folder4 = 'var2'
upload_startdate = '2025-11-19'
upload_enddate = '2025-11-25'
date_today = pd.to_datetime('today').date() - pd.Timedelta(days=1)

def standardize_folder_name(folder):
    folder.lower().replace(' ', '').replace('_', '').replace('-', '')
    # Set file path
    user_path = pathlib.Path.home()
    if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
        filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
    else:
        filepath = user_path / 'Central Group/PST Performance Team - Documents'
    # Specify the folder containing the Excel files

    file = 'upload_raw_data.xlsx'
    sheet = 'rawdata'
    usecols = 'A:M'
    path_read_file = filepath / 'Apps' / file


    # Read Excel file
    df = pd.read_excel(path_read_file, sheet_name=sheet, usecols=usecols, engine='openpyxl',dtype=str)
    # Filter columns
    df['start'] = pd.to_datetime(df['start'], errors='coerce') + pd.Timedelta(hours=7)
    df = df[df['start'] >= pd.to_datetime(date_today, errors='coerce')]
    keep_columns = ['Id', 'bu', 'stcode', 'cntdate', folder.lower()]
    df = df[keep_columns]
    df = df[df[folder.lower()].notnull()]

    path_onedrive = user_path / 'OneDrive - Central Group' / 'Apps' / 'Microsoft Forms' / 'Upload Raw DATA' / folder.upper()


    # Extract filenames from JSON
    def extract_filenames(json_str):
        # ... (ฟังก์ชันเดิม)
        try:
            data = json.loads(json_str)
            return [item['name'] for item in data if 'name' in item]
        except Exception:
            return []

    df['filename_list'] = df[folder].apply(extract_filenames)

    # Explode filenames into separate rows
    df = df.explode('filename_list').rename(columns={'filename_list': 'original_filename'})

    # Add file type
    df['file_type'] = df['original_filename'].str.extract(r'(\.[^.]+)$', expand=False).str.lower()
    df = df[df['file_type'].isin(['.xlsx', '.xls'])].copy() # 💡 เพิ่ม .copy() เพื่อหลีกเลี่ยง SettingWithCopyWarning

    if df.empty:
        print(f"No files found for folder '{folder}'. datafream is empty further processing.")
        return

    # 1. สร้างชื่อไฟล์หลัก (Base Filename) สำหรับจัดกลุ่ม
    # Base filename: folder_BU_STCODE_CNTDATETIME.EXT
    df['base_filename'] = df.apply(
        lambda row: f"{folder.upper()}_{row['bu']}_{row['stcode']}_{pd.to_datetime(row['cntdate'], errors='coerce').strftime('%Y%m%d')}{row['file_type']}",
        axis=1
    )

    # 2. นับไฟล์ซ้ำซ้อนโดยใช้ base_filename
    # นับจำนวนไฟล์ที่ชื่อหลักซ้ำกัน
    df['dup_count'] = df.groupby('base_filename').cumcount()

    # 3. สร้างชื่อไฟล์ที่ไม่ซ้ำกันโดยเพิ่มส่วนต่อท้าย
    def add_suffix(row):
        if row['dup_count'] == 0:
            return row['base_filename']
        else:
            stem = pathlib.Path(row['base_filename']).stem
            suffix = pathlib.Path(row['base_filename']).suffix
            return f"{stem}_{row['dup_count']}{suffix}"

    df['unique_filename'] = df.apply(add_suffix, axis=1)

    # 4. สร้างพาธปลายทาง (path_target) ที่ถูกต้องโดยใช้ unique_filename
    base_upload_path = filepath / 'Shared' / 'Performance' / 'report' / 'rawdata'
    df['path_target'] = df.apply(
        lambda row: base_upload_path / row['bu'] / row['unique_filename'],
        axis=1
    )

    # 5. กำหนดพาธต้นทาง (Source)
    df['destination'] = path_onedrive / df['original_filename']

    df['path_target'] = df.apply(lambda row: filepath / 'Shared' / 'Performance' / 'report' / 'rawdata' / row['bu'] / 
                                f"{folder.upper()}_{row['bu']}_{row['stcode']}_{pd.to_datetime(row['cntdate'], errors='coerce').strftime('%Y%m%d')}_{row['dup_count']}{row['file_type']}", 
                                axis=1
                                )

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

standardize_folder_name(folder1)
standardize_folder_name(folder2)
standardize_folder_name(folder3)
standardize_folder_name(folder4)


