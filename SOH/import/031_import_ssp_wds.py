import sys
import os
import pathlib
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from copy_util import copy_df

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tunnel import engine1, engine2, ensure_tunnel, DB_CONN_STR  # engine1=pstdb, engine2=pstdb2

ensure_tunnel(DB_CONN_STR)

# ==================== CONFIG ====================
bu = 'SSP'
path = pathlib.Path.home() / 'Documents' / 'soh' / 'wds'
table = 'sspwds_soh'
table_soh_update = 'soh_update'
# ==================================================

# ==================== READ EXCEL FILES ====================
if not path.exists():
    raise FileNotFoundError(f"ไม่พบโฟลเดอร์: {path}")

excels = [
    path / f for f in os.listdir(path)
    if f.endswith(('.xlsx', '.xls')) and (path / f).stat().st_size > 0
]

if not excels:
    raise RuntimeError(f"ไม่พบไฟล์ Excel ในโฟลเดอร์: {path}")

dataframes = []
for excel in excels:
    try:
        print(f"[*] Reading: {excel}")
        df_temp = pd.read_excel(excel, dtype=str, sheet_name='Sheet1')
        dataframes.append(df_temp)
    except Exception as e:
        print(f"❌ Failed to read {excel}: {e}")

if not dataframes:
    raise RuntimeError("No Excel files loaded")

df = pd.concat(dataframes, ignore_index=True)

# ==================== RENAME COLUMNS ====================
rename_columns = {
    'รหัสกลุ่มลูกค้า': 'groupcuscode', 'ชื่อกลุ่มลูกค้า': 'groupcuname',
    'รหัสลูกค้า': 'cuscode', 'ชื่อลูกค้า': 'cusname', 'ชื่อสั้นลูกค้า': 'cusssname',
    'SKU': 'sku', 'Barcode IBC': 'barcodeibc', 'Barcode SBC': 'barcodesbc',
    'ชื่อสินค้า': 'description', 'ยี่ห้อ': 'brand', 'รุ่น': 'model', 'สี': 'colour',
    'ขนาด': 'size', 'Stock': 'soh', 'จำนวนคงค้างออก Pre-order CN': 'preordercn',
    'ราคาปกติ': 'retail', 'ราคา Promotion': 'retailpromotion', 'GP ปกติ': 'gp',
    'GP Promotion': 'gppromotion', 'ราคาทุนต่อหน่วย': 'cost',
    'รหัส Dept': 'dept', 'ชื่อ Dept': 'deptname',
    'รหัส Sub Dept': 'sdept', 'ชื่อ Sub Dept': 'sdeptname',
    'รหัส Class': 'class', 'ชื่อ Class': 'classname',
    'รหัส Sub Class': 'sclass', 'ชื่อ Sub Class': 'sclassname',
    'วันที่แสดงข้อมูล': 'data_date'
}
df = df.rename(columns=rename_columns)

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"[*] Running {table} at {timestamp} with {len(df)} rows")

# ==================== INSERT RAW DATA ====================
try:
    n = copy_df(engine2, table, df)
    print(f"✅ Data imported to {table} ({n:,} rows)")

except SQLAlchemyError as e:
    raise RuntimeError(f"DB insert failed: {e}")

# ==================== LOAD CUSTOMER MAP ====================
print("[*] Loading customer map...")
query = "SELECT cuscode, stcode FROM ssp_wds_cuscode"
df_cuscode = pd.read_sql(query, con=engine2)
print(f"[*] Loaded {len(df_cuscode)} customer records")

df = df.merge(df_cuscode, on='cuscode', how='left')

# ==================== TRANSFORM ====================
df['soh'] = pd.to_numeric(df['soh'], errors='coerce')
df = df[df['soh'] > 0]

# cuscode ที่ไม่มี mapping จะได้ stcode เป็น NaN แล้วหายตอน groupby — เตือนให้เห็น
unmapped = df[df['stcode'].isna()]
if not unmapped.empty:
    print(f"⚠️ พบ {len(unmapped)} แถวที่ cuscode ไม่มีใน ssp_wds_cuscode (จะไม่ถูกนับใน soh_update):")
    print(unmapped[['cuscode', 'cusname']].drop_duplicates().to_string(index=False))
    df = df[df['stcode'].notna()]

df = df[['stcode', 'data_date', 'soh']]
df = df.rename(columns={'data_date': 'DATE'})

df['bu'] = bu
df['code'] = df['bu'] + df['stcode']

df['food_credit'] = df['soh']
df['nonfood_consign'] = 0
df['perishable_nonmer'] = 0
df['totalsoh'] = df['soh']

df = df.drop(columns='soh')

df = df.groupby(
    ['code', 'bu', 'stcode', 'DATE'],
    as_index=False
).sum(numeric_only=True)

# ==================== INSERT SOH UPDATE ====================
print("[*] Inserting into soh_update...")

try:
    df.to_sql(table_soh_update, engine1, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}'")

    # ==================== DELETE FILES ====================
    # ลบทุกไฟล์ในโฟลเดอร์ (รวมไฟล์ว่าง 0 KB ที่ถูกข้ามตอนอ่าน)
    for file in path.iterdir():
        if file.is_file():
            try:
                file.unlink()
                print(f"🗑️ Removed: {file}")
            except PermissionError as e:
                print(f"⚠️ ลบไม่ได้ (ไฟล์อาจเปิดอยู่): {file} — {e}")

except SQLAlchemyError as e:
    print("❌ Failed to insert soh_update")
    print(e)