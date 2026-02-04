import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from tqdm import tqdm
import os
import pathlib
from db_connect import db_url_pstdb, db_url_pstdb2

# ---------------- CONFIG ----------------
bu = 'SSP'
path = pathlib.Path.home() / 'Documents' / 'soh' / 'wds'
table = 'sspwds_soh'
table_soh_update = 'soh_update'
chunk_size = 1000
# ---------------------------------------

# ---------- READ EXCEL FILES ----------
excels = [
    path / f for f in os.listdir(path)
    if f.endswith(('.xlsx', '.xls')) and (path / f).stat().st_size > 0
]

dataframes = []

for excel in excels:
    try:
        print(f"Reading: {excel}")
        df_temp = pd.read_excel(excel, dtype=str, sheet_name='Sheet1')
        dataframes.append(df_temp)
    except Exception as e:
        print(f"❌ Failed to read {excel}: {e}")

if not dataframes:
    raise RuntimeError("No Excel files loaded")

df = pd.concat(dataframes, ignore_index=True)

# ---------- RENAME COLUMNS ----------
rename_columns = {
    'รหัสกลุ่มลูกค้า':'groupcuscode','ชื่อกลุ่มลูกค้า':'groupcuname',
    'รหัสลูกค้า':'cuscode','ชื่อลูกค้า':'cusname','ชื่อสั้นลูกค้า':'cusssname',
    'SKU':'sku','Barcode IBC':'barcodeibc','Barcode SBC':'barcodesbc',
    'ชื่อสินค้า':'description','ยี่ห้อ':'brand','รุ่น':'model','สี':'colour',
    'ขนาด':'size','Stock':'soh','จำนวนคงค้างออก Pre-order CN':'preordercn',
    'ราคาปกติ':'retail','ราคา Promotion':'retailpromotion','GP ปกติ':'gp',
    'GP Promotion':'gppromotion','ราคาทุนต่อหน่วย':'cost',
    'รหัส Dept':'dept','ชื่อ Dept':'deptname',
    'รหัส Sub Dept':'sdept','ชื่อ Sub Dept':'sdeptname',
    'รหัส Class':'class','ชื่อ Class':'classname',
    'รหัส Sub Class':'sclass','ชื่อ Sub Class':'sclassname',
    'วันที่แสดงข้อมูล':'data_date'
}

df = df.rename(columns=rename_columns)

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Running {table} at {timestamp} with {len(df)} rows")

# ---------- INSERT RAW DATA ----------
engine2 = create_engine(db_url_pstdb2)

try:
    total_chunks = len(df) // chunk_size + (len(df) % chunk_size > 0)

    with engine2.begin() as conn, tqdm(total=total_chunks, desc="Inserting Data", unit="chunk") as pbar:
        for i in range(0, len(df), chunk_size):
            df.iloc[i:i+chunk_size].to_sql(
                table, conn, if_exists='append', index=False
            )
            pbar.update(1)

    print(f"✅ Data imported to {table}")

except SQLAlchemyError as e:
    raise RuntimeError(f"DB insert failed: {e}")

# ---------- LOAD CUSTOMER MAP ----------
query = "SELECT cuscode, stcode FROM ssp_wds_cuscode"
df_cuscode = pd.read_sql(query, con=engine2)

df = df.merge(df_cuscode, on='cuscode', how='left')

# ---------- TRANSFORM ----------
df['soh'] = pd.to_numeric(df['soh'], errors='coerce')
df = df[df['soh'] > 0]

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

# ---------- INSERT SOH UPDATE ----------
engine1 = create_engine(db_url_pstdb)

try:
    df.to_sql(table_soh_update, engine1, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}'")

    # ---------- DELETE FILES ----------
    for file in path.iterdir():
        if file.is_file():
            file.unlink()
            print(f"🗑️ Removed: {file}")

except SQLAlchemyError as e:
    print("❌ Failed to insert soh_update")
    print(e)
