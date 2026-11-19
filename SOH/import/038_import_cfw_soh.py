import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from copy_util import copy_df
import os

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tunnel import engine1, engine2, ensure_tunnel, DB_CONN_STR  # engine1=pstdb, engine2=pstdb2

ensure_tunnel(DB_CONN_STR)

bu = 'CFW'
path = Path.home() / 'Downloads' / 'cfw_soh.xlsx'
table = 'cfw_soh'
table_soh_update = 'soh_update'

df = pd.read_excel(path, engine='openpyxl', dtype=str, parse_dates=['MSASDT'])

df.columns = df.columns.str.lower()
df['msasdt'] = pd.to_datetime(df['msasdt'], errors='coerce').dt.date
df['msstoh'] = pd.to_numeric(df['msstoh'], errors='coerce').fillna(0)
df['msstkc'] = pd.to_numeric(df['msstkc'], errors='coerce').fillna(0)

keep_columns = ['msstor', 'msstrn', 'mstrnc', 'mstrnd', 'mstype', 'msvdno', 'msvdnm', 'msdept', 'msdptn', 'mssdpt',
                'mssdpn','msbrnd', 'msclas', 'msclsn', 'msscls', 'msscln', 'mssku', 'mssdes', 'msibc', 'msstoh',
                'msstkc', 'msasdt', 'mspoor', 'mstoor', 'mssbc']

# คอลัมน์ที่ขาดไม่ได้ ถ้าไม่มีให้หยุดทันที (ไม่ข้ามเงียบๆ)
required_columns = ['msstor', 'msstoh', 'msasdt']
missing_required = [c for c in required_columns if c not in df.columns]
if missing_required:
    raise RuntimeError(f"ไฟล์ขาดคอลัมน์ที่จำเป็น: {missing_required}")

# ข้ามคอลัมน์ที่ไม่อยู่ใน keep_columns (คอลัมน์เสริมที่ไม่มีไม่เป็นไร)
df = df[[col for col in keep_columns if col in df.columns]]

if df['msasdt'].isna().any():
    raise RuntimeError(f"พบ msasdt ที่แปลงเป็นวันที่ไม่ได้ {int(df['msasdt'].isna().sum())} แถว — ตรวจไฟล์ก่อน import")

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Running {table} at {timestamp} with {len(df)} rows")

# ==================== INSERT RAW DATA (pstdb2) ====================
# ถ้าขั้นนี้ล้มเหลวให้หยุดทั้งสคริปต์ ไม่ไปต่อที่ soh_update และไม่ rename ไฟล์
try:
    n = copy_df(engine2, table, df)
    print(f"Copied {n:,} rows into {table}")

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Data {table} imported to database successfully at {timestamp}")
except SQLAlchemyError as e:
    raise RuntimeError(f"DB insert failed ({table}): {e}") from e

# ==================== TRANSFORM ====================
# msasdt เป็น date อยู่แล้ว (แปลงไว้ด้านบน) จึงแปลงเป็น YYYYMMDD ตรงๆ
# (เดิมส่ง format='%Y%m%d' ให้ค่า date ทำให้กลายเป็น NaT แล้ว groupby ทิ้งทุกแถว)
df['msasdt'] = pd.to_datetime(df['msasdt']).dt.strftime('%Y%m%d')

if 'mstype' not in df.columns:
    df['mstype'] = '1'
# '01' -> '1' เพื่อให้เทียบกับ '1','2','3' ได้
df['mstype'] = df['mstype'].astype(str).str.strip().str.lstrip('0')

keep_columns = ["msstor", "mstype", "msasdt", "msstoh"]
df = df[keep_columns].copy()

df = df.rename(columns={"msstor": "stcode", "msasdt": "DATE"})
df['bu'] = bu
df['code'] = df['bu'] + df['stcode']

df['food_credit'] = df['msstoh'].where(df['mstype'] == '1', 0)
df['nonfood_consign'] = df['msstoh'].where(df['mstype'] == '2', 0)
df['perishable_nonmer'] = df['msstoh'].where(df['mstype'] == '3', 0)

df = df.rename(columns={"msstoh": "totalsoh"})

df = df.groupby(["code", "bu", "stcode", "DATE"], as_index=False).sum(numeric_only=True)

if df.empty:
    raise RuntimeError("ไม่มีแถวเหลือสำหรับ soh_update หลัง transform — ตรวจข้อมูลต้นทาง")

print(df.head())

# ==================== INSERT SOH UPDATE (pstdb) ====================
try:
    df.to_sql(table_soh_update, engine1, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}' at {timestamp} ({len(df)} rows)")
    os.replace(path, path.with_suffix('.imported'))
    print("🗑️ File renamed to:", path.with_suffix('.imported'))
except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT renamed:", path)
