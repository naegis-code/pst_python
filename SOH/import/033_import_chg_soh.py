import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from tqdm import tqdm
import os
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta
from pathlib import Path  # Import Path from pathlib
import math
from dotenv import load_dotenv, find_dotenv
import socket
import subprocess
import time

# ==================== โหลดค่าจาก .env ====================
load_dotenv(find_dotenv())

def is_port_open(host, port, timeout=2):
    """เช็คว่าพอร์ต local เปิด (มีอะไร listening อยู่) หรือไม่"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        result = s.connect_ex((host, port))
        return result == 0

DB_HOST = "localhost"
DB_PORT = 5432

if is_port_open(DB_HOST, DB_PORT):
    print(f"✅ พอร์ต {DB_PORT} ที่ {DB_HOST} เปิดอยู่ — tunnel ทำงานอยู่")
else:
    print(f"❌ พอร์ต {DB_PORT} ที่ {DB_HOST} ปิดอยู่ — tunnel ยังไม่เปิด")
    subprocess.Popen(
        "start /b ssh -f -N pst-db",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    print("🔑 กำลังเปิด SSH tunnel... (รอ 5 วินาที)")
    time.sleep(5)
    print("✅ SSH tunnel เปิดแล้ว") if is_port_open(DB_HOST, DB_PORT) else print("❌ SSH tunnel ยังไม่เปิด — ตรวจสอบการเชื่อมต่อ SSH")

engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
engine2 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}")

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
timestamp_date = datetime.now().strftime('%Y%m%d')  # Use hyphens for file system safety

#timestamp_date = '20260701'
file = f'OPR-14_MSTKVAL{timestamp_date}.csv'  # File name
print("File to be processed:", file)


bu = 'CHG' # Business unit
table = 'chg_soh' # Table name for the main data
table_soh_update = 'soh_update'
path = Path('D:/Users/prthanap/Downloads') / file  # Correct way to join paths using Path

chunksize = 20000

# Count lines in file first (เพื่อรู้จำนวน chunk)
with open(path, 'r', encoding='cp874', errors='ignore') as f:
    total_lines = sum(1 for _ in f)

# ลบ header 1 line
total_rows = total_lines - 1
total_chunks = math.ceil(total_rows / chunksize)

print(f"📄 Total rows: {total_rows:,} → Processing in {total_chunks} chunks")

# total rows csv
with open(path, 'r', encoding='cp874', errors='ignore') as f:
    total_lines = sum(1 for _ in f)
total_rows = total_lines - 1  # exclude header
print(f"📄 Total rows in file: {total_rows:,}")

chunksize = 20000
print("Reading CSV in chunks with Progress Bar...")

dataframes = []

for chunk in tqdm(
        pd.read_csv(path, encoding='cp874', dtype=str, low_memory=False, chunksize=chunksize),
        total=total_rows // chunksize + 1,
        desc="📦 Importing",
        unit="chunk"
    ):

    # lowercase columns
    chunk.columns = chunk.columns.str.lower()

    # strip ' only on object columns
    obj_cols = chunk.select_dtypes(include=['object', 'string']).columns
    chunk[obj_cols] = chunk[obj_cols].apply(lambda col: col.str.strip("'"))

    chunk.to_sql(table, engine2, if_exists='append', index=False)

    dataframes.append(chunk)

df = pd.concat(dataframes, ignore_index=True)
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"✅ Data inserted into '{table}' at {timestamp}")

print("Processing data for soh_update...")

df['msstoh'] = pd.to_numeric(df['msstoh'], errors='coerce').fillna(0)
df = df[df['msstoh'] > 0]
df.rename(columns={"msstor": "stcode", "msasdt": "DATE"}, inplace=True)
df['bu'] = bu # Set the business unit
df['code'] = df['bu'] + df['stcode']

df['food_credit'] = df['msstoh'].where(df['mstype'] == '1', 0)
df['nonfood_consign'] = df['msstoh'].where(df['mstype'] == '2', 0)
df['perishable_nonmer'] = df['msstoh'].where(df['mstype'] == '3', 0)

df.rename(columns={"msstoh": "totalsoh"}, inplace=True)

df = df.groupby(["code", "bu", "stcode", "DATE"], as_index=False).sum(numeric_only=True)

try:
    df.to_sql(table_soh_update, engine1, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}' at {timestamp}")
    os.replace(path, path.with_suffix('.imported'))
    print("🗑️ File renamed to:", path.with_suffix('.imported'))
except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)

