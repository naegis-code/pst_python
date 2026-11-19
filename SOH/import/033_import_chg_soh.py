import pandas as pd
from copy_util import copy_df
from sqlalchemy import create_engine
from datetime import datetime
import os
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta
from pathlib import Path  # Import Path from pathlib
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
file = f'14_MSTKVAL{timestamp_date}.csv'  # File name
print("File to be processed:", file)


bu = 'CHG' # Business unit
table = 'chg_soh' # Table name for the main data
table_soh_update = 'soh_update'
path = Path('D:/Users/prthanap/Downloads') / file  # Correct way to join paths using Path

print("Reading CSV...")
df = pd.read_csv(path, encoding='cp874', encoding_errors='replace', dtype=str, low_memory=False)
print(f"📄 Total rows in file: {len(df):,}")

# lowercase columns
df.columns = df.columns.str.lower()

# strip ' only on object columns
obj_cols = df.select_dtypes(include=['object', 'string']).columns
df[obj_cols] = df[obj_cols].apply(lambda col: col.str.strip("'"))
n = copy_df(engine2, table, df)
print(f"Copied {n:,} rows into {table}")
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

