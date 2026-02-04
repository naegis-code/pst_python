import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from tqdm import tqdm
import os
from sqlalchemy.exc import SQLAlchemyError
from db_connect import db_url_pstdb,db_url_pstdb2
from datetime import timedelta
from pathlib import Path  # Import Path from pathlib
import math

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
timestamp_date = datetime.now().strftime('%Y%m%d')  # Use hyphens for file system safety
timestamp_date_1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # Previous day
#timestamp_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # Previous day
file = f'14_MSTKVAL{timestamp_date}.csv'  # File name
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

engine2 = create_engine(db_url_pstdb2)
engine = create_engine(db_url_pstdb)

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
    df.to_sql(table_soh_update, engine, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}' at {timestamp}")

except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)

