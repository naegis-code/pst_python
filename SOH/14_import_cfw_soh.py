import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from tqdm import tqdm
import os
from db_connect import db_url_pstdb, db_url_pstdb2
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path

bu = 'CFW'
path = Path('D:/Users/prthanap/Downloads/cfw_soh.xlsx')
table = 'cfw_soh'
table_soh_update = 'soh_update'

dtype_dict = {
    'MSSTOR': str, 'MSSTRN': str, 'MSTRNC': str, 'MSTRND': str, 'MSTYPE': str, 'MSVDNO': str, 'MSVDNM': str,
    'MSDEPT': str, 'MSDPTN': str, 'MSSDPT': str, 'MSSDPN': str, 'MSBRND': str, 'MSCLAS': str, 'MSCLSN': str,
    'MSSCLS': str, 'MSSCLN': str, 'MSSKU': str, 'MSSDES': str, 'MSIBC': str, 'MSSTOH': float, 'MSSTKC': float,
    'MSPOOR': float, 'MSTOOR': float
}

df = pd.read_excel(path, engine='openpyxl', dtype=dtype_dict, parse_dates=['MSASDT'])

df.columns = df.columns.str.lower()
df['msasdt'] = pd.to_datetime(df['msasdt'], errors='coerce').dt.date


timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Running {table} at {timestamp} with {len(df)} rows")

try:
    # Create database connection
    engine = create_engine(db_url_pstdb2)
    conn = engine.connect()
    
    # Use chunks for efficient insertion
    chunk_size = 1000  # Adjust based on performance
    total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size > 0 else 0)
    
    with tqdm(total=total_chunks, desc="Inserting Data", unit="chunk") as pbar:
        for i in range(0, len(df), chunk_size):
            df.iloc[i:i+chunk_size].to_sql(table, con=conn, if_exists='append', index=False)
            pbar.update(1)
    
    conn.close()
    
    # Success message
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Data {table} imported to database successfully at {timestamp}")
except FileNotFoundError:
	print("Error: The specified file was not found.")
except SQLAlchemyError as e:
	print(f"Database error occurred: {e}")
except Exception as e:
	print(f"An unexpected error occurred: {e}")

df['msasdt'] = pd.to_datetime(df['msasdt'], format='%Y%m%d', errors='coerce').dt.strftime('%Y%m%d')

keep_columns = ["msstor", "mstype", "msasdt", "msstoh"]
df = df[keep_columns]

df.rename(columns={"msstor": "stcode", "msasdt": "DATE"}, inplace=True)
df['bu'] = bu # Set the business unit
df['code'] = df['bu'] + df['stcode']


df['food_credit'] = df['msstoh'].where(df['mstype'] == '1', 0)
df['nonfood_consign'] = df['msstoh'].where(df['mstype'] == '2', 0)
df['perishable_nonmer'] = df['msstoh'].where(df['mstype'] == '3', 0)

df.rename(columns={"msstoh": "totalsoh"}, inplace=True)

df = df.groupby(["code", "bu", "stcode", "DATE"], as_index=False).sum(numeric_only=True)

engine = create_engine(db_url_pstdb)
try:
    df.to_sql(table_soh_update, engine, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}' at {timestamp}")
    
    # ลบไฟล์หลังจากนำเข้าข้อมูลสำเร็จ
    os.remove(path)
    print("🗑️ File deleted:", path)

except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)