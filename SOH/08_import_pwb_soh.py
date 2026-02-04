import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from tqdm import tqdm
import os
from sqlalchemy.exc import SQLAlchemyError
from db_connect import db_url_pstdb,db_url_pstdb2
from pathlib import Path  # Import Path from pathlib

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
timestamp_date = datetime.now().strftime('%Y%m%d')  # Use hyphens for file system safety
file = f'14_MSTKVAL_{timestamp_date}.csv'  # File name

bu = 'PWB' # Business unit
table = 'pwb_soh' # Table name for the main data
table_soh_update = 'soh_update'
path = Path('D:/Users/prthanap/Downloads') / file  # Correct way to join paths using Path
df = pd.read_csv(path, encoding='cp874')
df.columns = df.columns.str.lower()
df = df.map(lambda x: x.strip("'") if isinstance(x, str) else x)

print(f"Running {table} at {timestamp} with {len(df)} rows")

try:
    # Create database connection
    engine = create_engine(db_url_pstdb)
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

keep_columns = ["msstor", "mstype", "msasdt", "msstoh"]
df = df[keep_columns]
df = df[df['msstoh'] > 0]
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
    
except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)

