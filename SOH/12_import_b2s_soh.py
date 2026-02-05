import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from tqdm import tqdm
import os
from db_connect import db_url_pstdb, db_url_pstdb2
from sqlalchemy.exc import SQLAlchemyError
import pathlib

bu = 'B2S'
path = pathlib.Path.home() / 'Documents' / 'b2s_soh.csv'
table = 'b2s_soh'
table_soh_update = 'soh_update'
engine = create_engine(db_url_pstdb)
# Define column names
column_names = [
    "msstor", "msstrn", "mstrnc", "mstrnd", "mstype", "msvdno", "msvdnm", "msdept",
    "msdptn", "mssdpt", "mssdpn", "msbrnd", "msclas", "msclsn", "msscls", "msscln",
    "mssku", "mssdes", "msibc", "mssbc", "mspopg", "mscatl", "msskus", "msstkr",
    "msstkc", "msstoh", "msregp", "msorgp", "msancp", "msascn", "msasdt", "mspoor",
    "mstoor", "msrtvv", "msrtvi", "msdist", "msobsf", "msmqty", "msage", "msaget",
    "att_nam_1", "att_val_1", "att_desc_1", "att_nam_2", "att_val_2", "att_desc_2",
    "att_nam_3", "att_val_3", "att_desc_3", "att_nam_4", "att_val_4", "att_desc_4",
    "att_nam_5", "att_val_5", "att_desc_5", "att_nam_6", "att_val_6", "att_desc_6",
    "att_nam_7", "att_val_7", "att_desc_7", "preord", "mbyum", "mslum", "mstdpk",
    "rpl_code", "special_attribute"
]

# Read CSV with specified column names
df = pd.read_csv(path, encoding='cp874', header=None, dtype=str, names=column_names)

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


df['msstoh'] = pd.to_numeric(df['msstoh'], errors='coerce') # Convert to float, invalid entries become NaN
df = df[df['msstoh'] > 0]
#df = df[(df['mssdpt'] != '600') & (df['mssdpt'] != '700') &(df['msvdno'] != '530250')]
df = df[(df['mssdpt'] != '600') & (df['mssdpt'] != '700')]


#==============================================================Setp Block Vendors=========================================================
# Query blocked vendors for 'all' sdpt
query_block_all = text("""
                    select veno,sdpt from soh_b2s_block_veno where sdpt = 'all'
                   """
)
df_block_all = pd.read_sql_query(query_block_all, engine)
# Merge and filter out blocked vendors
df = df.merge(df_block_all, left_on='msvdno', right_on='veno', how='left', indicator=True)
# Keep only rows not in blocked vendors
df = df[df['_merge'] == 'left_only']
# Drop unnecessary columns
df.drop(columns=['_merge', 'veno', 'sdpt'], inplace=True)

query_block_sdpt = text("""
                    select veno,sdpt from soh_b2s_block_veno where sdpt != 'all'
                   """)
df_block_sdpt = pd.read_sql_query(query_block_sdpt, engine)
# Merge and filter out blocked vendors for specific sdpt
df = df.merge(df_block_sdpt, left_on=['msvdno', 'mssdpt'], right_on=['veno', 'sdpt'], how='left', indicator=True)
# Keep only rows not in blocked vendors for specific sdpt
df = df[df['_merge'] == 'left_only']
# Drop unnecessary columns
df.drop(columns=['_merge', 'veno', 'sdpt'], inplace=True)
#==============================================================End Block Vendors=========================================================


keep_columns = ["msstor", "mstype", "msasdt", "msstoh"]
df = df[keep_columns]

df.rename(columns={"msstor": "stcode", "msasdt": "DATE"}, inplace=True)
df['bu'] = bu # Set the business unit
df['code'] = df['bu'] + df['stcode']
df['DATE'] = '20' + df['DATE']

df['food_credit'] = df['msstoh'].where(df['mstype'] == '01', 0)
df['nonfood_consign'] = df['msstoh'].where(df['mstype'] == '02', 0)
df['perishable_nonmer'] = df['msstoh'].where(df['mstype'] == '03', 0)

df.rename(columns={"msstoh": "totalsoh"}, inplace=True)

df = df.groupby(["code", "bu", "stcode", "DATE"], as_index=False).sum(numeric_only=True)
print(df)

try:
    df.to_sql(table_soh_update, engine, if_exists='append', index=False)
    print(f"✅ Data inserted into '{table_soh_update}' at {timestamp}")
    os.rename(path, path.with_suffix('.imported'))
    print("🗑️ File renamed to:", path.with_suffix('.imported'))
except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)
