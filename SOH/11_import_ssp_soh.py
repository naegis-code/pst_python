import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from tqdm import tqdm
import os
from db_connect import db_url_pstdb, db_url_pstdb2
from sqlalchemy.exc import SQLAlchemyError
import pathlib

bu = 'SSP'
path = pathlib.Path.home() / 'Documents' / 'ssp_soh.csv'
table = 'ssp_soh'
table_soh_update = 'soh_update'
# Define column names
column_names = [
    "store","store_name","t_c","t_date","sku_t","vendor","vendor_name","dept","dept_name","sub_dept","sub_dept_name",
    "brand_description","class","class_name","sub_class","sub_class_name","sku","sku_description","ibc","sbc","popg",
    "catalogue","sts","stock_retail","stock_cost","soh","reg_ret","ori_ret","ancp","c","as_of_date","po_on_ord","to_on_ord",
    "rtv_vnd","rtv_item","dis_typ","color_desc","size_desc","att_nam_1","att_val_1","att_desc_1","att_nam_2","att_val_2",
    "att_desc_2","att_nam_3","att_val_3","att_desc_3","att_nam_4","att_val_4","att_desc_4","att_nam_5","att_val_5","att_desc_5",
    "att_nam_6","att_val_6","att_desc_6","att_nam_7","att_val_7","att_desc_7","preord","mbyum","mslum","mstdpk","unavail_qty","avail_qty"
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
      

df['soh'] = pd.to_numeric(df['soh'], errors='coerce') # Convert to float, invalid entries become NaN
df = df[df['soh'] > 0]
#df = df[(df['mssdpt'] != '600') & (df['mssdpt'] != '700')]

keep_columns = ["store", "sku_t", "as_of_date", "soh"]
df = df[keep_columns]
df.rename(columns={"store": "stcode", "as_of_date": "DATE"}, inplace=True)
df['bu'] = bu # Set the business unit
df['code'] = df['bu'] + df['stcode']
df['DATE'] = '20' + df['DATE']

df['food_credit'] = df['soh'].where(df['sku_t'] == '01', 0)
df['nonfood_consign'] = df['soh'].where(df['sku_t'] == '02', 0)
df['perishable_nonmer'] = df['soh'].where(df['sku_t'] == '03', 0)

# totalsoh is only for '01' and '03'
df['totalsoh'] = df['soh'].where(df['sku_t'].isin(['01']), 0)

# Drop original msstoh if no longer needed
df.drop('soh', axis=1, inplace=True)

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