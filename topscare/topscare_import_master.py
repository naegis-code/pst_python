import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import db_connect

filename = 'CHK7039-140526-201214.txt'
path = 'C:/Users/Soulyer/Documents/'

filepath = path + filename

print("Reading file:", filepath)

df = pd.read_csv(filepath, delimiter=',', encoding='cp874',dtype=str,header=None,skiprows=1)

column_names = ['stcode', 'cntnum', 'barcode', 'sku', 'description', 'pack', 'qnt', 'price', 'location', 'sdate', 'stime']

df.columns = column_names

df.dropna(subset=['barcode'], inplace=True)

engine = create_engine(db_connect.db_url_pstdb3)

# --- เพิ่ม pbar สำหรับการ insert ---
chunksize = 1000  # กำหนดขนาด Batch
num_chunks = (len(df) // chunksize) + int(len(df) % chunksize != 0)

with tqdm(total=len(df), desc='Uploading', unit='rows') as pbar:
    for start in range(0, len(df), chunksize):
        end = min(start + chunksize, len(df))
        df.iloc[start:end].to_sql('topscare_master', con=engine, if_exists='append', index=False)
        pbar.update(end-start)

print("Upload completed.", len(df), "rows uploaded.")
