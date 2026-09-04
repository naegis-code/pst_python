import pandas as pd
from sqlalchemy import create_engine,text
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv,find_dotenv
import os

load_dotenv(find_dotenv())
engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
engine2 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}")
engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")

bu = 'chg'
table = 'stk'
date = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime('%Y%m%d')
chunk_size = 10000

print(date)

'''
chg_db3 = create_engine(db_connect.db_url_pstdb3)
q_chg_db3 = f"""
SELECT 
    cntnum, stmerch, cntdate, deptcode, deptname, subdeptcode, subdeptname,
    sku, sbc, ibc, bndname, prname, prmodel,
    soh, cntqnt, varianceqnt,
    extphycnt_retail, extphycnt_cost,
    extphy_retailvar, extphy_costvar,
    skutype, rpname 
FROM {bu}_{table}_this_year
"""
df_chg_db3 = pd.read_sql(q_chg_db3, chg_db3)

chg_db = create_engine(db_connect.db_url_pstdb)
q_chg_db = f"""
SELECT DISTINCT
    stmerch, cntdate, skutype, rpname
FROM {bu}_{table}
WHERE cntdate >= '{date}'
"""
df_chg_db = pd.read_sql(q_chg_db, chg_db)

# Anti-join
keys = ['stmerch', 'cntdate', 'skutype', 'rpname']
mask = ~df_chg_db3.set_index(keys).index.isin(df_chg_db.set_index(keys).index)
df = df_chg_db3[mask].reset_index(drop=True)

print(df.shape)
print(df.head(3))

# Insert with progress bar
with tqdm(total=len(df), desc="Inserting", unit="rows") as pbar:
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start+chunk_size]
        chunk.to_sql(
            f"{bu}_{table}",
            chg_db,
            if_exists='append',
            index=False
        )
        pbar.update(len(chunk))

print(f"✅ Data {len(df)} inserted into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
'''