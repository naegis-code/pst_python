import pandas as pd
from sqlalchemy import create_engine
import db_connect
from tqdm import tqdm
from datetime import datetime

bu = 'ssp'
table = 'stk'
date = '20250101'
chunk_size = 10000

db3 = create_engine(db_connect.db_url_pstdb3)
q_db3 = f"""
    SELECT 
        store,countname,sku,ibc,sbc,
        brandid,sku_des,catalogue,vendor,vend_nam,
        dpt,sdpt,cls,scls,retail,
        cost,soh,qty_count,qty_var,percen_var,
        phycnt_rtl,phycnt_cst,extrtl_var,extcst_var,gm,
        vnd_vat_rate,coldsc,sizdsc,brnnam,skutype,
        cntdate,rpname
    FROM {bu}_{table}_this_year
"""

df_db3 = pd.read_sql(q_db3, db3)

db = create_engine(db_connect.db_url_pstdb)
q_db = f"""
    SELECT DISTINCT
        store, cntdate, skutype, rpname
    FROM {bu}_{table}
    WHERE cntdate >= '{date}'
"""
df_db = pd.read_sql(q_db, db)

# Anti-join
keys = ['store', 'cntdate', 'skutype', 'rpname']
mask = ~df_db3.set_index(keys).index.isin(df_db.set_index(keys).index)
df = df_db3[mask].reset_index(drop=True)

# Insert with progress bar
with tqdm(total=len(df), desc="Inserting", unit="rows") as pbar:
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start+chunk_size]
        chunk.to_sql(
            f"{bu}_{table}",
            db,
            if_exists='append',
            index=False
        )
        pbar.update(len(chunk))

print(f"✅ Data {len(df)} inserted into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
