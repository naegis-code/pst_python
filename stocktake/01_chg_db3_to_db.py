import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
from tqdm import tqdm

bu = 'chg'
date_start = '20250501'
date_end = '20250531'

chunksize = 10000

db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

def var_db3_to_db(bu, date_start, date_end):
    table = 'var'
    # เตียมข้อมูลจาก db3
    q_db3 = text(f"""
    SELECT *
    FROM {bu}_{table}_this_year
    where cntdate between '{date_start}' and '{date_end}'
    """)
    df_db3 = pd.read_sql(q_db3, db3)
    
    # เตียมข้อมูลจาก db
    q_db = f"""
    SELECT distinct bu,stcode,cntdate,skutype,rpname
    FROM {bu}_{table}
    WHERE cntdate between '{date_start}' and '{date_end}'
    """
    df_db = pd.read_sql(q_db, db)
    print(df_db.shape)
    print(df_db.head())

    # Faster anti-join
    keys = ['bu', 'stcode', 'cntdate', 'skutype', 'rpname']
    mask = ~df_db3.set_index(keys).index.isin(df_db.set_index(keys).index)
    df = df_db3[mask].reset_index(drop=True)

    # ===== Insert with tqdm =====
    total = len(df)

    with tqdm(total=total, desc='🚀 Insert VAR', unit='rows') as pbar:
        for start in range(0, total, chunksize):
            end = start + chunksize
            df.iloc[start:end].to_sql(
                f'{bu}_{table}',
                db,
                if_exists='append',
                index=False,
                method='multi'   # เร็วขึ้นมาก
            )
            pbar.update(end - start)

    print(f"✅ Insert completed",bu,"Start Date :",date_start)

    
var_db3_to_db('chg', '20250501', '20250531')
var_db3_to_db('chg', '20250601', '20250631')
var_db3_to_db('chg', '20250701', '20250731')
var_db3_to_db('chg', '20250801', '20250831')
var_db3_to_db('chg', '20250901', '20250931')
var_db3_to_db('chg', '20251001', '20251031')
var_db3_to_db('chg', '20251101', '20251131')
var_db3_to_db('chg', '20251201', '20251231')