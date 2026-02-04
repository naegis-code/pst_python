import pandas as pd
from sqlalchemy import create_engine
import db_connect

bu = 'chg'
table = 'stk'
date = '20250101'

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

# Faster anti-join
keys = ['stmerch', 'cntdate', 'skutype', 'rpname']
mask = ~df_chg_db3.set_index(keys).index.isin(df_chg_db.set_index(keys).index)
df = df_chg_db3[mask].reset_index(drop=True)

print(df.shape)
print(df.head(3))
