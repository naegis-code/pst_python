import pandas as pd
from sqlalchemy import create_engine, text
import db_connect

enging = create_engine(db_connect.db_url_pstdb)
enging3 = create_engine(db_connect.db_url_pstdb3)


df = pd.read_sql_query(text("""
    SELECT * from plan_dba"""), enging)

df3 = pd.read_sql_query(text("""
    SELECT id,'check' as recheck from plan_dba"""), enging3)

df = df.merge(df3, on='id', how='left', indicator=True)
df = df[df['recheck'].isna()].drop(columns=['_merge','recheck'])

print(df.head())

df.to_sql('plan_dba', enging3, if_exists='append', index=False)

print("Completed inserted:", len(df))
