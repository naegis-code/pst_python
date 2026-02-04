import pandas as pd
import polars as pl
from sqlalchemy import create_engine,text
import db_connect

engine3 = create_engine(db_connect.db_url_pstdb3)
engine5 = create_engine(db_connect.db_url_pstdb5)

query3 = text("""
SELECT * FROM b2s_master_bar
""")

df = pl.read_database(query3, engine3)

df = df.to_pandas()

df.to_sql('b2s_master_bar', engine5, if_exists='append', index=False)

print(df.head())