from decimal import Decimal
import pandas as pd
import pathlib
import db_connect
from datetime import datetime
from sqlalchemy import create_engine,text

path = pathlib.Path.home() / 'Downloads'
file = 'Summary Hold  7 Day VS Full 2025.xlsx'
sheet_name = 'Full Count (2)'
usecols = 'A:AE'
engine = create_engine(db_connect.db_url_pstdb3)

query = text(f"""
    select stcode,"location" ,skcode ,cntqnt 
    from chg_var_this_year cvty 
    where rpname = 'VAR2'
	    and "location" not like ''
        and cntqnt > 0
""")

df_var = pd.read_sql(query, engine)

df = pd.read_excel(path / file, sheet_name=sheet_name, usecols=usecols, skiprows=0, dtype=str)

df.columns = df.columns.str.lower()

columns_to_numeric = ['ราคาขายปลีก', 'จำนวน hold', 'total']
for col in columns_to_numeric:
    df[col] = (
        df[col]
        .str.replace(',', '')
        .apply(lambda x: Decimal(x) if x not in ['nan', 'None', ''] else None)
    )

df = df.merge(df_var, how='left', left_on=['สาขา', 'รหัส sku'], right_on=['stcode', 'skcode']).drop(columns=['stcode', 'skcode'])


df.to_excel(path / 'Summary Hold  7 Day VS Full 2025_Processed2.xlsx', index=False)
print(df.head())


