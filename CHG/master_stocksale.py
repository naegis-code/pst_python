import os
import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib
import shutil
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import chardet
from tqdm import tqdm

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/DTAP - เอกสาร').exists():
    filepath = user_path / 'Central Group/DTAP - เอกสาร'
else:
    filepath = user_path / 'Central Group/DTAP - Documents'


bu = 'CHG'
table = 'chg_stocksale_update'
f_path = filepath / 'CHG' / 'Master' 
date_stamp = '20250805' 
date_stamp = pd.Timestamp.now().strftime('%Y%m%d')
filepath = f_path / f'StockSales_Summary_{date_stamp}.csv'


timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')


df = pd.read_csv(filepath, dtype=str,encoding='cp874',encoding_errors='replace')

# Clean column names: strip whitespace and lower-case
df.columns = df.columns.str.strip().str.lower().str.replace("'", '')

# Clean all string data: remove single quotes from all string cells
df = df.applymap(lambda x: x.replace("'", '') if isinstance(x, str) else x)

keep_columns = ['as_of_date','sku_number','prname','brand','model','color','size',
                'sdept','sdept_name','spcode','sp_name','prtype']

df = df[keep_columns]

engine = create_engine(db_connect.db_url_pstdb3)

try:
    chunksize = 10000
    total = len(df)

    with engine.begin() as connection:
        connection.execute(text(f'DELETE FROM "{table}"'))
        print(f"✅ Deleted existing data from '{table}'")

        for start in tqdm(range(0, total, chunksize), desc="Inserting chunks", unit="chunk"):
            chunk = df.iloc[start:start+chunksize]
            chunk.to_sql(
                table,
                con=connection,
                if_exists='append',
                index=False,
                method='multi'
            )

    # commit จะเกิดตรงนี้อัตโนมัติ
    print(f"✅ Inserted new data into '{table}'")
    os.remove(filepath)
    print(f"✅ Removed file '{filepath}'")

except SQLAlchemyError as e:
    print(f"❌ SQL Error - {e}")
except Exception as e:
    print(f"❌ General Error - {e}")
