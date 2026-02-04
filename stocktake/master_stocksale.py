import os
import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib
import shutil
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import chardet

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/DTAP - เอกสาร').exists():
    filepath = user_path / 'Central Group/DTAP - เอกสาร'
else:
    filepath = user_path / 'Central Group/DTAP - Documents'


bu = 'CHG'
table = 'chg_stocksale_update'
f_path = filepath / 'CHG' / 'Master' 
date_stamp = '20250610' #pd.Timestamp.now().strftime('%Y%m%d')
filepath = f_path / f'StockSales_Summary_{date_stamp}.csv'


timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')


df = pd.read_csv(filepath, dtype=str,encoding='cp874',encoding_errors='replace')

# Clean column names: strip whitespace and lower-case
df.columns = df.columns.str.strip().str.lower().str.replace("'", '')

# Clean all string data: remove single quotes from all string cells
df = df.applymap(lambda x: x.replace("'", '') if isinstance(x, str) else x)

df = df.drop(columns=['storesize','import','item status', 'atbcode', 'popgradee',
                       'autominmax','minorder', 'distribute method','class','classname',
                        'sclass', 'sclassname','sp_leadtime','sp_minpoamt', 'asrweek',
                        'asrwkday', 'packsize', 'innerpack','toptype', 'topsdept'])

engine = create_engine(db_connect.db_url_pstdb3)
del_query = text(f"""DELETE FROM {table}""")
                 
try:
    with engine.begin() as connection:
        connection.execute(text(f'DELETE FROM "{table}"'))
        print(f"✅ Deleted existing data from '{table}'")
        df.to_sql(table, con=connection, if_exists='append', index=False, method='multi')
        print(f"✅ Inserted new data into '{table}'")
        os.remove(filepath)  # Remove the file after successful insertion
        print(f"✅ Removed file '{filepath}' after successful insertion")
except SQLAlchemyError as e:
    print(f"❌ Error deleting existing data - {e}")

except Exception as e:
    print(f"❌ Error inserting data - {e}")
