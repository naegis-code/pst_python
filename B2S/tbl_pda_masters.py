import pandas as pd
from sqlalchemy import create_engine, text
import os
import db_connect
import time
import shutil
import pathlib
import sys

user_path = pathlib.Path.home()
export_path = user_path / 'Downloads'

path_sqllite = r'D:\Program Files\B2S\Apps\pda-master.db'

# Create a connection to the SQLite database
engine_sqlite = create_engine(f'sqlite:///{path_sqllite}')

# Corrected table name to 'pda_masters' to match the to_sql command later
# Delete all rows from pda_masters
with engine_sqlite.connect() as conn:
    conn.execute(text('DELETE FROM pda_masters;'))
    conn.commit()

# Reset AUTOINCREMENT only if it exists
with engine_sqlite.connect() as conn:
    conn.execute(text("DELETE FROM sqlite_sequence WHERE name='pda_masters';"))
    conn.commit()

connect_db3 = create_engine(db_connect.db_url_pstdb3)
query_db3 = f"""select 
    vendor as "vendorCode",
    v_name as "vendorName",
    lpad(sku_no, 13, '0') as sku,
    lpad(sku_no, 13, '0') as "barcodeIBC",
    case when ibc = '0' then null else lpad(ibc, 13, '0') end as barcode1,
    case when "sbc#1" = '0' then null else lpad("sbc#1", 13, '0') end as barcode2,
    case when "sbc#2" = '0' then null else lpad("sbc#2", 13, '0') end as barcode3,
    case when "sbc#3" = '0' then null else lpad("sbc#3", 13, '0') end as barcode4,
    case when "sbc#4" = '0' then null else lpad("sbc#4", 13, '0') end as barcode5,
    case when "sbc#5" = '0' then null else lpad("sbc#5", 13, '0') end as barcode6,
    regexp_replace(sku_descr, E'[\\n\\r,]', '', 'g') as "productName",
    color_des as "color",
    inner_pack as "unitOfMeasure",
    size_des as "size",
    reg_retail as "cost",
    0 as stock,
    'A' as status
from b2s_master 
where sku_type <> '03';
"""

query_sqllite_stkid_stocktakeid = f"""SELECT countName as stocktakeId,storeCode,storeName FROM stocktakes;"""

df_db3 = pd.read_sql(query_db3, connect_db3)
df_stocktake = pd.read_sql(query_sqllite_stkid_stocktakeid, engine_sqlite)

df_db3['stocktakeId'] = df_stocktake.iloc[0]['stocktakeId']
df_db3['storeCode'] = df_stocktake.iloc[0]['storeCode']
df_db3['storeName'] = df_stocktake.iloc[0]['storeName']

# Corrected table name to 'pda_masters' to match the deletion command
df_db3.to_sql('pda_masters', engine_sqlite, if_exists='append', index=False)

with engine_sqlite.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    conn.execute(text("VACUUM"))

# Export with correct stocktake ID
timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
stocktake_id = df_stocktake.iloc[0]['stocktakeId']
export_filename = f"pda_masters_{stocktake_id}_{timestamp}.db"
export_path_filename = os.path.join(export_path, export_filename)
shutil.copy(path_sqllite, export_path_filename)

print("Data transfer to 'pda_masters' table completed successfully.")
print(f"Total records inserted: {len(df_db3)}")