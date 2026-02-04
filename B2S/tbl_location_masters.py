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
    conn.execute(text('DELETE FROM location_masters;'))
    conn.commit()

# Reset AUTOINCREMENT only if it exists
with engine_sqlite.connect() as conn:
    conn.execute(text("DELETE FROM sqlite_sequence WHERE name='location_masters';"))
    conn.commit()

query_sqllite_stkid_stocktakeid = f"""SELECT countName as stocktakeId,storeCode,storeName FROM stocktakes;"""
df_stocktake = pd.read_sql(query_sqllite_stkid_stocktakeid, engine_sqlite)

countName = df_stocktake.iloc[0]['stocktakeId']

connect_db3 = create_engine(db_connect.db_url_pstdb3)
query_db3 = f"""select 
    location,
    cntnum as "stocktakeId"
from topscare_location 
where cntnum = '{countName}';
"""

df_db3 = pd.read_sql(query_db3, connect_db3)

# Corrected table name to 'pda_masters' to match the deletion command
df_db3.to_sql('location_masters', engine_sqlite, if_exists='append', index=False)

print("Data transfer to 'location_masters' table completed successfully.")
print(f"Total records inserted: {len(df_db3)}")