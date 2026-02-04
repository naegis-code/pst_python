import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from db_connect import db_url_pstdb, db_url_pstdb3
import pathlib
import os

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

t_path = filepath / 'Apps' / 'stk_report.csv'
timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"🕒 Start time: {timestamp}")

cntdate_start = '20250101'
cntdate_end = '20251231'

engine_db = create_engine(db_url_pstdb)
engine_db3 = create_engine(db_url_pstdb3)

query_db = f"SELECT * FROM stk_report_view_only where cntdate between '{cntdate_start}' and '{cntdate_end}'"
query_db3 = f"DELETE FROM stk_report where cntdate between '{cntdate_start}' and '{cntdate_end}'"



# Start time
datetimestart = datetime.now()

# Load data from db
with engine_db.connect() as conn:
    df_db = pd.read_sql(query_db, conn)
    print(f"✅ Data {len(df_db)} loaded from stk_report_view_only at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# Delete existing data in db3
with engine_db3.connect() as conn:
    conn.execute(text(query_db3))
    conn.commit()
    print(f"✅ Existing data deleted from stk_report in db3 at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Insert data into db3 with progress bar
from tqdm import tqdm
chunk_size = 10000
with tqdm(total=len(df_db), desc="Inserting", unit="rows") as pbar:
    for start in range(0, len(df_db), chunk_size):
        chunk = df_db.iloc[start:start+chunk_size]
        chunk.to_sql(
            "stk_report",
            engine_db3,
            if_exists='append',
            index=False
        )
        pbar.update(len(chunk))
print(f"✅ Data {len(df_db)} inserted into stk_report at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# End time
datetimeend = datetime.now()

# Total time spent
print("Total time spent:", (datetimeend - datetimestart).seconds, "seconds")
