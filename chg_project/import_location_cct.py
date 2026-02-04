import pandas as pd
import pathlib
from sqlalchemy import create_engine, text
import sqlite3

# Set user directory
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/CHG data special list - เอกสาร'
    if (userpath / 'Central Group/CHG data special list - เอกสาร').exists()
    else userpath / 'Central Group/CHG data special list - Documents'
)

src_path = filepath / '2025' / 'Location POG' / 'Location by Store'
format_store_path = filepath / '2025' / 'Location POG' / 'เดิม' / '1.STORE FORMAT' / 'FORMAT STORE.xlsx'
sku_master_path = filepath / '2025' / 'Location POG' / 'SKU Master' / 'NEW POG LOCATION CCT.xlsx'
#db_path = filepath / '2025' / 'Location POG' / 'chg.db'
db_path = pathlib.Path('D:/chg.db')

files = sorted(src_path.glob('*.csv'))
total = len(files)

if total == 0:
    print("❌ No CSV files found in", src_path)
else:
    print(f"🔍 Found {total} CSV files.")

# Database connection
engine = create_engine(f'sqlite:///{db_path}')
with engine.begin() as conn:
    conn.execute(text("DELETE FROM location_cct"))
    conn.execute(text("DELETE FROM format_store"))
    conn.execute(text("DELETE FROM location_list"))
    print('🧹 Cleared existing data.')

    for i, file in enumerate(files, start=1):
        df = pd.read_csv(file, dtype=str, encoding='cp874')
        df.to_sql('location_cct', con=conn, if_exists='append', index=False)
        print(f'✅ Processed file {i}/{total}: {file.name} ({len(df)} records)')

    df_format = pd.read_excel(format_store_path,sheet_name='FORMAT STORE',dtype=str,usecols='A:F')

    df_location_list = pd.read(format_store_path,sheet_name='locationlist',dtype=str,usecols='A:A')

'SM'
'TC'
'HB'
'TW'

skip = 6 
header=none

    

# Vacuum DB หลังนำเข้าทั้งหมด
with sqlite3.connect(str(db_path)) as sqlite_conn:
    sqlite_conn.execute("VACUUM")
    print("🧩 Database vacuum completed.")
