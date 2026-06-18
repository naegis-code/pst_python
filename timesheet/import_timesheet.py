import polars as pl
from sqlalchemy import create_engine, text
import pathlib 
import db_connect as dbc

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path = filepath / 'Apps' / 'time_sheet_excel.xlsx'

engine = create_engine(dbc.db_url_pstdb)

query_old = """
            select max(id)
            from stime_sheet
            """

#read sql
df_old = pl.read_database(query_old, engine)
print(df_old)
# Read the Excel file into a DataFrame

df = pl.read_excel(path, sheet_name='Sheet1')
df = df.filter(pl.col('id') > df_old[0,0])

df = df.rename({
    'employeecode': 'empcode',
    'checkin_date': 'check_in_date',
    'checkin_hh': 'check_in_hour',
    'checkin_mm': 'check_in_minute',
    'checkout_date': 'check_out_date',
    'checkout_hh': 'check_out_hour',
    'checkout_mm': 'check_out_minute',
    'transportation_expenses': 'transport_expense',
    'timestamp': 'stamptime',
    'pda': 'pda_no'
})

print(df)
print(df.columns)

# ========== Insert ==========
try:
    if df.is_empty():
        print("ไม่มีข้อมูลใหม่")
    else:
        df.write_database(
            table_name='stime_sheet',        # ✅ ลำดับ argument ถูกต้อง
            connection=engine,               # ✅ ใช้ connection= 
            if_table_exists='append'         # ✅ if_table_exists ไม่ใช่ if_exists
        )
        print(f"Data inserted successfully. {df.shape[0]:,} records inserted.")
except Exception as e:
    print(f"Error inserting data: {e}")

