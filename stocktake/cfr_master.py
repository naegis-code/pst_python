import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
from pathlib import Path

# ========== PARAMETERS ==========
folder = 'PDASTOCK_123_0000512601'
file = folder+'.txt'
path = Path('D:/Users/prthanap/Downloads') / folder / file


# ========== DATABASE CONNECTION ==========
connect_db = create_engine(db_connect.db_url_pstdb)
query_delete = text(f"""DELETE FROM cfr_master""")

# ========== DELETE DATA FROM TABLE ==========

with connect_db.connect() as connection:
    connection.execute(query_delete)
    connection.commit()
    print("Deleted existing data from cfr_master table.")

# ========== READ DATA FROM FILE ==========
df = pd.read_csv(path, sep='|',dtype=str,encoding='cp874',header=None)
df.columns = [
    'cyclecount',
    'stcode',
    'barcode',
    'product_name',
    'dept_code',
    'dept_name',
    'subdept_code',
    'subdept_name',
    'soh',
    'pack',
    'cost',
    'retail',
    'status'
]
df.drop(columns=['cyclecount','stcode','soh'], inplace=True)

df.to_sql('cfr_master', con=connect_db, if_exists='append', index=False)

print("Inserted data into cfr_master table : ", len(df), "rows.")






