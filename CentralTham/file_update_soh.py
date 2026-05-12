import pandas as pd
from sqlalchemy import create_engine, text
import db_connect as db
import sys
import pathlib
sys.stdout.reconfigure(encoding='utf-8')

stocktakeid = 'CentralTham10153F20260507001'

userpath = pathlib.Path.home()
path = userpath / 'Downloads'

file = "SOH CDS Patong as of 06-05-26.xlsx"

path = userpath / 'Downloads' / file
path_file = path

df = pd.read_excel(path_file,sheet_name='Sheet1',usecols='A:B',dtype=str)

df.columns = df.columns.str.strip().str.lower()  # ลบช่องว่างที่อาจมีในชื่อคอลัมน์และแปลงเป็นตัวพิมพ์เล็ก

colum_mapping = {
    'qty': 'soh'
}

df = df.rename(columns=colum_mapping)
df['stocktakeid'] = stocktakeid

df.drop_duplicates(inplace=True)

engine_db3 = create_engine(db.db_url_pstdb3)
with engine_db3.begin() as connection:
    connection.execute(text("DELETE FROM central_tham_soh_update WHERE stocktakeid = :stocktakeid"), {"stocktakeid": stocktakeid})
    print(f"Existing records with stocktakeid {stocktakeid} deleted from central_tham_soh_update.")
    
    df.to_sql('central_tham_soh_update', con=connection, if_exists='append', index=False)

    print("Data inserted into central_tham_soh_update successfully." + str(len(df.index)))
