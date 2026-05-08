import pandas as pd
from sqlalchemy import create_engine, text
import db_connect as db
import shutil
import sys
import pathlib
sys.stdout.reconfigure(encoding='utf-8')

bu = 'CFR'
stcode = '017'
atype = '3F'
cntdate = '20260508'

filename = 'PDASTOCK_17_0000515963'
filename = filename + '.txt'

userpath = pathlib.Path.home()
path = userpath / 'Downloads' / filename



engine_db = create_engine(db.db_url_pstdb)
engine_db3 = create_engine(db.db_url_pstdb3)


query_plan = text("""
    SELECT branch
    FROM planall2 
    WHERE bu = :bu 
      AND stcode = :stcode 
      AND atype = :atype 
      AND cntdate = :cntdate
""")

params = {
    "bu": bu,
    "stcode": stcode,
    "atype": atype,
    "cntdate": cntdate
}

plan = pd.read_sql_query(query_plan, engine_db, params=params)



if not plan.empty:
    stocktakeid = bu + stcode + atype[-1] + cntdate + '001'
    storeCode = stcode
    branch = plan['branch'].iloc[0]
    storeName = branch
    print(stocktakeid)
    print(storeCode)
    print(storeName)

    df = pd.read_csv(path, sep='|', dtype=str, encoding='cp874', header=None)
    df.columns = ['cyclecount', 'stcode', 'barcode', 'description', 'dept','deptname','subdept','subdeptname','soh','pack','cost','price','status']
    df['stocktakeid'] = stocktakeid
    df['stcode'] = df['stcode'].str.zfill(3)
    print(df)

    with engine_db3.begin() as connection:
        connection.execute(text("DELETE FROM cfr_master WHERE stocktakeid = :stocktakeid"), {"stocktakeid": stocktakeid})
        print(f"Existing records with stocktakeid {stocktakeid} deleted from cfr_master.")
        
        df.to_sql('cfr_master', con=connection, if_exists='append', index=False,method='multi')

        print("Data inserted into cfr_master successfully." + str(len(df.index)))
    
else:
    print("No plan found for the given parameters.")
