import pandas as pd
from sqlalchemy import create_engine, text
import os
import db_connect

bu = 'b2S'
stcode = '50008'
cntdate = '031025'

countName = f"{bu.upper()}{stcode}F{cntdate}001"  # Example stocktake ID

print(f"Processing stocktake ID: {countName}")

storeCode = stcode
bu = bu.upper()

engine_db = create_engine(db_connect.db_url_pstdb)
query_db = text("SELECT bu, stcode, stname_th as branch FROM plan_dba WHERE bu = :bu AND stcode = :stcode")
df_db = pd.read_sql_query(query_db, engine_db, params={"bu": bu, "stcode": storeCode})

engine_db3 = create_engine(db_connect.db_url_pstdb3)
# Convert cntdate (assumed 'yymmdd') to 'yyyy-mm-dd'
cntdate_yyyy_mm_dd = f"20{cntdate[4:]}-{cntdate[2:4]}-{cntdate[:2]}"

# Check if stocktakeid already exists
check_query = text(
    "SELECT 1 FROM stocktakeid WHERE cntnum = :cntNum AND bu = :bu AND stcode = :stcode AND cntdate = :cntdate"
)
with engine_db3.connect() as conn:
    result = conn.execute(check_query, {
        "cntNum": countName,
        "bu": bu,
        "stcode": storeCode,
        "cntdate": cntdate_yyyy_mm_dd
    }).fetchone()
    if result:
        print(f"❌ มี stocktakeid {countName} นี้แล้ว")
    else:
        query_db3 = text(
            "INSERT INTO stocktakeid (cntnum,bu,stcode,cntdate,atype,status) "
            "VALUES (:cntNum,:bu,:stcode,:cntdate,'F','Count_1')"
        )
        with engine_db3.begin() as trans_conn:
            trans_conn.execute(query_db3, {
                "cntNum": countName,
                "bu": bu,
                "stcode": storeCode,
                "cntdate": cntdate_yyyy_mm_dd
            })


if df_db.empty:
    print(f"❌ Store code {storeCode} not found in plan_dbase.")
    exit()

branch = df_db.at[0, 'branch']
storeName = branch


path_sqllite = r'D:\Program Files\B2S\Apps\pda-master.db'
# Create a connection to the SQLite database
engine_sqlite = create_engine(f'sqlite:///{path_sqllite}')


query_sqllite_update = text("""
    UPDATE stocktakes SET 
        countName = :countName,
        storeCode = :storeCode,
        storeName = :storeName,
        bu = :bu,
        branch = :branch
    WHERE id = 1;
""")

with engine_sqlite.begin() as conn:  # begin() auto-commits
    conn.execute(query_sqllite_update, {
        "countName": countName,
        "storeCode": storeCode,
        "storeName": storeName,
        "bu": bu,
        "branch": branch
    })


print("Stocktake information updated successfully.")