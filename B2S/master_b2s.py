import pandas as pd
from sqlalchemy import create_engine, text
import db_connect as db
import shutil
import sys
sys.stdout.reconfigure(encoding='utf-8')

bu = 'B2S'
stcode = '50112'
atype = '3F'
cntdate = '20250104'

path_master = 'D:\\Master.db'

engine_db = create_engine(db.db_url_pstdb)
engine_db3 = create_engine(db.db_url_pstdb3)
engine_master = create_engine(f"sqlite:///{path_master}")

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
    countName = bu + stcode + atype[-1] + cntdate + '001'
    storeCode = stcode
    branch = plan['branch'].iloc[0]
    storeName = branch

    q_users_pass = text("""
    SELECT employee_code as username, encryptedpassword, employee_code as empCode
    FROM employees
    WHERE last_work_date is null or last_work_date > CURRENT_DATE
                  """)
    df_users = pd.read_sql_query(q_users_pass, engine_db)

    query_get_master = text("""
    SELECT 
        upc as ibc,
        des as "productName",
        0 as stock,
        price as "retailPrice",
        'A' as status
    FROM b2s_master_bar
    union all
    select distinct 
        lpad(sku,13,'0') as ibc,
        des as "productName",
        0 as stock,
        price as "retailPrice",
        'A' as status
    from b2s_master_bar
    """)

    df_get_master = pd.read_sql_query(query_get_master, engine_db3)


    # 👉 pad barcode ใน pandas (ปลอดภัยทุก DB)
    df_get_master['sku'] = df_get_master['ibc'].astype(str).str.zfill(13)
    df_get_master['barcodeIBC'] = df_get_master['sku']

    df_get_master['stocktakeid'] = countName
    df_get_master['storeCode'] = storeCode
    df_get_master['storeName'] = storeName

    df_get_master.drop(columns=['ibc'], inplace=True)

    with engine_master.begin() as connection:
        connection.execute(text("""
            UPDATE stocktakes
            SET countName = :countName,
                storeCode = :storeCode,
                bu = :bu,
                branch = :branch,
                storeName = :storeName
            WHERE id = 1
        """), {
            "countName": countName,
            "storeCode": storeCode,
            "bu": bu,
            "branch": branch,
            "storeName": storeName
        })

        connection.execute(text("DELETE FROM pda_masters"))
        connection.execute(text("DELETE FROM users"))
        connection.execute(text("DELETE FROM sqlite_sequence WHERE name='pda_masters'"))
        connection.execute(text("DELETE FROM sqlite_sequence WHERE name='users'"))

        df_get_master.to_sql('pda_masters', con=connection, if_exists='append', index=False)
        df_users.to_sql('users', con=connection, if_exists='append', index=False)

        print(f"✅ Inserted {len(df_get_master)} records")

    # 👉 VACUUM (ต้องอยู่นอก transaction)
    with engine_master.connect() as conn:
        conn.execute(text("VACUUM"))

    shutil.copy(path_master, f"D:\\{countName}.db")

    print(f"✅ Master database created: D:\\{countName}.db")
    
else:
    print("No plan found for the given parameters.")
