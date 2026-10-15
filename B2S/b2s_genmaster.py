from datetime import datetime
import os
import shutil
import polars as pl
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv
import sys
from pathlib import Path


bu = "B2S"
stcode = "50002"
cntdate = "20261015"
stocktakeid = bu + stcode + 'F' + cntdate
storecode = stcode
storename = "Bangna"
branch = "Bangna"

print(f"Stocktake ID: {stocktakeid}")

master = "D:/Master.db"
engine = create_engine(f"sqlite:///{master}")


# 3. จัดการ Database
try:
    # 3.1 สั่ง Delete และ Update ข้อมูลผ่าน engine.begin() เพื่อจัดการ Transaction
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pda_masters;"))
        conn.execute(text("DELETE FROM sqlite_sequence WHERE name='pda_masters';"))
        conn.execute(text("DELETE FROM location_masters;"))
        conn.execute(text("DELETE FROM sqlite_sequence WHERE name='location_masters';"))

        query_update = text("""
            UPDATE stocktakes 
            SET countname = :stocktakeid, 
                storecode = :storecode, 
                storename = :storename, 
                bu = :bu, 
                branch = :branch
            WHERE id = 1
        """)
        conn.execute(
            query_update,
            {
                "stocktakeid": stocktakeid,
                "storecode": storecode,
                "storename": storename,
                "bu": bu,
                "branch": branch
            },
        )


except Exception as e:
    print(f"Error during database operations: {e}")

# 4. บีบไฟล์ DB ด้วย VACUUM
with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT").execute(text("VACUUM;"))

# 5. Backup ไฟล์ DB และ Export Excel
current_time = datetime.now().strftime("%Y%m%d%H%M")
folder_path = os.path.dirname(master)

new_master_path = os.path.join(
    folder_path, f"Master_{stocktakeid}_{current_time}.db"
)

shutil.copy2(master, new_master_path)

print(f"Backup file created successfully at: {new_master_path}")
print("export completed")

