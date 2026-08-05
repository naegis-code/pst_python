import polars as pl
from datetime import datetime
import os
import pathlib
from dotenv import load_dotenv, find_dotenv
import subprocess
import time

pl.Config.set_tbl_cols(-1)

# ==================== โหลดค่าจาก .env ====================
load_dotenv(find_dotenv())

engine1 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
engine2 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}"
engine3 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}"


def is_db_reachable(conn_str, timeout=3):
    try:
        pl.read_database_uri("SELECT 1", conn_str)
        return True
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return False


def ensure_tunnel(conn_str, ssh_alias="pst-db", max_wait=15, check_interval=1):
    if is_db_reachable(conn_str):
        print("✅ Database ต่อได้อยู่แล้ว — tunnel ทำงานปกติ")
        return

    print("❌ ต่อ Database ไม่ได้ — กำลังเปิด SSH tunnel...")
    subprocess.Popen(
        "start /b ssh -f -N " + ssh_alias,
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )

    print("🔑 รอ tunnel พร้อมใช้งาน...")
    waited = 0
    while waited < max_wait:
        time.sleep(check_interval)
        waited += check_interval
        if is_db_reachable(conn_str):
            print(f"✅ SSH tunnel เปิดสำเร็จ (ใช้เวลา {waited} วินาที)")
            return

    raise RuntimeError(f"❌ เปิด tunnel ไม่สำเร็จภายใน {max_wait} วินาที — ต่อ Database ไม่ได้")


# --- main ---
ensure_tunnel(engine1)

user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path = filepath / 'Apps' / 'Report_Statistics4.xlsx'
table = 'report_statistics'

df = pl.read_excel(path, table_name='Table1')

df = df.rename({
    "ID": "id",
    "Email": "email",
    "StartTime": "starttime",
    "BU": "bu",
    "StoreCode": "storecode",
    "CNTDATE": "cntdate",
    "Checklist": "checklist",
    "Report1": "report1",
    "Report3": "report3",
    "FVF_Missrate": "fvf_missrate",
    "Issues": "issues",
    "Document Cut-Off Excel &amp; PDF": "document cut-off excel & pdf",
    "Other": "other"
})

query_old = f"SELECT id FROM {table}"

df_old = pl.read_database_uri(query_old, engine1)

df = df.join(df_old, left_on="id", right_on="id", how="anti")
print(df)
df.write_database(table, engine1, if_table_exists="append")
print(f"✅ อัปโหลดข้อมูลไปยังตาราง {table} จำนวน {df.height} แถวเรียบร้อยแล้ว")


