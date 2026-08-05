import polars as pl
from datetime import datetime
from tqdm import tqdm
import os
import pathlib
from dotenv import load_dotenv, find_dotenv
import subprocess
import time

#pl.Config.set_tbl_cols(-1)

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

path = filepath / 'Apps' / 'customer_satisfaction.xlsx'
table = 'customer_satisfaction'

df = pl.read_excel(path, table_name='Table2')

df = df.with_columns([
    pl.col("a1").cast(pl.Int64),
    pl.col("a2").cast(pl.Int64),
    pl.col("b1").cast(pl.Int64),
    pl.col("b2").cast(pl.Int64),
    pl.col("b3").cast(pl.Int64),
    pl.col("b4").cast(pl.Int64),
    pl.col("c1").cast(pl.Int64),
    pl.col("c2").cast(pl.Int64),
    pl.col("c3").cast(pl.Int64),
    pl.col("c4").cast(pl.Int64),
    pl.col("c5").cast(pl.Int64),
    pl.col("d1").cast(pl.Int64),
    pl.col("d2").cast(pl.Int64),
    pl.col("d3").cast(pl.Int64),
    pl.col("e1").cast(pl.Int64),
    pl.col("e2").cast(pl.Int64),
    pl.col("e3").cast(pl.Int64),
    pl.col("e4").cast(pl.Int64),
])

query_old = f"SELECT id FROM {table}"

df_old = pl.read_database_uri(query_old, engine1)

df = df.join(df_old, left_on="id", right_on="id", how="anti")

print(df)

df.write_database(table, engine1, if_table_exists="append")
print(f"✅ อัปโหลดข้อมูลไปยังตาราง {table} จำนวน {df.height} แถวเรียบร้อยแล้ว")

