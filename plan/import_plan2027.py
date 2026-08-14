import polars as pl
import pathlib
import os
from dotenv import load_dotenv, find_dotenv
import socket
import subprocess
import time

#pl.Config.set_tbl_cols(-1)

# ==================== โหลดค่าจาก .env ====================
load_dotenv(find_dotenv())

def is_port_open(host, port, timeout=2):
    """เช็คว่าพอร์ต local เปิด (มีอะไร listening อยู่) หรือไม่"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        result = s.connect_ex((host, port))
        return result == 0

DB_HOST = "localhost"
DB_PORT = 5432

if is_port_open(DB_HOST, DB_PORT):
    print(f"✅ พอร์ต {DB_PORT} ที่ {DB_HOST} เปิดอยู่ — tunnel ทำงานอยู่")
else:
    print(f"❌ พอร์ต {DB_PORT} ที่ {DB_HOST} ปิดอยู่ — tunnel ยังไม่เปิด")
    subprocess.Popen(
        "start /b ssh -f -N pst-db",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    print("🔑 กำลังเปิด SSH tunnel... (รอ 5 วินาที)")
    time.sleep(5)
    print("✅ SSH tunnel เปิดแล้ว") if is_port_open(DB_HOST, DB_PORT) else print("❌ SSH tunnel ยังไม่เปิด — ตรวจสอบการเชื่อมต่อ SSH")

engine1 = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
engine2 = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}"

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

filename = 'Annual Plan 2027 All Update (By Div).xlsx'

table_name = 'AnnualPlan'
table_db = 'plan2027'

path = filepath / 'Report/2027/99 Plan' / filename

df = pl.read_excel(path, table_name=table_name).select(pl.all().cast(pl.String)).rename({
    'No': 'no','BUs.': 'bu','Acronym': 'acronym','Store Code': 'stcode','Branch': 'branch',
    'Province': 'province','HUB': 'shub','FOOD': 'food_soh','NONFOOD': 'nonfood_soh','PERISHABLE': 'perishable_soh',
    'Total SOH': 'total_soh','Size': 'size','Type': 'type1','Atype': 'atype','Total EST.Man': 'est_man_total',
    'EST.ManControl': 'est_man_control','EST.ManExpire': 'est_man_expire','EST.ManCount': 'est_man_count','CNTDATE': 'cntdate','Day': 'day',
    'Month': 'month','Total PlanManday': 'div_pman_total','Manday Control': 'div_pman_control','Manday Expire2': 'div_pman_expire','Manday Count': 'div_pman_count',
    'ManStore': 'div_pman_store','Outsource': 'div_cman_outsource','Part-Time local': 'div_pman_pt','Outsource By DIV': 'div_pman_outsource','Status การจ้าง Outsource': 'hiring_outsource',
    'ประเภทการตรวจนับ': 'outsource_cnt_type','Round': 'round','Status2': 'job_status','POST Date': 'post_date','Case LP No': 'case_lp_no',
    'Case LP Date': 'case_lp_date','Code For Copy':'code_for_copy'
}).with_columns([
    pl.col('cntdate').cast(pl.Date, strict=False),
    pl.col('food_soh').cast(pl.Float64, strict=False).round(2),
    pl.col('nonfood_soh').cast(pl.Float64, strict=False).round(2),
    pl.col('perishable_soh').cast(pl.Float64, strict=False).round(2),
    pl.col('total_soh').cast(pl.Float64, strict=False).round(2),
    pl.col('est_man_control').cast(pl.Int64, strict=False),
    pl.col('est_man_count').cast(pl.Int64, strict=False),
    pl.col('est_man_expire').cast(pl.Int64, strict=False),
    pl.col('div_pman_control').cast(pl.Int64, strict=False),
    pl.col('div_pman_count').cast(pl.Int64, strict=False),
    pl.col('div_pman_expire').cast(pl.Int64, strict=False),
    pl.col('div_pman_store').cast(pl.Int64, strict=False),
    pl.col('div_pman_outsource').cast(pl.Int64, strict=False),
    pl.col('div_pman_pt').cast(pl.Int64, strict=False),
    pl.col('div_pman_total').cast(pl.Int64, strict=False),
    pl.col('div_cman_outsource').cast(pl.Int64, strict=False),
    pl.col('post_date').cast(pl.Date, strict=False),
    pl.col('case_lp_date').cast(pl.Date, strict=False),
    pl.col('no').cast(pl.Int64, strict=False)
]).drop_nulls('bu').select(['no', 'bu', 'acronym', 'stcode', 'branch', 'province', 'shub', 'food_soh',
    'nonfood_soh', 'perishable_soh', 'total_soh', 'size', 'type1', 'atype',
    'est_man_total', 'est_man_control', 'est_man_expire', 'est_man_count',
    'cntdate', 'day', 'month', 'div_pman_total', 'div_pman_control', 
    'div_pman_expire', 'div_pman_count', 'div_pman_store', 'div_cman_outsource',
    'div_pman_pt', 'div_pman_outsource', 'hiring_outsource', 'outsource_cnt_type',
    'round', 'job_status', 'post_date', 'case_lp_no', 'case_lp_date', 'code_for_copy'])


df.write_database(table_db, engine1, if_table_exists='replace')

print(f"✅ อัปโหลดข้อมูล {table_name} ไปยังฐานข้อมูล {table_db} เรียบร้อย จำนวน {df.shape[0]} แถว")


