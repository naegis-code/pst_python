import polars as pl
import pathlib
import os
from dotenv import load_dotenv, find_dotenv
import socket
import subprocess
import time
from datetime import datetime

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

engine1 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
engine2 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}"
engine3 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}"

start_time = datetime.now()
print(f"starttime: {start_time}")
# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'PWB'
sdate = '20260101'
edate = '20261231'

path_report = filepath / 'Apps' / 'Stocktake' / 'pwb_stk_report.csv'
path_report_dept = filepath / 'Apps' / 'Stocktake' / 'pwb_stk_report_dept.csv'

q_plan = f"""SELECT bu,
                    stcode,
                    acronym,
                    branch,
                    shub,
                    type1,
                    cntdate,
                    round,
                    post_date,
                    hiring_outsource,
                    outsource_cnt_type
              FROM planall2
              WHERE bu = '{bu.upper()}'
                AND atype = '3F'
                AND cntdate between '{sdate}' and '{edate}'
              """

df_plan = pl.read_database_uri(q_plan, engine1)
print(f"✅ Plan data retrieved successfully. Total rows: {len(df_plan)}")

q_report = f"""
                select stmerch as stcode ,cntdate ,rpname ,skutype ,
                    count(*) as sku_count,
                    sum(
                        case when varianceqnt = 0 then 1 else 0 end) as sku_eq,
                    sum(
                        case when varianceqnt > 0 then 1 else 0 end) as sku_gain,
                    sum(
                        case when varianceqnt < 0 then 1 else 0 end) as sku_loss,
                    sum(soh) as qnt_soh,
                    sum(cntqnt) as qnt_physical,
                    sum(
                        case when varianceqnt > 0 then varianceqnt else 0 end) as qnt_gain,
                    sum(
                        case when varianceqnt < 0 then varianceqnt else 0 end) as qnt_loss,
                    sum(varianceqnt) as qnt_variance,
                    sum(extphycnt_retail -extphy_retailvar) as retail_soh,
                    sum(extphycnt_cost -extphy_costvar) as cost_soh,
                    sum(extphycnt_retail) as retail_physical,
                    sum(extphycnt_cost) as cost_physical,
                    sum(
                        case when extphy_retailvar > 0 then extphy_retailvar else 0 end) as retail_gain,
                    sum(
                        case when extphy_costvar > 0 then extphy_costvar else 0 end) as cost_gain,
                    sum(
                        case when extphy_retailvar < 0 then extphy_retailvar else 0 end) as retail_loss,
                    sum(
                        case when extphy_costvar < 0 then extphy_costvar else 0 end) as cost_loss,
                    sum(extphy_retailvar) as retail_net,
                    sum(extphy_costvar) as cost_net
                from pwb_stk_this_year psty 
                where cntdate between '{sdate}' and '{edate}'
                group by stmerch ,cntdate ,rpname ,skutype
                """

df_report = pl.read_database_uri(q_report, engine3)
print(f"✅ Report data retrieved successfully. Total rows: {len(df_report)}")

df_report.write_csv(path_report)

print(f"✅ Report data saved to {path_report} successfully. Total rows: {len(df_report)}")

end_time = datetime.now()
print(f"endtime: {end_time}")
print(f"Usetime: {end_time - start_time}")
