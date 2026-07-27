import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib
import os
from dotenv import load_dotenv, find_dotenv
import socket
import subprocess
import time

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

engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
engine2 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}")

userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

save_path_checklist = filepath / 'Apps' / 'checklist_all.csv'
save_path_planall2 = filepath / 'Apps' / 'planall2_checklist.csv'

path = r"D:\Users\prthanap\OneDrive - Central Group\Apps\checklist_all.csv"

filter_count_date = '20240101'

q_checklist = text("""
                   select c.stcode 
                        ,c.cntdate as check_date
                        ,c.question_code as "Attribute"
                        ,c.point as "Value"
                        ,c.bu as "BU"
                        ,p.type1 as "Type1"
                        ,c.zone as "ZONE"
                        ,c.weight as "Weight"
                        ,c."section" as "Subject"
                        ,c.subject as "Description"
                        ,c.subdescription as "SubDesctiption"
                        ,c."full" as "Full"
                        ,c.act as "Act"
                    from checklist c 
                    left join planall2 p
                        on c.bu = p.bu and c.stcode = p.stcode and c.cntdate = p.cntdate 
                    where p.atype in ('3F','3Q')
                        and c.checkdate >= :filter_count_date
                        and p.branch is not null
                   """)

df_checklist = pd.read_sql(q_checklist, engine1, params={'filter_count_date': filter_count_date})

q_planall2 = text("""
                    select p.*
                        ,row_number() over (partition by bu, stcode order by cntdate desc) as running
                    from planall2 p 
                    where atype in('3F')
                        and cntdate >= :filter_count_date
                     """)
df_planall2 = pd.read_sql(q_planall2, engine1, params={'filter_count_date': filter_count_date})


df_checklist.to_csv(save_path_checklist, index=False)
print(f"✅ Checklist data saved to {save_path_checklist} successfully. Total rows: {len(df_checklist)}")

df_planall2.to_csv(save_path_planall2, index=False)
print(f"✅ Planall2 data saved to {save_path_planall2} successfully. Total rows: {len(df_planall2)}")