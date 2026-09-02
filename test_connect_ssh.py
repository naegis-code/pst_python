import pandas as pd
from sqlalchemy import create_engine,text
from dotenv import load_dotenv, find_dotenv
import subprocess
import time
import os
# ==================== โหลดค่าจาก .env ====================
load_dotenv(find_dotenv())

engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")

query = text("SELECT 1")

try:
    df = pd.read_sql(query, engine1)
    print("✅ SSH tunnel เปิดอยู่แล้ว")
except Exception as e:
    print(f"Error: {e}")
    print("❌ SSH tunnel ยังไม่เปิด — กำลังพยายามเชื่อมต่อ SSH")
    
    subprocess.Popen(
        "start /b ssh -f -N pst-db",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    print("🔑 กำลังเปิด SSH tunnel... (รอ 5 วินาที)")
    time.sleep(5)
    try:
        df = pd.read_sql(query, engine1)
        print("✅ SSH tunnel เปิดแล้ว")
    except Exception as e:
        print(f"Error: {e}")
        print("❌ SSH tunnel ยังไม่เปิด — ตรวจสอบการเชื่อมต่อ SSH")