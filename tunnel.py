from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import os
from dotenv import load_dotenv, find_dotenv
import subprocess
import time

# ==================== โหลดค่าจาก .env ====================
load_dotenv(find_dotenv())

DB_CONN_STR = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
engine2 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}")
engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")


def is_db_reachable(conn_str, timeout=3):
    """ยิง query ทดสอบจริง แทนการเช็คแค่พอร์ต"""
    try:
        test_engine = create_engine(conn_str, connect_args={"connect_timeout": timeout})
        with test_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        test_engine.dispose()
        return True
    except OperationalError:
        return False
    except SQLAlchemyError:
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
ensure_tunnel(DB_CONN_STR)