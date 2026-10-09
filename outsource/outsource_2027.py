import polars as pl
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv
import pathlib
import os
import requests
import traceback
import subprocess
import time

load_dotenv(find_dotenv())

script_name = 'outsource_2027.py'

def send_telegram(message):
    token = "8694562639:AAG8TbsWxIDg9VVoLVn_qdloCgQoDNFwxMw"
    chat_id = "-5181414443"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    
    try:
        res = requests.post(url, json=payload, timeout=10)
        res_json = res.json()
        if not res_json.get("ok"):
            print(f"⚠️ Telegram API Error: {res_json}")
        else:
            print("📱 Telegram Notification sent successfully!")
    except Exception as e:
        print(f"❌ Failed to send Telegram notification: {e}")

def ssl(engine1):
    query = text("SELECT 1")
    df = pl.read_database(query, engine1)
    if len(df) == 1:
        print("✅ Database connection test passed.")
    else:
        print("❌ Database connection test failed.")
        subprocess.Popen(
            "start /b ssh -f -N pst-db",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        print("🔑 กำลังเปิด SSH tunnel... (รอ 5 วินาที)")
        time.sleep(5)

def pathname(name):
    
    if pathlib.Path.home().name == name:
        engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
        engine2 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}")
        engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")
    else:
        engine1 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@103.22.182.82:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
        engine2 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@103.22.182.82:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}")
        engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@103.22.182.82:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")
    return engine1, engine2, engine3

try:
    engine1, engine2, engine3 = pathname('prthanap')
    ssl(engine1)

    query_outsource = text("""with d as (
                            select bu,stcode,branch,province ,shub ,type1 as st_type,cntdate ,outsource_cnt_type as cnt_type ,hiring_outsource as hiring_status
                                ,food_soh
                                ,nonfood_soh
                                ,perishable_soh
                                ,total_soh 
                                ,case 
                                    when outsource_cnt_type = 'All' then total_soh
                                    when outsource_cnt_type = 'Food' then food_soh
                                    when outsource_cnt_type like 'Food+%' then food_soh
                                    when outsource_cnt_type like 'Food & Non-Food%' then food_soh+nonfood_soh 
                                    when outsource_cnt_type like 'Food & Perishable%' then food_soh+perishable_soh 
                                    when outsource_cnt_type = 'Non-Food' then nonfood_soh 
                                    when outsource_cnt_type like 'Non-Food+%' then nonfood_soh 
                                    when outsource_cnt_type like 'Non-Food & Perishable%' then nonfood_soh+perishable_soh 
                                    when outsource_cnt_type like 'Perishable%' then perishable_soh
                                else 0 end 
                                as soh_outsource
                                ,case when div_pman_outsource > 0 then div_pman_outsource else div_cman_outsource end as est_man
                            from plan2027 p 
                            where p.hiring_outsource = 'YES'
                            )
                            select bu,stcode,branch,province ,shub ,st_type,cntdate ,cnt_type,hiring_status,soh_outsource
                                ,round(soh_outsource/10500)+est_man as est_man_outsource
                            from d""")  
    
    df = pl.read_database(query_outsource,engine1)

    with engine1.begin() as conn:
        conn.execute(text("DELETE FROM outsources where cntdate between '20270101' and '20271231'"))
        print(f"✅ Deleted existing records from outsources for 2027")

    df.write_database('outsources', engine1, if_table_exists='append')
    print(f"✅ Inserted {len(df):,} rows into outsources for 2027")

    success_msg = (
            f"✅ [{script_name}]\nRun Completed Successfully!\n"
            f"----------------------------------------\n"
            f"📊 df_outsource inserted: {len(df):,} rows"
        )
    print(success_msg)
    send_telegram(success_msg)

except Exception as e:
    error_msg = (
        f"❌ [{script_name}]\nRun Failed!\n"
        f"----------------------------------------\n"
        f"Error: {str(e)}\n"
        f"Traceback: {traceback.format_exc()}"
    )
    print(error_msg)
    send_telegram(error_msg)
