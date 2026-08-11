import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
import pathlib
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


# Set file path
user_path = pathlib.Path.home()

table = 'cfr_soh2'
table_soh_update = 'soh_update'
bu = 'CFR'

def load_and_process_data(path):
    sheets = pd.read_excel(path, sheet_name=['สาขา RMS V.12', 'สาขา RMS V.16'], skiprows=3, usecols="A:O", dtype=str, header=None)
    
    column_map = {
        1: 'stcode',
        7: 'food_credit',
        9: 'nonfood_consign',
        11: 'perishable_nonmer',
        13: 'totalsoh'
    }

    def process_sheet(df):
        df = df.copy()  # <--- Add this line
        df.columns = [column_map.get(i, i) for i in range(len(df.columns))]
        df['bu'] = 'CFR'
        df = df[['bu', 'stcode', 'food_credit', 'nonfood_consign', 'perishable_nonmer', 'totalsoh']]
        return df

    df1 = process_sheet(sheets['สาขา RMS V.12'])
    df2 = process_sheet(sheets['สาขา RMS V.16'])

    # Extract and format date
    date_info = pd.read_excel(path, sheet_name='สาขา RMS V.12', header=None, dtype=str, usecols="A", nrows=1)
    get_date = pd.to_datetime(date_info.iloc[0, 0].split()[-1], format='%d-%b-%Y').strftime('%Y%m%d')

    df1['date'] = get_date
    df2['date'] = get_date

    # Create 'code' column
    df1['code'] = df1['bu'] + df1['stcode']
    df2['code'] = df2['bu'] + df2['stcode']

    return pd.concat([df1, df2], ignore_index=True)



path = user_path / 'Downloads' / 'cfr_soh.xlsx'
df = load_and_process_data(path)
df = df.rename(columns={'date': 'data_date'})
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Print log message
print(f"Running cfr_soh.py at {timestamp} with {len(df)} rows")


# Create a connection to the database
from sqlalchemy.exc import SQLAlchemyError

# Use environment variables or a configuration file for sensitive information

try:

	df.to_sql('cfr_soh2', con=engine2, if_exists='append', index=False)
	timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print(f"Data cfr_soh2 imported to database successfully at {timestamp}")
except FileNotFoundError:
	print("Error: The specified file was not found.")
except SQLAlchemyError as e:
	print(f"Database error occurred: {e}")
except Exception as e:
	print(f"An unexpected error occurred: {e}")
	
df.rename(columns={"data_date": "DATE"}, inplace=True)

try:
    
    df.to_sql(table_soh_update, con=engine1, if_exists='append', index=False)
    os.replace(path, path.with_suffix('.imported'))
    print("🗑️ File renamed to:", path.with_suffix('.imported'))
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Data imported to database successfully at {timestamp}")
except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)