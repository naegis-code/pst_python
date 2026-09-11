import os
import pandas as pd
from sqlalchemy import create_engine, text
import pathlib
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
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


# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

table = 'cnext'
f_path = filepath / 'Report' / '2022' / '92 Time Sheet' / 'CNEXT'
log_path = filepath / 'Apps' / 'log.csv'
timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

scriptsname = 'cnext.py'

#D:\Users\prthanap\Central Group\PST Performance Team - เอกสาร\Report\2022\92 Time Sheet\CNEXT
excel_files = [f for f in os.listdir(f_path) if f.endswith(('.xlsx', '.xls')) and os.path.getsize(os.path.join(f_path, f)) > 0]

# PostgreSQL connection details
plan_query = f"select employee_id ,time_off_date , id from cnext"
df_plan = pd.read_sql(plan_query, con=engine1)
df_plan['time_off_date'] = pd.to_datetime(df_plan['time_off_date']).dt.strftime('%Y-%m-%d')

for file in excel_files:
    file_path = f_path / file
    try:
        df = pd.read_excel(file_path, sheet_name='Sheet1', skiprows=6, usecols="A:AA", dtype=str)

        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

        columns_to_keep = ['Employee Name', 'Employee ID', 'Time Off Type', 'Time Off Date',
                           'Start Time', 'End Time', 'Value', 'Status', 'Request Submitted Date']
        df = df[columns_to_keep]

        df[['Employee Name', 'Employee ID']] = df[['Employee Name', 'Employee ID']].ffill()
        df = df.infer_objects(copy=False)
        df = df[df['Time Off Date'].notna()]

        if 'TaSingha' in file:
            df['Time Off Date'] = pd.to_datetime(df['Time Off Date'], format='%A %m/%d/%Y', errors='coerce')
        else:
            df['Time Off Date'] = pd.to_datetime(df['Time Off Date'], format='%A %d/%m/%Y', errors='coerce')

        df['Time Off Date'] = df['Time Off Date'].dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates()

        types_to_exclude = ["Off-Site Work (ปฏิบัติงานนอกสถานที่ )", "OT Request (ขออนุมัติเวลาการทำ OT)"]
        df['Time Off Type'] = df['Time Off Type'].str.strip()
        df = df[~df['Time Off Type'].isin(types_to_exclude)]

        df = df.rename(columns={
            'Employee Name': 'employee_name',
            'Employee ID': 'employee_id',
            'Time Off Type': 'time_off_type',
            'Time Off Date': 'time_off_date',
            'Start Time': 'start_time',
            'End Time': 'end_time',
            'Value': 'value',
            'Status': 'status',
            'Request Submitted Date': 'request_submitted_date'
        })

        join_plan = df.merge(df_plan[['employee_id', 'time_off_date', 'id']],
                             on=['employee_id', 'time_off_date'], how='left')
        df = join_plan[join_plan['id'].isnull()].drop(columns=['id'])

        # Count the number of records before inserting
        record_count = len(df)

        # Log the number of records and other details
        log_entry = [scriptsname, file, 'Data inserted' if record_count > 0 else 'No valid data after filtering', record_count, timestamp]
        
        if record_count > 0:
            df.to_sql(table, engine1, if_exists='append', index=False)
            print(f"✅ Data inserted into '{table}' from {file} at {timestamp} with {record_count} records")
        else:
            print(f"No valid data in {file}")

        # Append log entry to the log file
        log_df = pd.DataFrame([log_entry], columns=['scriptsname', 'filename', 'status', 'record', 'timestamp'])
        log_df.to_csv(log_path, mode='a', index=False, header=not log_path.exists())

    except Exception as e:
        print(f"❌ Error processing {file}: {e}")
        log_entry = [scriptsname, file, str(e), 0, timestamp]
        log_df = pd.DataFrame([log_entry], columns=['scriptsname', 'filename', 'status', 'record', 'timestamp'])
        log_df.to_csv(log_path, mode='a', index=False, header=not log_path.exists())