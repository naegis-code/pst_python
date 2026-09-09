import os
from datetime import datetime, timedelta
import adbc_driver_postgresql.dbapi as adbc_psycopg  # เปลี่ยนเป็น Driver DB ที่คุณใช้
from dotenv import find_dotenv, load_dotenv
import polars as pl
from tqdm import tqdm

load_dotenv(find_dotenv())

engine1 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
engine2 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}"
engine3 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}"


date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
print(f"Processing data for date: {date}")


print(f"Start Process: {datetime.now()}")

# ---------------------------------------------------------------------
# 1. อ่านข้อมูลจาก engine3
# ---------------------------------------------------------------------
var_query = f"""
    SELECT *
    FROM chg_var_this_year
    WHERE cntdate = '{date}'
        and bu = 'CHG'
"""

stk_query = f"""
    SELECT *
    FROM chg_stk_this_year
    WHERE cntdate = '{date}'
        and bu = 'CHG'
"""

df_var = pl.read_database_uri(var_query, uri=engine3)
total_rows_var = len(df_var)
print(f"Total rows in chg_var_this_year: {total_rows_var}")

df_stk = pl.read_database_uri(stk_query, uri=engine3)
total_rows_stk = len(df_stk)
print(f"Total rows in chg_stk_this_year: {total_rows_stk}")

# ---------------------------------------------------------------------
# 2. บันทึกข้อมูลลง engine1 (ปลายทาง)
# ---------------------------------------------------------------------
try:
    if total_rows_var > 0:
        df_var.write_database(
            table_name="chg_var",
            connection=engine1,
            if_table_exists="append",
            engine="adbc",
        )
        print(f"Written {total_rows_var} records to DB1 (chg_var)")

        # ---------------------------------------------------------------------
        # 3. เมื่อเขียนลง engine1 สำเร็จค่อยลบข้อมูลออกจาก engine3 (ต้นทาง)
        # ---------------------------------------------------------------------
        with adbc_psycopg.connect(engine3) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM chg_var_this_year WHERE cntdate = '{date}'")
                conn.commit()
                conn.autocommit = True
                cursor.execute("vacuum full chg_var_this_year")
                conn.autocommit = False
        print(f"Deleted records from DB3 (chg_var_this_year) for cntdate = '{date}'")
    else:
        print(f"No records found in DB3 (chg_var_this_year) for cntdate = '{date}'")

except Exception as e:
    print(f"An error occurred: {e}")

print(f"End Process Var: {datetime.now()}")

# ---------------------------------------------------------------------
# บันทึกข้อมูลลง engine1_stk (ปลายทาง)
# ---------------------------------------------------------------------

try:
    if total_rows_stk > 0:
        df_stk.write_database(
            table_name="chg_stk",
            connection=engine1,
            if_table_exists="append",
            engine="adbc",
        )
        print(f"Written {total_rows_stk} records to DB1 (chg_stk)")

        # ---------------------------------------------------------------------
        # 3. เมื่อเขียนลง engine1 สำเร็จค่อยลบข้อมูลออกจาก engine3 (ต้นทาง)
        # ---------------------------------------------------------------------
        with adbc_psycopg.connect(engine3) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM chg_stk_this_year WHERE cntdate = '{date}'")
                conn.commit()
                conn.autocommit = True
                cursor.execute("vacuum full chg_stk_this_year")
                conn.autocommit = False
        print(f"Deleted records from DB3 (chg_stk_this_year) for cntdate = '{date}'")
    else:
        print(f"No records found in DB3 (chg_stk_this_year) for cntdate = '{date}'")

except Exception as e:
    print(f"An error occurred: {e}")

print(f"End Process Stk: {datetime.now()}")



