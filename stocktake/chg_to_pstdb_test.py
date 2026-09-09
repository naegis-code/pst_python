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

sdate = '20260601'
edate = '20260631'

def to_database(sdate, edate):
    print(f"Processing data for date: {sdate} to {edate}")


    print(f"Start Process: {datetime.now()}")

    # ---------------------------------------------------------------------
    # 1. อ่านข้อมูลจาก engine3
    # ---------------------------------------------------------------------
    var_query = f"""
        SELECT *
        FROM chg_var_this_year
        WHERE cntdate BETWEEN '{sdate}' AND '{edate}'
    """

    stk_query = f"""
        SELECT *
        FROM chg_stk_this_year
        WHERE cntdate BETWEEN '{sdate}' AND '{edate}'
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
            print(f"Writing {total_rows_var} records to DB1 (chg_var)")
            df_var.write_database(
                table_name="chg_var",
                connection=engine1,
                if_table_exists="append",
                engine="adbc",
            )
            

            # ---------------------------------------------------------------------
            # 3. เมื่อเขียนลง engine1 สำเร็จค่อยลบข้อมูลออกจาก engine3 (ต้นทาง)
            # ---------------------------------------------------------------------
            with adbc_psycopg.connect(engine3) as conn:
                with conn.cursor() as cursor:
                    print(f"Deleting records from DB3 (chg_var_this_year) for cntdate BETWEEN '{sdate}' AND '{edate}'")
                    cursor.execute(f"DELETE FROM chg_var_this_year WHERE cntdate BETWEEN '{sdate}' AND '{edate}'")
                    conn.commit()


        else:
            print(f"No records found in DB3 (chg_var_this_year) for cntdate BETWEEN '{sdate}' AND '{edate}'")

    except Exception as e:
        print(f"An error occurred: {e}")

    print(f"End Process Var: {datetime.now()}")

    # ---------------------------------------------------------------------
    # บันทึกข้อมูลลง engine1_stk (ปลายทาง)
    # ---------------------------------------------------------------------

    try:
        if total_rows_stk > 0:
            print(f"Writing {total_rows_stk} records to DB1 (chg_stk)")
            df_stk.write_database(
                table_name="chg_stk",
                connection=engine1,
                if_table_exists="append",
                engine="adbc",
            )


            # ---------------------------------------------------------------------
            # 3. เมื่อเขียนลง engine1 สำเร็จค่อยลบข้อมูลออกจาก engine3 (ต้นทาง)
            # ---------------------------------------------------------------------
            with adbc_psycopg.connect(engine3) as conn:
                with conn.cursor() as cursor:
                    print(f"Deleting records from DB3 (chg_stk_this_year) for cntdate BETWEEN '{sdate}' AND '{edate}'")
                    cursor.execute(f"DELETE FROM chg_stk_this_year WHERE cntdate BETWEEN '{sdate}' AND '{edate}'")
                    conn.commit()
        else:
            print(f"No records found in DB3 (chg_stk_this_year) for cntdate BETWEEN '{sdate}' AND '{edate}'")

    except Exception as e:
        print(f"An error occurred: {e}")

    print(f"End Process Stk: {datetime.now()}")


to_database('20260601','20260630')
to_database('20260701','20260731')
to_database('20260801','20260831')

