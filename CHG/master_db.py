import polars as pl
import sqlite3
import psycopg2
import db_connect as dbc
import pathlib
import tqdm
import os



# ตั้งค่าให้โชว์คอลัมน์ครบ (ไม่ตัด ...)
pl.Config.set_tbl_cols(-1)

user_path = pathlib.Path.home()

filename = 'Master260625'
path = user_path / 'Downloads' / f"{filename}.db"

engine_lite = sqlite3.connect(path)
engine3 = psycopg2.connect(dbc.db_url_pstdb3_native)

query_product = "SELECT * FROM PRODUCT"
query_masterfilelog = "SELECT MODIFIEDDATE FROM MASTERFILELOG WHERE FILENAME = 'PRODUCT'"

df = pl.read_database(query_product, connection=engine_lite)
df_log = pl.read_database(query_masterfilelog, connection=engine_lite).with_columns(pl.col('MODIFIEDDATE').cast(pl.Utf8)).unique('MODIFIEDDATE')
log_date = df_log['MODIFIEDDATE'].item(0)

df2 = df.clone().drop('PR_CODE').with_columns(pl.col('SKCODE').cast(pl.Utf8).alias('PR_CODE')).unique('PR_CODE')

df3 = pl.concat([df, df2], how='align').unique('PR_CODE').with_columns(
                pl.col('SKCODE').cast(pl.Utf8),
                pl.lit(log_date).str.to_datetime().alias('createddate')
                ).select(pl.all().name.to_lowercase())

try:
    # 1. ลบข้อมูลเก่าออกก่อน
    with engine3.cursor() as cursor:
        cursor.execute("DELETE FROM chg_product_master")
        print("Deleted existing records from chg_product_master")
        engine3.commit()

    # 2. แบ่งข้อมูลเป็น chunk ละ 10,000 แถว
    chunk_size = 10000
    total_chunks = (len(df3) + chunk_size - 1) // chunk_size 

    print(f"Starting insertion: Total rows = {len(df3):,}, Total chunks = {total_chunks}")
    '''
    with tqdm.tqdm(total=total_chunks, desc="Inserting Data", unit="chunk") as pbar:
        # แก้ไขจาก n=chunk_size เป็น n_rows=chunk_size ครับ
        for chunk_df in df3.iter_slices(n_rows=chunk_size):
            
            chunk_df.write_database(
                table_name='chg_product_master', 
                connection=dbc.db_url_pstdb3_native, 
                if_table_exists='append'
            )
            pbar.update(1)
    '''
    df3 = pd.
    print("Database sync completed successfully!")

except Exception as e:
    print(f"An unexpected error occurred: {e}")