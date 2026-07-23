import polars as pl
import pathlib
import os
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

load_dotenv(pathlib.Path(__file__).parent / '.env')

db_url = os.getenv('database_connection_string')
table_name = 'pwb_mms_product'

filename = 'Master260718.db'
user_path = pathlib.Path.home()
sub_folder = 'Downloads'
file_path = user_path / sub_folder / filename

df = pl.read_database(query=text('SELECT * FROM "PRODUCT"'), connection=create_engine(f"sqlite:///{file_path}"))
df = df.rename({c: c.lower() for c in df.columns})

df = (df
      .with_columns(pl.col(pl.String).str.to_lowercase())
      .with_columns(pl.col(pl.String).str.strip_chars())
      .with_columns(pl.col(pl.String).str.replace_all(r"[\r\n]+", " "))
      .with_columns(pl.col(pl.String).str.replace_all(",", ""))
      .with_columns(pl.col(pl.String).str.replace_all(r"\s+", " ").str.strip_chars())
      .with_columns(pl.col(pl.String).replace("", None))
      .with_columns(
            pl.col('pr_code').str.zfill(13),
            pl.lit(datetime.date.today()).cast(pl.Datetime).alias('as_date')
        )
      )
print(df)
print(f"อ่านและคลีนข้อมูลจากไฟล์เรียบร้อย: {df.height} แถว")

engine = create_engine(db_url)

if inspect(engine).has_table(table_name):
    with engine.connect() as conn:
        existing = pl.read_database(
            query=text(f'SELECT DISTINCT pr_code, as_date FROM {table_name}'),
            connection=conn,
        )
    df_new = df.join(existing, on=['pr_code', 'as_date'], how='anti')
else:
    print(f"ยังไม่มีตาราง '{table_name}' ในฐานข้อมูล จะสร้างใหม่และนำเข้าข้อมูลทั้งหมด")
    df_new = df

if df_new.height == 0:
    print("ไม่มีข้อมูลใหม่ (pr_code + as_date) ที่ต้อง import เข้าฐานข้อมูล")
else:
    df_new.write_database(
        table_name=table_name,
        connection=db_url,
        if_table_exists='append',
    )
    print(f"นำเข้าข้อมูลใหม่ {df_new.height} แถว เข้าตาราง '{table_name}' เรียบร้อยแล้ว")

# =========================================================
# ส่วนแปลงข้อมูลจาก pwb_mms_product เข้า pwb_master_trans
# =========================================================
trans_table_name = 'pwb_master_trans'

# skcode หนึ่งตัวมีได้หลาย pr_code (บาร์โค้ด) ต่อแถว จึง pivot pr_code ให้กลายเป็น barcode1-10
# ก่อน concat_ws รวมเป็นสตริงเดียว (เหมือนที่ทำใน master_dc_master_to_db.py)
trans_query = f"""
    WITH ranked AS (
        SELECT skcode, pr_code, prname, bndcode, prmodel, clrcode, pkcode, as_date,
            ROW_NUMBER() OVER (PARTITION BY skcode, as_date ORDER BY pr_code) AS rn
        FROM {table_name}
    ),
    pivoted AS (
        SELECT skcode, as_date,
            MAX(CASE WHEN rn = 1 THEN pr_code END) AS barcode1,
            MAX(CASE WHEN rn = 2 THEN pr_code END) AS barcode2,
            MAX(CASE WHEN rn = 3 THEN pr_code END) AS barcode3,
            MAX(CASE WHEN rn = 4 THEN pr_code END) AS barcode4,
            MAX(CASE WHEN rn = 5 THEN pr_code END) AS barcode5,
            MAX(CASE WHEN rn = 6 THEN pr_code END) AS barcode6,
            MAX(CASE WHEN rn = 7 THEN pr_code END) AS barcode7,
            MAX(CASE WHEN rn = 8 THEN pr_code END) AS barcode8,
            MAX(CASE WHEN rn = 9 THEN pr_code END) AS barcode9,
            MAX(CASE WHEN rn = 10 THEN pr_code END) AS barcode10,
            MAX(prname) AS prname,
            MAX(bndcode) AS bndcode,
            MAX(prmodel) AS prmodel,
            MAX(clrcode) AS clrcode,
            MAX(pkcode) AS pkcode
        FROM ranked
        GROUP BY skcode, as_date
    )
    SELECT skcode,
        concat_ws(',', barcode1,barcode2,barcode3,barcode4,barcode5,barcode6,barcode7,barcode8,barcode9,barcode10) as barcode,
        prname,
        bndcode,
        prmodel,
        clrcode,
        pkcode,
        'Store' as "group",
        as_date
    FROM pivoted
"""

with engine.connect() as conn:
    df_trans = pl.read_database(query=text(trans_query), connection=conn, infer_schema_length=None)

if inspect(engine).has_table(trans_table_name):
    with engine.connect() as conn:
        existing_trans = pl.read_database(
            query=text(f'SELECT DISTINCT skcode, "group", as_date FROM {trans_table_name}'),
            connection=conn,
        )
    df_trans_new = df_trans.join(existing_trans, on=['skcode', 'group', 'as_date'], how='anti')
else:
    print(f"ยังไม่มีตาราง '{trans_table_name}' ในฐานข้อมูล จะสร้างใหม่และนำเข้าข้อมูลทั้งหมด")
    df_trans_new = df_trans

if df_trans_new.height == 0:
    print(f"ไม่มีข้อมูลใหม่ (skcode + group + as_date) ที่ต้อง import เข้าตาราง '{trans_table_name}'")
else:
    df_trans_new.write_database(
        table_name=trans_table_name,
        connection=db_url,
        if_table_exists='append',
    )
    print(f"นำเข้าข้อมูลใหม่ {df_trans_new.height} แถว เข้าตาราง '{trans_table_name}' เรียบร้อยแล้ว")

engine.dispose()

