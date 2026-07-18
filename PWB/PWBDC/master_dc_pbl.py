import polars as pl
import pathlib
import os
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

load_dotenv(pathlib.Path(__file__).parent / '.env')

db_url = os.getenv('database_connection_string')
table_name = 'pwb_master_pbl'

filename = 'Master PBL.xls'
user_path = pathlib.Path.home()
sub_folder = 'Downloads'
file_path = user_path / sub_folder / filename

df = pl.read_excel(file_path, sheet_name='Table1', infer_schema_length=None)

df = (df
      .with_columns(pl.col(pl.String).str.strip_chars())
      .with_columns(pl.col(pl.String).str.replace_all(r"[\r\n]+", " "))
      .with_columns(pl.col(pl.String).str.replace_all(",", ""))
      .with_columns(pl.col(pl.String).str.replace_all(r"\s+", " ").str.strip_chars())
      .with_columns(pl.col(pl.String).replace("", None))
      .with_columns(
            pl.col('Barcode1', 'Barcode2', 'Barcode3', 'Barcode4', 'Barcode5','Barcode6','Barcode7','Barcode8','Barcode9','Barcode10').str.zfill(13),
            pl.lit(datetime.date.today()).cast(pl.Datetime).alias('as_date')
        )
      )

print(f"อ่านและคลีนข้อมูลจากไฟล์เรียบร้อย: {df.height} แถว")

engine = create_engine(db_url)

if inspect(engine).has_table(table_name):
    with engine.connect() as conn:
        table_columns = [
            row[0] for row in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :t ORDER BY ordinal_position"
                ),
                {'t': table_name},
            )
        ]

    # ชื่อคอลัมน์ในตารางถูกแปลงเป็นตัวพิมพ์เล็ก/ตัดให้สั้นลงตอน import ครั้งแรก
    # จับคู่ตามลำดับตำแหน่งคอลัมน์แทนชื่อ เพื่อให้ rename ตรงกับตารางจริง
    if len(table_columns) == df.width:
        df = df.rename(dict(zip(df.columns, table_columns)))
    else:
        print(f"คำเตือน: จำนวนคอลัมน์ไม่ตรงกับตาราง ({df.width} vs {len(table_columns)}) ข้ามการจับคู่ชื่อคอลัมน์")

    with engine.connect() as conn:
        existing = pl.read_database(
            query=text(f'SELECT DISTINCT "sku", "as_date" FROM {table_name}'),
            connection=conn,
        )
    df_new = df.join(existing, on=['sku', 'as_date'], how='anti')
else:
    print(f"ยังไม่มีตาราง '{table_name}' ในฐานข้อมูล จะสร้างใหม่และนำเข้าข้อมูลทั้งหมด")
    df = df.rename({c: c.lower() for c in df.columns})
    df_new = df

if df_new.height == 0:
    print("ไม่มีข้อมูลใหม่ (SKU + as_date) ที่ต้อง import เข้าฐานข้อมูล")
else:
    df_new.write_database(
        table_name=table_name,
        connection=db_url,
        if_table_exists='append',
    )
    print(f"นำเข้าข้อมูลใหม่ {df_new.height} แถว เข้าตาราง '{table_name}' เรียบร้อยแล้ว")

# =========================================================
# ส่วนแปลงข้อมูลจาก pwb_master_pbl เข้า pwb_master_trans
# =========================================================
trans_table_name = 'pwb_master_trans'

trans_query = f"""
    SELECT sku as skcode,
        concat_ws(',',barcode1,barcode2,barcode3,barcode4,barcode5,barcode6,barcode7,barcode8,barcode9,barcode10) as barcode,
        "ชื่อสินค้า" as prname,
        "ชื่อยี่ห้อ" as bndcode,
        "รุ่น" as prmodel,
        "ชื่อสี" as clrcode,
        "ชื่อเต็ม หน่วยขาย" as pkcode,
        'PBL' as "group",
        as_date
    FROM {table_name}
"""

with engine.connect() as conn:
    df_trans = pl.read_database(query=text(trans_query), connection=conn)

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
