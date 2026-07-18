import polars as pl
import pathlib
import sqlite3
import datetime
import os
import shutil
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(pathlib.Path(__file__).parent / '.env')
pg_db_url = os.getenv('database_connection_string')

# --- ตั้งค่าตัวแปรเบื้องต้น ---
bu = 'PWB'
store = 'DC'
cntdate = '20260718'
atype = '3F'

stocktakeid = bu + store + atype[1:2] + cntdate
print(f"Stocktake ID: {stocktakeid}")

db_path = r"D:\Master.db"
table_stocktakes = 'stocktakes'
table_product = 'pda_masters'
table_location = 'location_masters'
table_users = 'users'
pg_db_url_pstdb = pg_db_url.replace('/pstdb3', '/pstdb')
fixed_password_hash = '$2a$10$7GuGVla.6OBZ2Qp1ocXzyOGdhABhQBOBC1Dxs3qXO7Oo0pSAJ8Eb2'

# --- เตรียมข้อมูลสำหรับตาราง stocktakes ---
df_stocktake = pl.DataFrame({
    'countName': [stocktakeid],
    'StoreCode': [store],
    'StoreName': [store],
    'BU': [bu],
    'branch': [store],
    'startTime': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
    'endTime': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
})

# --- 1. โหลดข้อมูลสินค้าจาก pwb_mms_product ---
# skcode หนึ่งตัวมีได้หลาย pr_code (บาร์โค้ด) ต่อแถว จึง pivot pr_code ให้กลายเป็น Barcode1-10 ต่อ 1 skcode
product_query = """
    WITH ranked AS (
        SELECT skcode, pr_code, prname, bndcode, prmodel, clrcode, szcode,
            ROW_NUMBER() OVER (PARTITION BY skcode ORDER BY pr_code) AS rn
        FROM pwb_mms_product
        WHERE as_date >= '20260101'
    )
    SELECT
        skcode,
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
        MAX(szcode) AS szcode
    FROM ranked
    GROUP BY skcode
"""

pg_engine = create_engine(pg_db_url)
with pg_engine.connect() as pg_conn:
    df_product = pl.read_database(query=text(product_query), connection=pg_conn, infer_schema_length=None)
pg_engine.dispose()

df_product = (
    df_product
    .with_columns(
        pl.col('skcode').str.zfill(13).alias('BarcodeIBC'),
        pl.lit(0).cast(pl.Float64).alias('RetailPrice'),
    )
    .rename({
        'skcode': 'SKU',
        'barcode1': 'Barcode1',
        'barcode2': 'Barcode2',
        'barcode3': 'Barcode3',
        'barcode4': 'Barcode4',
        'barcode5': 'Barcode5',
        'barcode6': 'Barcode6',
        'barcode7': 'Barcode7',
        'barcode8': 'Barcode8',
        'barcode9': 'Barcode9',
        'barcode10': 'Barcode10',
        'prname': 'ProductName',
        'bndcode': 'BrandName',
        'prmodel': 'Model',
        'clrcode': 'Color',
        'szcode': 'Size',
    })
    .with_columns(
        pl.lit(stocktakeid).alias('stocktakeId'), # ยัด ID เชื่อมตารางให้ตรงกับในฐานข้อมูล
        pl.lit(store).alias('StoreCode'),
        pl.lit(store).alias('StoreName'),
        pl.lit('A').alias('Status')
    )
)

# --- 2. โหลดข้อมูล location จาก Postgres (pwbdc_sohupdate) ---
pg_engine = create_engine(pg_db_url)
location_query = """
    SELECT DISTINCT "location"
    FROM pwbdc_sohupdate
    WHERE cntdate >= '20260101'
"""
with pg_engine.connect() as pg_conn:
    df_location = pl.read_database(query=text(location_query), connection=pg_conn)
pg_engine.dispose()

df_location = df_location.with_columns(
    pl.lit(stocktakeid).alias('stocktakeId')
)

# --- 3. โหลดข้อมูล user (employee_code จาก pstdb.public.employees + ajis/pcs/ino) ---
pg_engine = create_engine(pg_db_url_pstdb)
with pg_engine.connect() as pg_conn:
    df_employees = pl.read_database(
        query=text('SELECT DISTINCT employee_code FROM employees WHERE employee_code IS NOT NULL'),
        connection=pg_conn,
    )
pg_engine.dispose()

df_employees = df_employees.rename({'employee_code': 'username'}).with_columns(
    pl.col('username').alias('empCode')
)

extra_usernames = (
    [f'ajis{i:02d}' for i in range(1, 51)]
    + [f'pcs{i:02d}' for i in range(1, 51)]
    + [f'ino{i:02d}' for i in range(1, 51)]
)
df_extra = pl.DataFrame({'username': extra_usernames, 'empCode': [None] * len(extra_usernames)})

df_users = pl.concat([df_employees, df_extra], how='vertical').with_columns(
    pl.lit(fixed_password_hash).alias('encryptedPassword'),
    pl.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('createdOn'),
    pl.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('updatedOn'),
)

print("\n--- ตัวอย่างข้อมูลที่กำลังจะนำเข้า ---")
print(df_stocktake)
print(df_product.head(3)) # แสดงตัวอย่างข้อมูลสินค้า 3 แถวแรก
print(df_location.head(3)) # แสดงตัวอย่างข้อมูล location 3 แถวแรก
print(df_users.head(3)) # แสดงตัวอย่างข้อมูล user 3 แถวแรก

# =========================================================
# ส่วนจัดการฐานข้อมูล (เคลียร์ข้อมูลเก่า)
# =========================================================

# [แก้ไขจุดที่ 1]: ต้องประกาศเปิดเชื่อมต่อ engine ตรงนี้ก่อนเปิดใช้งาน cursor
engine = sqlite3.connect(db_path)
cursor = engine.cursor()

target_tables = [table_stocktakes, table_product, table_location, table_users]

print("\nกำลังเคลียร์ข้อมูลเก่าและรีเซ็ตค่า ID ของทุกตาราง...")
for table in target_tables:
    # 1. ลบข้อมูลทั้งหมดออกจากตาราง
    cursor.execute(f"DELETE FROM {table};")
    
    # 2. รีเซ็ตเลข Auto-Increment ใน sqlite_sequence
    cursor.execute(f"UPDATE sqlite_sequence SET seq = 0 WHERE name = '{table}';")

# ยืนยันการลบข้อมูล (Commit)
engine.commit()

# 3. สั่ง VACUUM จัดระเบียบและบีบอัดขนาดไฟล์ฐานข้อมูล
print("กำลังจัดระเบียบและบีบอัดขนาดไฟล์ฐานข้อมูล (VACUUM)...")
cursor.execute("VACUUM;")

# [สำคัญที่สุด]: ปิดการเชื่อมต่อดั้งเดิมทันที เพื่อปลดล็อกไฟล์ให้ Polars นำข้อมูลเข้าต่อได้
cursor.close()
engine.close()

print("เคลียร์ระบบเก่าและปลดล็อกฐานข้อมูลเรียบร้อยแล้ว...")

# =========================================================
# ส่วนนำข้อมูลเข้า SQLite โดยใช้ Polars
# =========================================================
print("ระบบพร้อมแล้ว กำลังนำข้อมูลใหม่เข้า...")

# 4. นำข้อมูลใหม่เข้าตาราง stocktakes
df_stocktake.write_database(
    table_name=table_stocktakes,
    connection=f"sqlite:///{db_path}",
    if_table_exists="append"
)

# 5. นำข้อมูลใหม่เข้าตาราง pda_masters
df_product.write_database(
    table_name=table_product,
    connection=f"sqlite:///{db_path}",
    if_table_exists="append"
)

# 6. นำข้อมูลใหม่เข้าตาราง location_masters
df_location.write_database(
    table_name=table_location,
    connection=f"sqlite:///{db_path}",
    if_table_exists="append"
)

# 7. นำข้อมูลใหม่เข้าตาราง users
df_users.write_database(
    table_name=table_users,
    connection=f"sqlite:///{db_path}",
    if_table_exists="append"
)

print("ล้างฐานข้อมูลเก่า และนำเข้าข้อมูลใหม่ของทั้ง 4 ตารางสำเร็จเรียบร้อยแล้วครับ! 🚀")

# 8. สำเนาไฟล์ฐานข้อมูลเก็บไว้เป็น {stocktakeid}.db
backup_path = pathlib.Path(db_path).with_name(f"{stocktakeid}.db")
shutil.copy2(db_path, backup_path)
print(f"สำเนาฐานข้อมูลไปยัง '{backup_path}' เรียบร้อยแล้ว")