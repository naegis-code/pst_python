from datetime import datetime
import os
import shutil
import polars as pl
from sqlalchemy import create_engine, text

# 1. กำหนด Path และสร้าง SQLAlchemy Engine
master = "D:/Master.db"
engine = create_engine(f"sqlite:///{master}")

bu = "NEW"
stcode = "000"
cntdate = "20260903"

stocktakeid = f"{bu}{stcode}{cntdate}"
storecode = stcode
storename = ""
stock = 0
status = "A"

path = "D:/Users/prthanap/Documents"
filename = "Item Master 26-09-03.xlsx"
pathfile = f"{path}/{filename}"

# 2. อ่านไฟล์และทำความสะอาดข้อมูลด้วย Polars
df = pl.read_excel(pathfile, sheet_name='Sheet1', infer_schema_length=0)
df = df.rename({col: col.lower() for col in df.columns})

df = (
    df.select([
        "fbarcode", 
        "itemid", 
        "itemname", 
        "directconsign", 
        "fbarcodeunit",
        "division",
        "group",
        "pcs_retail"
    ])
    .with_columns(
        pl.col(pl.String).str.replace_all(r"[,|\r|\n]", "")
    )
    .with_columns(
        pl.col("fbarcode").fill_null(pl.col("itemid"))
    )
    .with_columns(
        pl.col("fbarcode").str.zfill(13),
        pl.col("itemid").str.zfill(13),
    )
    .filter(
        pl.col("directconsign").str.to_lowercase() == "direct"
    )
)

# เตรียมข้อมูล df_barcode
df_barcode = (
    df.select(["fbarcode", "itemname", "directconsign", "fbarcodeunit", "division", "group", "pcs_retail"])
    .unique(subset=["fbarcode"], keep="first")
    .with_columns(
        pl.lit(stocktakeid).alias("stocktakeid"),
        pl.lit(storecode).alias("storecode"),
        pl.lit(storename).alias("storename"),
        pl.col("fbarcode").alias("sku"),
        pl.col("fbarcode").alias("barcodeibc"),
        pl.col("itemname").alias("productname"),
        pl.lit(stock).alias("stock"),
        pl.col("fbarcodeunit").alias("packsize"),
        pl.col("pcs_retail").alias("retailprice"),
        pl.lit(status).alias("status")
    )
    .select(["stocktakeid", "storecode", "storename", "sku", "barcodeibc", "productname", "stock", "packsize", "retailprice", "status"])
    .unique(subset=["barcodeibc"], keep="first")
)

# เตรียมข้อมูล df_sku
df_sku = (
    df.select(["itemid", "itemname", "directconsign", "fbarcodeunit", "division", "group", "pcs_retail"])
    .unique(subset=["itemid"], keep="first")
    .with_columns(
        pl.lit(stocktakeid).alias("stocktakeid"),
        pl.lit(storecode).alias("storecode"),
        pl.lit(storename).alias("storename"),
        pl.col("itemid").alias("sku"),
        pl.col("itemid").alias("barcodeibc"),
        pl.col("itemname").alias("productname"),
        pl.lit(stock).alias("stock"),
        pl.col("fbarcodeunit").alias("packsize"),
        pl.col("pcs_retail").alias("retailprice"),
        pl.lit(status).alias("status")
    )
    .select(["stocktakeid", "storecode", "storename", "sku", "barcodeibc", "productname", "stock", "packsize", "retailprice", "status"])
    .unique(subset=["barcodeibc"], keep="first")
)

df_master = pl.concat([df_barcode, df_sku]).unique(subset=["barcodeibc"], keep="first")

# 3. จัดการ Database
try:
    # 3.1 สั่ง Delete และ Update ข้อมูลผ่าน engine.begin() เพื่อจัดการ Transaction
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pda_masters;"))
        conn.execute(text("DELETE FROM sqlite_sequence WHERE name='pda_masters';"))

        query_update = text("""
            UPDATE stocktakes 
            SET countname = :stocktakeid, 
                storecode = :storecode, 
                storename = :storename, 
                bu = 'NEW', 
                branch = :storename 
            WHERE id = 1
        """)
        conn.execute(
            query_update,
            {
                "stocktakeid": stocktakeid,
                "storecode": storecode,
                "storename": storename,
            },
        )

    # 3.2 เขียน Polars DataFrame ลง SQLite ด้วย SQLAlchemy Engine
    df_master.write_database(
        table_name="pda_masters",
        connection=engine,             # ส่งตัวแปร engine เข้าไปโดยตรง
        if_table_exists="append",
        engine="sqlalchemy"            # ระบุ engine="sqlalchemy" ชัดเจน
    )

    print(f"Delete and Reset ID completed and data inserted into pda_masters: {len(df_master)}")

except Exception as e:
    print(f"Error during database operations: {e}")

# 4. บีบไฟล์ DB ด้วย VACUUM
with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT").execute(text("VACUUM;"))

# 5. Backup ไฟล์ DB และ Export Excel
current_time = datetime.now().strftime("%Y%m%d%H%M")
folder_path = os.path.dirname(master)

new_master_path = os.path.join(
    folder_path, f"Master_{stocktakeid}_{current_time}.db"
)
df_master.write_excel(f"{path}/df_master_{current_time}.xlsx")

shutil.copy2(master, new_master_path)

print(f"Backup file created successfully at: {new_master_path}")
print("export completed")