from datetime import datetime
import os
import shutil
import polars as pl
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

path = "D:/Users/prthanap/Downloads"
filename = "Item Master 26-09-09.xlsx"
pathfile = f"{path}/{filename}"
#ItemMaster+Cat
tablename = "new_maxvalu_master"

engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")

as_date = datetime.now().strftime("%Y%m%d")



# 2. อ่านไฟล์และทำความสะอาดข้อมูลด้วย Polars
df = (pl.read_excel(pathfile, sheet_name="Sheet1", infer_schema_length=0)
      .with_columns(
        pl.col(pl.String).str.replace_all(r"[,|\r|\n]", "")
    )
)
df = df.rename({col: col.lower() for col in df.columns})

df_barcode = (
    df.select([
        "fbarcode", 
        "itemid", 
        "itemname", 
        "directconsign", 
        "fbarcodeunit",
        "division",
        "group",
        "pcs_retail",
        "unitcost"
    ])
    .with_columns(
        pl.col("fbarcode").fill_null(pl.col("itemid"))
    )
    .with_columns(
        pl.col("fbarcode").str.zfill(13),
        pl.col("itemid").str.zfill(13),
    )
)

df_sku = (
    df.select([
        pl.col("itemid").alias("fbarcode"),
        "itemid", 
        "itemname", 
        "directconsign", 
        "fbarcodeunit",
        "division",
        "group",
        "pcs_retail",
        "unitcost"
    ])
    .with_columns(
        pl.col("itemid").str.zfill(13),
        pl.col("fbarcode").str.zfill(13),
    )
)

mapping_columns = {
    "fbarcode": "barcodeibc",
    "itemid": "sku",
    "itemname": "productname",
    "directconsign": "directconsign",
    "fbarcodeunit": "packsize",
    "division": "dept",
    "group": "subdept",
    "pcs_retail": "retailprice",
    "unitcost": "costprice"
}

df_all = (
    pl.concat([df_barcode, df_sku])
    .unique(subset=["fbarcode"], keep="first")
    .with_columns(
        pl.col("pcs_retail").cast(pl.Decimal(21, 3)),
        pl.col("unitcost").cast(pl.Decimal(21, 3)),
    )
    .rename(mapping_columns)
)

df_all.write_database(table_name=tablename, connection=engine3, if_table_exists="replace")

print(f"Data has been written to the database. {len(df_all)} rows.")


