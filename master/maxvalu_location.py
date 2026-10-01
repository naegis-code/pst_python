import os
import pathlib
import polars as pl
from sqlalchemy import create_engine,text
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

bu = 'NEW'
stcode = '0042'
cntdate = '20260924'
stocktakeid = f"{bu}{stcode}F{cntdate}"

filename = '042 Lay Out Stocktakking  Sale Area & Back room 24092026 V2'
extention = '.xlsx'


path = pathlib.Path().home()

# Determine host based on user profile
if path.name == "prthanap":
    db_host = os.getenv("DB_HOST")
elif path.name == "shthanapat":
    db_host = "103.22.182.82"
else:
    raise ValueError(f"Unsupported user environment: {path.name}")

# Build connection strings safely
db_base = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{db_host}:{os.getenv('DB_PORT')}"
engine0 = create_engine(f"{db_base}/{os.getenv('DB_pstdb')}")
engine3 = create_engine(f"{db_base}/{os.getenv('DB_pstdb3')}")

file = path / "Downloads" / f"{filename}{extention}"

df = (pl.read_excel(file,sheet_name = 'Gon Request',columns=[0, 1, 2],has_header=False,read_options={'skip_rows': 1},infer_schema_length = 0)
      .with_columns([
          pl.col("column_1").str.replace_all("'", "").str.zfill(5),
          pl.col("column_2").str.replace_all("'", "").str.zfill(5),
          pl.col("column_3").str.replace_all("'", "").str.zfill(5)
      ])
     .unpivot().drop_nulls(subset=["value"]).sort("value").unique(subset=["value"],keep="last")
     .with_columns([
         pl.lit(stocktakeid).alias("stocktakeid")
     ])
)

df_location_master = df.rename({"value": "location_no"}).select(["location_no", "stocktakeid"])

df_location_recount = df.rename({"value": "location"}).filter(pl.col("variable").is_in(["column_2","column_3"])).select(["location", "stocktakeid"])

df_check = pl.read_database(query=
    text(f"SELECT 1 FROM location_master WHERE stocktakeid = '{stocktakeid}'"),
    connection=engine3
)

if df_check.is_empty():
    
    df_location_master.write_database(table_name="location_master", connection=engine3, if_table_exists="append")
    print(f"Inserting location_master for stocktakeid: {stocktakeid} records:{len(df_location_master)}")
    
    df_location_recount.write_database(table_name="location_recount", connection=engine3, if_table_exists="append")
    print(f"Inserting location_recount for stocktakeid: {stocktakeid} records:{len(df_location_recount)}")

else:
    print("Records found in location for the given stocktakeid.")





