from datetime import datetime
import os
import shutil
import polars as pl
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv


bu = "NEW"
stcode = "0021"
cntdate = "20260912"

load_dotenv(find_dotenv())

engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")

# 1. กำหนด Path และสร้าง SQLAlchemy Engine
master = "D:/Master.db"
engine = create_engine(f"sqlite:///{master}")

stocktakeid = f"{bu}{stcode}F{cntdate}"
print(stocktakeid)
storecode = stcode
stock = 0

path = "D:/Users/prthanap/Documents"
filename = "Item Master 26-09-03.xlsx"
pathfile = f"{path}/{filename}"

stocktake = text(f"select cntnum,stcode as storecode,branch as storename,bu,branch,stocktakeid from stocktakeid where stocktakeid = '{stocktakeid}'")
df_stocktakes = (pl.read_database(query=stocktake,connection=engine3)).unique()
row = df_stocktakes.row(0, named=True)
print(df_stocktakes)

location = text(f"select location_no as location,stocktakeid from location_master where stocktakeid = '{stocktakeid}'")
df_location = (pl.read_database(query=location,connection=engine3))
print(df_location)

masterbarcode = text(f"select barcodeibc,barcodeibc as sku,productname,packsize,retailprice,'A' as status from new_maxvalu_master where directconsign = 'Direct'")
df_master = (pl.read_database(query=masterbarcode,connection=engine3)
        .with_columns(
            pl.lit(row["stocktakeid"]).alias("stocktakeid"),
            pl.lit(row["storecode"]).alias("storecode"),
            pl.lit(row["storename"]).alias("storename"),
            pl.lit(stock).alias("stock"),
        )
)
print(df_master)


# 3. จัดการ Database
try:
    # 3.1 สั่ง Delete และ Update ข้อมูลผ่าน engine.begin() เพื่อจัดการ Transaction
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pda_masters;"))
        conn.execute(text("DELETE FROM sqlite_sequence WHERE name='pda_masters';"))
        conn.execute(text("DELETE FROM location_masters;"))
        conn.execute(text("DELETE FROM sqlite_sequence WHERE name='location_masters';"))

        query_update = text("""
            UPDATE stocktakes 
            SET countname = :stocktakeid, 
                storecode = :storecode, 
                storename = :storename, 
                bu = :bu, 
                branch = :branch
            WHERE id = 1
        """)
        conn.execute(
            query_update,
            {
                "stocktakeid": row["stocktakeid"],
                "storecode": row["storecode"],
                "storename": row["storename"],
                "bu": row["bu"],
                "branch": row["branch"]
            },
        )

    # 3.2 เขียน Polars DataFrame ลง SQLite ด้วย SQLAlchemy Engine
    df_master.write_database(
        table_name="pda_masters",
        connection=engine,             # ส่งตัวแปร engine เข้าไปโดยตรง
        if_table_exists="append",
        engine="sqlalchemy"            # ระบุ engine="sqlalchemy" ชัดเจน
    )

    df_location.write_database(
        table_name="location_masters",
        connection=engine,
        if_table_exists="append",
        engine="sqlalchemy"
    )

    print(f"Delete and Reset ID completed and data inserted into pda_masters: {len(df_master)}")
    print(f"Delete and Reset ID completed and data inserted into location_masters: {len(df_location)}")

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