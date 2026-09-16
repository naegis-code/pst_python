import polars as pl
import pathlib
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

pl.Config.set_tbl_cols(-1)

load_dotenv(pathlib.Path(__file__).parent / '.env')
db_url = os.getenv('database_connection_string')
table_name = 'cntfiles_this_year'

user_path = pathlib.Path.home()
sub_folder = 'Central Group\\DTAP - เอกสาร\\PWBDC2026\\countfiles'

full_path = user_path / sub_folder

dfs = []
for file in full_path.rglob('*.csv'):
    if file.name == "combined.csv":
        continue
    
    df = pl.read_csv(file, infer_schema_length=0, quote_char=None)
    print(f"{file.relative_to(full_path)}: {df.height} แถว")

    df = df.with_columns(pl.lit(str(file.relative_to(full_path))).alias("source_file"))
    dfs.append(df)

if not dfs:
    print("ไม่พบไฟล์ CSV เลย ลองเช็ค path หรือโครงสร้างโฟลเดอร์อีกครั้ง")
else:
    combined_df = pl.concat(dfs, how="vertical_relaxed").select(pl.all().name.to_lowercase())
    print(combined_df)
    result = pl.sql("""SELECT 
                        rowid,
                        stocktakeid,
                        docnum,
                        macaddress,
                        inspector,
                        seq,
                        location,
                        sku,
                        barcode,
                        productname,
                        qnt,
                        saleprice,
                        datetime,
                        expirydate,
                        remark,
                        '20260719' AS cntdate,
                        source_file
                    FROM combined_df""").collect()
    
    result = result.with_columns(
    pl.col("source_file").str.split("\\").list.get(0).alias("bu"),
    pl.col("source_file").str.split("\\").list.get(1).alias("stcode"),
    ).select(
    "rowid", "stocktakeid", "docnum", "macaddress", "inspector", "seq",
    "location", "sku", "barcode", "productname", "qnt", "saleprice",
    "datetime", "expirydate", "remark","cntdate", "bu", "stcode"
    )

    result = result.with_columns(
        pl.col("bu").replace({"PBL": "PWBPBL", "Store": "PWBDC"})
    )

    #combined_df.write_csv(output_path)
    print(result)

    # =========================================================
    # เทียบ docnum กับ pstdb3.cntfiles_this_year แล้ว import เฉพาะที่ยังไม่มี
    # =========================================================
    engine = create_engine(db_url)

    if inspect(engine).has_table(table_name):
        with engine.connect() as conn:
            existing_docnum = pl.read_database(
                query=text(f'SELECT DISTINCT docnum FROM {table_name}'),
                connection=conn,
            )
        result_new = result.join(existing_docnum, on='docnum', how='anti')
    else:
        print(f"ยังไม่มีตาราง '{table_name}' ในฐานข้อมูล จะสร้างใหม่และนำเข้าข้อมูลทั้งหมด")
        result_new = result

    if result_new.height == 0:
        print("ไม่มี docnum ใหม่ที่ต้อง import เข้าฐานข้อมูล")
    else:
        
        result_new.write_database(
            table_name=table_name,
            connection=db_url,
            if_table_exists='append',
        )
        print(f"นำเข้าข้อมูลใหม่ {result_new.height} แถว เข้าตาราง '{table_name}' เรียบร้อยแล้ว")

    engine.dispose()