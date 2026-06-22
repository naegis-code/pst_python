import polars as pl
from sqlalchemy import create_engine,text
import db_connect as dbc
import tqdm

# ตั้งค่าให้โชว์คอลัมน์ครบ (ไม่ตัด ...)
pl.Config.set_tbl_cols(-1)

sync_engine_test = create_engine(dbc.db_url_pstdbtest_p)
sync_engine = create_engine(dbc.db_url_pstdb)


#Parameters
bu = 'CFR'
stcode = '7011'
cntdate = '20261106'

filename = 'CHK7011-140225-150819'
path = f'D:/Users/prthanap/Downloads/{filename}.txt'

table_name = 'master'
stocktakeid = f'{bu}_{stcode}_{cntdate}'

# อ่านไฟล์ CSV ด้วย Polars
#'cyclecount', 'stcode', 'barcode', 'description', 'dept_code', 'dept_name', 'sdept_code', 'sdept_name', 'soh', 'pack','cost', 'retail', 'status'
df = pl.read_csv(path, separator=',',quote_char='"',skip_rows=1, has_header=False, encoding='cp874',infer_schema=False ,
                 new_columns=[]).with_columns(
                                  pl.lit(stocktakeid).alias('stocktakeid'),
                                  pl.lit(bu).alias('bu'),
                                  pl.lit(stcode).alias('stcode'),
                                  pl.lit(cntdate).alias('cntdate')
                                  ).rename({
                                    'column_2': 'cyclecount',
                                    'column_3': 'barcode',
                                    'column_4': 'sku',
                                    'column_5': 'description',
                                    'column_6': 'pack',
                                    'column_7': 'soh',
                                    'column_8': 'retail'
                                    }).drop(['column_1','column_9','column_10','column_11']).drop_nulls(subset=['sku'])  # ลบแถวที่มีค่า null ในคอลัมน์ 'sku'

df2 = df.clone()  # สร้างสำเนาของ DataFrame df

# ลบคอลัมน์ 'barcode' ออกจาก DataFrame df2
df2 = df2.drop('barcode').with_columns(pl.col('sku').alias('barcode'))  # เพิ่ม column 'barcode' ใหม่โดยใช้ค่าเดียวกับ 'sku'


query = f"SELECT DISTINCT stocktakeid FROM {table_name} WHERE stocktakeid = '{stocktakeid}'"   # ใช้เงื่อนไขที่ไม่คืนค่าใดๆ เพื่อสร้างโครงสร้างตาราง
query_plan = f"select bu,stcode,cntdate,branch from planall2 where bu = '{bu}' and stcode = '{stcode}' and cntdate = '{cntdate}'"
query_stocktake_id = f"select stocktakeid from stocktake_id where stocktakeid = '{stocktakeid}'"

df_query = pl.read_database(query, sync_engine_test)
df_plan = pl.read_database(query_plan, sync_engine)
df_stocktake_id = pl.read_database(query_stocktake_id, sync_engine_test)

print("ข้อมูล", df)

print("ข้อมูลสำเนา", df2)

df3 = pl.concat([df, df2],how='diagonal').unique(subset=['barcode','sku'])

print("ข้อมูลรวม", df3)

print("ข้อมูลแผน", df_plan)

print("ข้อมูลเดิม master", df_query)

print("ข้อมูล stocktake_id", df_stocktake_id)

# --- จุดแก้ไขที่ 1: เช็กก่อนว่า df_plan มีข้อมูลไหมเพื่อป้องกันตารางพัง ---
if not df_plan.is_empty() and df_stocktake_id.is_empty() and df_query.is_empty():
    branch_val = df_plan[0, "branch"]
    # --- จุดแก้ไขที่ 2: ปรับ Syntax การสร้าง DataFrame ให้ถูกต้อง (รวมเป็น dict เดียว) ---
    df_stocktake_id = pl.DataFrame({
        'stocktakeid': [stocktakeid],
        'bu': [bu],
        'stcode': [stcode],
        'cntdate': [cntdate],
        'branch': [branch_val],
        'status': ['C1']  # เพิ่มคอลัมน์ status ด้วยค่า default 'C1'
    })  # ดึงค่าแถวที่ 0 ของคอลัมน์ branch
    print(df_stocktake_id)
    df_stocktake_id.write_database('stocktake_id', sync_engine_test, if_table_exists='append')
    df3.write_database(table_name, sync_engine_test, if_table_exists='append')
    print(f"Data inserted into {table_name} : {len(df3)} rows successfully.")

elif not df_plan.is_empty() and not df_stocktake_id.is_empty() and not df_query.is_empty():
    with sync_engine_test.begin() as conn:
        conn.execute(text(f"DELETE FROM {table_name} WHERE stocktakeid = '{stocktakeid}'"))
    df3.write_database(table_name, sync_engine_test, if_table_exists='append')
    print(f"Existing data for stocktakeid {stocktakeid} deleted and new data inserted into {table_name} : {len(df3)} rows successfully.")

else:
    print(f"No data found in df_plan or stocktake_id already exists. No action taken.")
    exit()  # ออกจากฟังก์ชันหรือสคริปต์หากไม่มีข้อมูลใน df_plan

