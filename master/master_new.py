from datetime import datetime
import os
import shutil
import pandas as pd
from sqlalchemy import create_engine, text


master = "D:\\Master.db"
engine = create_engine(f"sqlite:///{master}")

bu = "NEW"
stcode = "000"
cntdate = "20260903"

stocktakeid = f"{bu}{stcode}{cntdate}"
storecode = stcode
storename = ""
stock = 0
status = "A"

path = f"D:/Users/prthanap/Documents"
filename = "Item Master 26-09-03.xlsx"
pathfile = f"{path}/{filename}"

# 1. อ่านและจัดการข้อมูล Pandas DataFrame
df = pd.read_excel(pathfile,sheet_name=0,usecols="A:CJ", dtype=str)
df.columns.str.lower()
df = df[["fbarcode", "itemid", "iteamname", "directconsign", "fbarcodeunit","division","group","pcs_retail"]]
df = df[df["division"] == "Direct"]

print(df)
print(f"reading {pathfile} length: {len(df)} records")
'''
df = df.replace(to_replace=[r',', r'\r', r'\n'], value='', regex=True).apply(lambda col: col.str.strip())

# 2. เติม 0 ด้านหน้า (Left Pad) ให้ sku และ barcode เป็น 13 หลัก
df["sku"] = df["itemid"].str.zfill(13)
df["barcode"] = df["fbarcode"].str.zfill(13)


df_master0 = df.drop_duplicates(subset=["sku"], keep="first").copy()
df_master0["barcode"] = df_master0["sku"]
df_master1 = df.drop_duplicates(subset=["barcode"], keep="first").copy()

df_master = pd.concat([df_master0, df_master1])
df_master["sku"] = df_master["barcode"]
df_master["retailprice"] = df_master["unit_cost"]
df_master["stocktakeid"] = stocktakeid
df_master["storecode"] = storecode
df_master["storename"] = storename
df_master["stock"] = stock
df_master["status"] = status
df_master = df_master.rename(
    columns={
        "barcode": "barcodeibc",
        "unit": "packsize",
        "item_name": "productname",
        "unit_cost": "cost",
    }
)

print(df_master)


# 3. จัดการฐานข้อมูล SQLite
# VACUUM นอก transaction (ใช้ execution_options เพื่อ autocommit)
with engine.connect() as conn:
  conn.execution_options(isolation_level="AUTOCOMMIT").execute(text("VACUUM;"))

# Update, Delete และ Insert ข้อมูล
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

  df_master.to_sql(
      "pda_masters",
      con=conn,
      if_exists="append",
      index=False,
      method="multi",
      chunksize=1000,
  )
  

print(
    f"Delete and Reset ID completed and data inserted into pda_masters:"
    f" {len(df_master)}"
)

# 4. คืน Connection Pool ก่อนทำการ Copy ไฟล์
engine.dispose()

# 5. สร้างชื่อไฟล์ใหม่ตามฟอร์แมต {master}_{stocktakeid}_{yyyymmddhhmm}.db
current_time = datetime.now().strftime("%Y%m%d%H%M")
folder_path = os.path.dirname(master)

# จะได้ชื่อไฟล์ เช่น D:\Master_NEW000F20260903001_202609021837.db
new_master_path = os.path.join(
    folder_path, f"Master_{stocktakeid}_{current_time}.db"
)
df_master.to_excel(f"{path}/df_master_{current_time}.xlsx", index=False)

# ทำการ Copy ไฟล์
shutil.copy2(master, new_master_path)

print(f"Backup file created successfully at: {new_master_path}")
print("export completed")
'''