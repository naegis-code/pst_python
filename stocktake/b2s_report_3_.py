import os
import pathlib
import xlwings as xw
import pandas as pd
from sqlalchemy import create_engine
import db_connect
from datetime import datetime, timedelta
import shutil

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

bu = 'b2s'
sheet_stk2 = 'detail'
column_stk2 = 'A:V'
sheet_sale = 'sale'
column_sale = 'A:I'


f_path = pathlib.Path.home() / 'Documents' / 'soh' / 'report3' / bu.upper()
t_path = pathlib.Path.home() / 'Documents' / 'soh' / 'Completed'

table_stk2 = 'stk'

date = '20250101'

# ✅ ใช้ glob เก็บเป็น Path objects ชัวร์
excel_files = [
    f for f in f_path.glob("*.xlsx")
    if f.stat().st_size > 0
] + [
    f for f in f_path.glob("*.xls")
    if f.stat().st_size > 0
]

print(f"{len(excel_files)} Files")

# get data from stk db3
c_db3 = create_engine(db_connect.db_url_pstdb3)
q_db3 = f"""
SELECT distinct
    store, cntdate,skutype, rpname,'recheck' as recheck
FROM {bu}_{table_stk2}_this_year
where rpname = 'STK2'
"""
df_db3 = pd.read_sql(q_db3, c_db3)

# get data from sale db3
q_sale_db3 = f"""
SELECT distinct
    mdstor as store,cntdate ,'recheck' as recheck
FROM {bu}_sale_this_year
"""
df_sale_db3 = pd.read_sql(q_sale_db3, c_db3)

# get data from plan db
plan_db = create_engine(db_connect.db_url_pstdb)
q_plan_db = f"""
SELECT 
    stcode as store, cntdate,branch
FROM planall2
where atype = '3F'
    and cntdate >= '{date}'
    and bu = '{bu.upper()}'
"""
df_plan_db = pd.read_sql(q_plan_db, plan_db)




app = xw.App(visible=False)

# ✅ Convert .xls → .xlsx
for xls_file in f_path.glob("*.xls"):
    xlsx_file = f_path / (xls_file.stem + ".xlsx")

    try:
        wb = app.books.open(str(xls_file))
        wb.save(str(xlsx_file))
        wb.close()
        xls_file.unlink()  # ลบไฟล์เก่า
        print(f"✅ แปลงไฟล์ และ 🗑️ ลบไฟล์ต้นฉบับ: {xls_file.name} → {xlsx_file.name}")
    except Exception as e:
        print(f"❌ Error แปลง {xls_file.name}: {e}")

# ✅ แก้ชื่อ sheet และบันทึกเป็น .xlsx
for excel_file in excel_files:
    try:
        wb = app.books.open(str(excel_file))
        changed = False

        for sheet in wb.sheets:
            old_name = sheet.name
            new_name = old_name.lower().replace(" ", "")
            if new_name != old_name:
                sheet.name = new_name
                changed = True
                print(f"✅ {excel_file.name}: '{old_name}' ➝ '{new_name}'")

        # Save เป็น .xlsx เสมอ
        new_file = excel_file.with_suffix(".xlsx")
        wb.save(str(new_file))
        wb.close()

        # ถ้าไฟล์เก่าเป็น .xls → ลบทิ้ง
        if excel_file.suffix.lower() == ".xls":
            excel_file.unlink()

        print(f"💾 Saved & replaced: {new_file.name} ({excel_files.index(excel_file)+1}/{len(excel_files)})")

    except Exception as e:
        print(f"❌ Error {excel_file.name}: {e}")

app.quit()

# ✅ refresh excel_files หลังแปลง .xls → .xlsx
excel_files = [
    f for f in f_path.glob("*.xlsx")
    if f.stat().st_size > 0
]


# ✅ Import STK2
for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_stk2 in xls.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet_stk2, usecols=column_stk2, dtype=str, engine="openpyxl")
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'STK2'
            df['sku'] = df['sku'].fillna('')

            df['skutype'] = df['countname'].apply(lambda x: 'Credit' if x[1:2] == 'B' else 'Consign')
            df = df[df['store'] == cols3]

            df = df.merge(df_plan_db[['store', 'cntdate','branch']], on=['store', 'cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(df_db3[['store','cntdate','skutype','rpname','recheck']],
                          on=['store','cntdate','skutype','rpname'], how='left')
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck'])

            if df.empty:
                xls.close()
                print(f"⚠️ {file.name} No new STK2 data to insert ({i}/{len(excel_files)})")
                continue

            df.to_sql(f'{bu}_{table_stk2}_this_year', c_db3, if_exists='append', index=False)

            xls.close()

            print(f"✅ STK2 inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")
        else:
            print(f"❌ STK2 sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error STK2 {file.name}: {e}")
    finally:
        if xls is not None:
            xls.close()



# ✅ SALE
for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_sale in xls.sheet_names:   # ✅ ใช้ sheet_var1
            df = pd.read_excel(
                file,
                sheet_name=sheet_sale,
                usecols=column_sale,
                dtype=str,
                engine="openpyxl"
            )
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['store'] = df['mdstor2'].copy()
            df = df[df['store'] == cols3]

            #MDSTOR2	STORE NAME	MDDEPT	DEPT NAME	MDSDPT	SDPT NAME	Credit	Consign	ผลรวมทั้งหมด
            column_name = {
                'mdstor2':'mdstor', 'store name':'mdstrn', 'mddept':'mddept', 'dept name':'mddptn','MDSDPT':'mdsdpt',
                'sdpt name':'mdsdpn', 'credit':'credit', 'consign':'consignment', 'ผลรวมทั้งหมด':'grand_total'
            }
            df.rename(columns=column_name, inplace=True)

            df = df.merge(df_plan_db[['store','cntdate','branch']], on=['store','cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(
                df_sale_db3[['store','cntdate','recheck']],
                on=['store','cntdate'],
                how='left'
            )
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck','store'])

            if df.empty:
                # close xls
                xls.close()
                # ✅ move file ไป Completed เสมอ
                shutil.move(str(file), str(t_path / file.name))
                print(f"⚠️ No new VAR1 data to insert in {file.name} ({i}/{len(excel_files)}).")
            else:
                df.to_sql(f'{bu}_sale_this_year', c_db3, if_exists='append', index=False)
                print(f"✅ SALE inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")

                # close xls
                xls.close()
                # ✅ move file ไป Completed เสมอ
                shutil.move(str(file), str(t_path / file.name))
        else:
            print(f"❌ SALE sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error SALE {file.name}: {e} ({i}/{len(excel_files)})")

    finally:
        print("All done for this file.")


