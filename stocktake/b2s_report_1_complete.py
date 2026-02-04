# ## li+paramiter

# %%
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
sheet_stk1 = 'sumvariance'
sheet_var1 = 'detailskuday0'
column_stk1 = 'A:V'
column_var1 = 'A:V'
copy_f_path = filepath / 'Shared' / 'Performance' / 'report' / 'report1' / bu
#t_path = filepath / 'Shared' / 'Performance' / 'report' / 'report_completed'

f_path = pathlib.Path.home() / 'Documents' / 'soh' / 'report1' / bu.upper()
t_path = pathlib.Path.home() / 'Documents' / 'soh' / 'Completed'

# Copy all files from copy_f_path to f_path, then remove the originals
for item in copy_f_path.glob("*"):
    dest = f_path / item.name
    if item.is_file():
        shutil.copy2(item, dest)
        item.unlink()
        print(f"Copied and removed file: {item.name}")
    elif item.is_dir():
        shutil.copytree(item, dest, dirs_exist_ok=True)
        shutil.rmtree(item)
        print(f"Copied and removed directory: {item.name}")

table_stk1 = 'stk'
table_var1 = 'var'
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
FROM {bu}_{table_stk1}_this_year
where rpname = 'STK1'
"""
df_db3 = pd.read_sql(q_db3, c_db3)

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

# get data from var db3
q_db3 = f"""
SELECT distinct
    store, cntdate,skutype, rpname,'recheck' as recheck
FROM {bu}_{table_var1}_this_year
where rpname = 'VAR1'
"""
df_db3_var1 = pd.read_sql(q_db3, c_db3)

# %% [markdown]
# ## rename sheet

# %%
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

# %% [markdown]
# ## import STK1

# %%
# ✅ Import STK1
for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_stk1 in xls.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet_stk1, usecols=column_stk1, dtype=str, engine="openpyxl")
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'STK1'
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
                print(f"⚠️ {file.name} No new STK1 data to insert ({i}/{len(excel_files)})")
                continue

            df.to_sql(f'{bu}_{table_stk1}_this_year', c_db3, if_exists='append', index=False)

            xls.close()

            print(f"✅ STK1 inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")
        else:
            print(f"❌ STK1 sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error STK1 {file.name}: {e}")
    finally:
        if xls is not None:
            xls.close()


# %% [markdown]
# ## import VAR1

# %%
# ✅ Import VAR1
for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_var1 in xls.sheet_names:   # ✅ ใช้ sheet_var1
            df = pd.read_excel(
                file,
                sheet_name=sheet_var1,
                usecols=column_var1,
                dtype=str,
                skiprows=1,
                engine="openpyxl"
            )
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'VAR1'
            df['sku'] = df['sku'].fillna('')

            df['skutype'] = df['countname'].apply(lambda x: 'Credit' if x[1:2] == 'B' else 'Consign')
            df = df[df['store'] == cols3]

            df = df.merge(df_plan_db[['store','cntdate','branch']], on=['store','cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(
                df_db3_var1[['store','cntdate','skutype','rpname','recheck']],
                on=['store','cntdate','skutype','rpname'],
                how='left'
            )
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck'])

            if df.empty:
                # close xls
                xls.close()
                # ✅ move file ไป Completed เสมอ
                shutil.move(str(file), str(t_path / file.name))
                print(f"⚠️ No new VAR1 data to insert in {file.name} ({i}/{len(excel_files)}).")
            else:
                df.to_sql(f'{bu}_{table_var1}_this_year', c_db3, if_exists='append', index=False)
                print(f"✅ VAR1 inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")

                # close xls
                xls.close()
                # ✅ move file ไป Completed เสมอ
                shutil.move(str(file), str(t_path / file.name))
        else:
            print(f"❌ VAR1 sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error VAR1 {file.name}: {e} ({i}/{len(excel_files)})")

    finally:
        print("All done for this file.")


