# %% import libraries
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

bu = 'pwb'
sheet_stk1_credit = 'detailsku(first-credit)' #Detail SKU (First-Credit)
sheet_var1_credit = 'variancelocation(credit)' #Variance Location(Credit)
sheet_stk1_consign = 'detailsku(first-consign)' #Detail SKU (First-Consign)
sheet_var1_consign = 'variancelocation(consign)' #Variance Location(Consign)
column_stk1 = 'A:AF'
column_var1 = 'A:AB'

copy_f_path = filepath / 'Shared' / 'Performance' / 'report' / 'report1' / bu.upper()
#t_path = filepath / 'Shared' / 'Performance' / 'report' / 'report_completed'

f_path = pathlib.Path.home() / 'Documents' / 'soh' / 'report1' / bu.upper()
t_path = pathlib.Path.home() / 'Documents' / 'soh' / 'Completed'

# ✅ Move with overwrite (like dirs_exist_ok=True)
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
    stmerch as store, cntdate,skutype, rpname,'recheck' as recheck
FROM {bu}_{table_stk1}_this_year
where rpname = 'STK1'
"""
df_db3 = pd.read_sql(q_db3, c_db3)

# get data from var db3
q_db3 = f"""
SELECT distinct
    substring(cntnum, 1, 5) as store,
    concat('20', "substring"(cntnum, 11, 2), substring(cntnum, 9, 2), substring(cntnum, 7, 2)) AS cntdate,
    prtype , rpname,'recheck' as recheck
FROM {bu}_{table_var1}_this_year
where rpname = 'VAR1'
"""
df_db3_var1 = pd.read_sql(q_db3, c_db3)

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

# get cntnum & prtype from var_db3
q_prtype_db3 = f"""select distinct
    cntnum, prtype
from {bu}_{table_var1}_this_year
where rpname = 'VAR1'
"""
df_prtype_db3 = pd.read_sql(q_prtype_db3, c_db3)


# %% Rename sheet and convert .xls to .xlsx
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

excel_files = [
    f for f in f_path.glob("*.xlsx")
    if f.stat().st_size > 0
]

# %% Import VAR1 Credit

for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_var1_credit in xls.sheet_names:   # ✅ ใช้ sheet_var1
            df = pd.read_excel(
                file,
                sheet_name=sheet_var1_credit,
                usecols=column_var1,
                dtype=str,
                engine="openpyxl"
            )
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'VAR1'
            df['skcode'] = df['skcode'].fillna('')

            df = df[df['cntnum'].str[:5] == cols3]

            df['store'] = df['cntnum'].str[:5]

            # Extract date part (positions 6–11 → '151225')
            df['cntdate'] = df['cntnum'].str[6:12]

            # Convert ddmmyy → yyyymmdd
            df['cntdate'] = pd.to_datetime(df['cntdate'], format='%d%m%y').dt.strftime('%Y%m%d')

            df = df.merge(df_plan_db[['store','cntdate','branch']], on=['store','cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(
                df_db3_var1[['store','cntdate','prtype','rpname','recheck']],
                on=['store','cntdate','prtype','rpname'],
                how='left'
            )
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck','store','cntdate'])

            if df.empty:
                # close xls
                xls.close()
                print(f"⚠️ No new VAR1 data to insert in {file.name} ({i}/{len(excel_files)}).")
            else:
                df.to_sql(f'{bu}_{table_var1}_this_year', c_db3, if_exists='append', index=False)
                print(f"✅ VAR1 inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")

                # close xls
                xls.close()

        else:
            print(f"❌ VAR1 sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error VAR1 {file.name}: {e} ({i}/{len(excel_files)})")

    finally:
        print("All done for this file.")

# %% Import VAR1 Consign

for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_var1_credit in xls.sheet_names:   # ✅ ใช้ sheet_var1
            df = pd.read_excel(
                file,
                sheet_name=sheet_var1_consign,
                usecols=column_var1,
                dtype=str,
                engine="openpyxl"
            )
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'VAR1'
            df['skcode'] = df['skcode'].fillna('')

            df = df[df['cntnum'].str[:5] == cols3]

            df['store'] = df['cntnum'].str[:5]

            # Extract date part (positions 6–11 → '151225')
            df['cntdate'] = df['cntnum'].str[6:12]

            # Convert ddmmyy → yyyymmdd
            df['cntdate'] = pd.to_datetime(df['cntdate'], format='%d%m%y').dt.strftime('%Y%m%d')

            df = df.merge(df_plan_db[['store','cntdate','branch']], on=['store','cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(
                df_db3_var1[['store','cntdate','prtype','rpname','recheck']],
                on=['store','cntdate','prtype','rpname'],
                how='left'
            )
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck','store','cntdate'])

            if df.empty:
                # close xls
                xls.close()
                print(f"⚠️ No new VAR1 data to insert in {file.name} ({i}/{len(excel_files)}).")
            else:
                df.to_sql(f'{bu}_{table_var1}_this_year', c_db3, if_exists='append', index=False)
                print(f"✅ VAR1 inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")

                # close xls
                xls.close()

        else:
            print(f"❌ VAR1 sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error VAR1 {file.name}: {e} ({i}/{len(excel_files)})")

    finally:
        print("All done for this file.")
# %%✅ Import STK1 Credit

# get data from var db3
q_db3 = f"""
SELECT distinct
    substring(cntnum, 1, 5) as store,
    concat('20', "substring"(cntnum, 11, 2), substring(cntnum, 9, 2), substring(cntnum, 7, 2)) AS cntdate,
    prtype , rpname,'recheck' as recheck
FROM {bu}_{table_var1}_this_year
where rpname = 'VAR1'
"""
df_db3_var1 = pd.read_sql(q_db3, c_db3)

for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_stk1_credit in xls.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet_stk1_credit, usecols=column_stk1, dtype=str, engine="openpyxl")
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'STK1'
            df['sku'] = df['sku'].fillna('')

            df['skutype'] = 'Credit'
            df['store'] = df['stmerch'].copy()

            df = df[df['store'] == cols3]

            df = df.merge(df_plan_db[['store', 'cntdate','branch']], on=['store', 'cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(df_prtype_db3, on='cntnum', how='left')
            df['prtype'] = df['prtype'].fillna('')
            df['prtype'].rename('skutype')

            df = df.merge(df_db3[['store','cntdate','skutype','rpname','recheck']],
                          on=['store','cntdate','skutype','rpname'], how='left')
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck','store','prtype'])

            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].str.strip()
                
            if df.empty:
                xls.close()
                print(f"⚠️ {file.name} No new STK1_Credit data to insert ({i}/{len(excel_files)})")
                continue

            df.to_sql(f'{bu}_{table_stk1}_this_year', c_db3, if_exists='append', index=False)

            xls.close()

            print(f"✅ STK1_Credit inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")
        else:
            print(f"❌ STK1_Credit sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error STK1_Credit {file.name} ({i}/{len(excel_files)}): {e}")
    finally:
        if xls is not None:
            xls.close()

# %%✅ Import STK1 Consign
for i, file in enumerate(excel_files, start=1):
    xls = None
    try:
        xls = pd.ExcelFile(file, engine="openpyxl")
        if sheet_stk1_credit in xls.sheet_names:
            df = pd.read_excel(file, sheet_name=sheet_stk1_credit, usecols=column_stk1, dtype=str, engine="openpyxl")
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['cntdate'] = cols4
            df['rpname'] = 'STK1'
            df['sku'] = df['sku'].fillna('')

            df['skutype'] = 'Consign'
            df['store'] = df['stmerch'].copy()

            df = df[df['store'] == cols3]

            df = df.merge(df_plan_db[['store', 'cntdate','branch']], on=['store', 'cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(df_prtype_db3, on='cntnum', how='left')
            df['prtype'] = df['prtype'].fillna('')
            df['prtype'].rename('skutype')

            df = df.merge(df_db3[['store','cntdate','skutype','rpname','recheck']],
                          on=['store','cntdate','skutype','rpname'], how='left')
            df = df[df['recheck'].isna()].drop(columns=['branch','recheck','store','prtype'])

            # Trim whitespace from all string columns
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].str.strip()

            if df.empty:
                xls.close()
                shutil.move(str(file), str(t_path / file.name))
                print(f"⚠️ {file.name} No new STK1 data to insert ({i}/{len(excel_files)})")
                continue

            df.to_sql(f'{bu}_{table_stk1}_this_year', c_db3, if_exists='append', index=False)

            xls.close()

            shutil.move(str(file), str(t_path / file.name))

            print(f"✅ STK1_Consign inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")
        else:
            print(f"❌ STK1_Consign sheet not found in {file.name} ({i}/{len(excel_files)})")
    except Exception as e:
        print(f"❌ Error STK1_Consign {file.name} ({i}/{len(excel_files)}): {e}")
    finally:
        if xls is not None:
            xls.close()