import pandas as pd
import pathlib
import os
import xlwings as xw
import shutil
from sqlalchemy import create_engine, text
import db_connect

# ========== PARAMETERS ==========

bu = 'PWB'
var1 = 'var1'
var2 = 'var2'
stk1 = 'stk1'
stk2 = 'stk2'
usecol_var = "A:AE"
usecol_stk = "A:AF"


# first step is to move files from source to destination
# Database connections
connect_db_plan = create_engine(db_connect.db_url_pstdb)
connect_db_result = create_engine(db_connect.db_url_pstdb3)

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

src_path = filepath / 'Shared' / 'Performance' / 'report' / 'rawdata' / bu.upper()
dst_path = userpath / 'Documents' / 'soh' / 'rawdata' / bu.upper()
cpt_path = userpath / 'Documents' / 'soh' / 'Completed'

excel_files = [f for f in os.listdir(src_path) if f.endswith(('.xlsx', '.xls'))]
print(f"📦 Found {len(excel_files)} Excel files to move")

if not excel_files:
    print("🚫 No Excel files found.")

def move():
    """Move and normalize files from source to destination"""

    # ========== Move files ==========
    for item in src_path.glob("*"):
        dest = dst_path / item.name
        if item.is_file():
            shutil.copy2(item, dest)
            item.unlink()
            print(f"✅ Moved file: {item.name}")
        elif item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
            shutil.rmtree(item)
            print(f"✅ Moved directory: {item.name}")

def normalize(): 
    app = xw.App(visible=False)
    app.display_alerts = False
    app.screen_updating = False

    try:
        # Convert .xls → .xlsx
        for f in dst_path.glob('*.xls'):
            path = pathlib.Path(f)
            try:
                wb = app.books.open(str(f))
                xlsx_path = f.with_suffix('.xlsx')
                wb.save(str(xlsx_path))
                wb.close()
                path.unlink()  # delete old .xls
                print(f"🧾 Converted: {path.name} → {xlsx_path.name}")
            except Exception as e:
                print(f"⚠️ Skip convert {path.name}: {e}")

        # Rename sheet names in .xlsx
        for f in dst_path.glob('*.xlsx'):
            try:
                wb = app.books.open(str(f))
                changed = False
                for idx, sheet in enumerate(wb.sheets):
                    old = sheet.name
                    new = "Sheet1" if idx == 0 else old.lower().replace(" ", "")
                    if old != new:
                        sheet.name = new
                        changed = True
                        print(f"🪶 {f.name}: '{old}' → '{new}'")
                    else:
                        print(f"🔍 {f.name}: '{old}' unchanged")

                if changed:
                    wb.save()
                wb.close()
            except Exception as e:
                print(f"❌ Error renaming sheets in {f.name}: {e}")

    finally:
        app.quit()

def standardize_runvar(bu,rpname,usecol):
    """Load plan and result data from the database"""
    df_plan = pd.read_sql(
        text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}'"),
        connect_db_plan
    )
    df_plan_add = pd.read_sql(
        text(f"SELECT bu, stcode, cntdate, branch FROM plan_add WHERE bu = '{bu.upper()}'"),
        connect_db_result
    )

    df_plan = pd.concat([df_plan, df_plan_add], ignore_index=True)

    df_result = pd.read_sql(
        text(f"""
            SELECT bu, stcode, cntdate, rpname, skutype AS prtype, 'check' AS recheck
            FROM report_checker 
            WHERE rpname = '{rpname.upper()}' AND bu = '{bu.upper()}'
        """),
        connect_db_result
    )

    files = [f for f in os.listdir(dst_path) if f.startswith(rpname.upper()) and f.endswith('.xlsx')]

    for index, file in enumerate(files, 1):

        try:
            file_path = dst_path / file
            df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl', usecols=usecol,dtype=str)
            file_parts = pathlib.Path(file).stem.split('_')
            cols1, cols2, cols3, cols4, _ = (file_parts + [None]*5)[:5]

            # Clean & setup columns
            df.columns = df.columns.str.lower()
            df = df[df['skcode'].fillna('') != ''].drop_duplicates()

            df['rpname'], df['bu'], df['stcode'], df['cntdate'] = cols1, cols2, cols3, cols4

            # Compare first 5 characters of cntnum with cols3
            if (df['cntnum'].str[:5] != cols3).any():
                print(f"⚠️ {file}: Some 'cntnum' values do not match the filename : {file} Content {df['cntnum'].iloc[0][:5]} != {cols3}.")
                continue

            # Join plan
            df = df.merge(df_plan, how='left', on=['bu', 'stcode', 'cntdate'])
            df = df[df['branch'].notna()].drop(columns=['branch'], errors='ignore')

            if df.empty:
                print(f"🚫 {rpname.upper()} No plan found for {file} ({index}/{len(files)})")
                continue

            # Join check result
            df = df.merge(df_result, how='left', on=['bu', 'stcode', 'cntdate', 'rpname', 'prtype'])
            df_na = df[df['recheck'].isna()]
            table_name = f"{bu.lower()}_{rpname[:3].lower()}_this_year"

            # Query for delete
            query_delete = text(f"""
                DELETE FROM {table_name}
                WHERE rpname = '{df['rpname'].iloc[0]}'
                AND cntnum = '{df['cntnum'].iloc[0]}'
                AND prtype = '{df['prtype'].iloc[0]}'
            """)

            if not df_na.empty:
                # Case 1: มีข้อมูลใหม่ → insert
                df_na = df_na.drop(columns=['recheck', 'stcode', 'cntdate', 'bu'], errors='ignore')
                df_na.to_sql(table_name, connect_db_result, if_exists='append', index=False)
                shutil.move(str(dst_path / file), str(cpt_path / file))
                print(f"✅ {rpname.upper()} inserted {file} ({len(df_na)} rows) ({index}/{len(files)})")
            else:
                # Case 2: ไม่มีข้อมูลใหม่ → delete ก่อนแล้วค่อย insert
                with connect_db_result.begin() as conn:
                    conn.execute(query_delete)
                    print(f"🗑️ Deleted old data for {df['rpname'].iloc[0]}, {df['cntnum'].iloc[0]}, {df['prtype'].iloc[0]}")
                df_clean = df.drop(columns=['recheck', 'stcode', 'cntdate', 'bu'], errors='ignore')
                df_clean.to_sql(table_name, connect_db_result, if_exists='append', index=False)
                shutil.move(str(dst_path / file), str(cpt_path / file))
                print(f"♻️ Reinserted {file} ({len(df_clean)} rows) ({index}/{len(files)})")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

def standardize_runstk(bu,rpname,usecol):
    """Load plan and result data from the database"""
    df_plan = pd.read_sql(
        text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}'"),
        connect_db_plan
    )
    df_plan_add = pd.read_sql(
        text(f"SELECT bu, stcode, cntdate, branch FROM plan_add WHERE bu = '{bu.upper()}'"),
        connect_db_result
    )

    df_plan = pd.concat([df_plan, df_plan_add], ignore_index=True)
    df_result = pd.read_sql(
        text(f"""
            SELECT bu, stcode, cntdate, rpname, skutype, 'check' AS recheck
            FROM report_checker 
            WHERE rpname = '{rpname.upper()}' AND bu = '{bu.upper()}'
        """),
        connect_db_result
    )
    df_sku_type = pd.read_sql(
        text(f"""SELECT distinct cntnum,prtype as skutype FROM {bu.lower()}_var_this_year"""),
        connect_db_result
    )

    files = [f for f in os.listdir(dst_path) if f.startswith(rpname.upper()) and f.endswith('.xlsx')]

    for index, file in enumerate(files, 1):

        try:
            file_path = dst_path / file
            df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl', usecols=usecol,dtype=str)
            file_parts = pathlib.Path(file).stem.split('_')
            cols1, cols2, cols3, cols4, _ = (file_parts + [None]*5)[:5]

            # Clean & setup columns
            df.columns = df.columns.str.lower()

            df = df[df['sku'].fillna('') != ''].drop_duplicates()

            df = df.merge(df_sku_type, how='left', left_on='cntnum', right_on='cntnum')
            df['skutype'] = df['skutype'].fillna('Unknown')

            if (df['skutype'] == 'Unknown').all():
                print(f"⚠️ {file}: All SKUs have unknown types.")
                continue
            

            df['rpname'], df['bu'], df['stcode'], = cols1, cols2, cols3


            if (df['stmerch'] != cols3).any():
                print(f"⚠️ {file}: Some 'stcode' values do not match the filename : {file} Content {df['stmerch'].iloc[0]} != {cols3}.")
                continue

            # Join plan
            df = df.merge(df_plan, how='left', on=['bu', 'stcode', 'cntdate'])
            df = df[df['branch'].notna()].drop(columns=['branch'], errors='ignore')

            if df.empty:
                print(f"🚫 {rpname.upper()} No plan found for {file} ({index}/{len(files)})")
                continue

            # Join check result
            df = df.merge(df_result, how='left', on=['bu', 'stcode', 'cntdate', 'rpname', 'skutype'])
            df_na = df[df['recheck'].isna()]
            table_name = f"{bu.lower()}_{rpname[:3].lower()}_this_year"

            # Query for delete
            query_delete = text(f"""
                DELETE FROM {table_name}
                WHERE rpname = '{df['rpname'].iloc[0]}'
                AND cntnum = '{df['cntnum'].iloc[0]}'
            """)

            if not df_na.empty:
                # Case 1: มีข้อมูลใหม่ → insert
                df_na = df_na.drop(columns=['recheck', 'stcode', 'bu'], errors='ignore')
                df_na.to_sql(table_name, connect_db_result, if_exists='append', index=False)
                shutil.move(str(dst_path / file), str(cpt_path / file))
                print(f"✅ {rpname.upper()} inserted {file} ({len(df_na)} rows) ({index}/{len(files)})")
            else:
                # Case 2: ไม่มีข้อมูลใหม่ → delete ก่อนแล้วค่อย insert
                with connect_db_result.begin() as conn:
                    conn.execute(query_delete)
                    print(f"🗑️ Deleted old data for {df['rpname'].iloc[0]}, {df['cntnum'].iloc[0]}")
                df_clean = df.drop(columns=['recheck', 'stcode', 'bu'], errors='ignore')
                df_clean.to_sql(table_name, connect_db_result, if_exists='append', index=False)
                shutil.move(str(dst_path / file), str(cpt_path / file))
                print(f"♻️ Reinserted {file} ({len(df_clean)} rows) ({index}/{len(files)})")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

move()
normalize()
standardize_runvar(bu, var1, usecol_var)
standardize_runvar(bu, var2, usecol_var)
standardize_runstk(bu, stk1, usecol_stk)
standardize_runstk(bu, stk2, usecol_stk)
