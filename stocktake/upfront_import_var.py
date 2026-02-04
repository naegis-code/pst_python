import os
import pathlib
import xlwings as xw
import pandas as pd
from sqlalchemy import create_engine
import db_connect
from datetime import datetime, timedelta

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

def upfront_import_var(bu,date,table,column):
    # Set paths for different business units
    path = user_path / 'Desktop' / 'New folder (3)' / bu.upper()

    column_var = column

    # ✅ ใช้ glob เก็บเป็น Path objects ชัวร์
    excel_files = [
        f for f in path.glob("*.xlsx")
        if f.stat().st_size > 0
    ] + [
        f for f in path.glob("*.xls")
        if f.stat().st_size > 0
    ]

    app = xw.App(visible=False)

    # ✅ Convert .xls → .xlsx
    if path.exists() and path.is_dir():
        for xls_file in path.glob("*.xls"):
            xlsx_file = path / (xls_file.stem + ".xlsx")

            try:
                wb = app.books.open(str(xls_file))
                wb.save(str(xlsx_file))
                wb.close()
                xls_file.unlink()  # ลบไฟล์เก่า
                print(f"✅ แปลงไฟล์ และ 🗑️ ลบไฟล์ต้นฉบับ: {xls_file.name} → {xlsx_file.name}")
            except Exception as e:
                print(f"❌ Error แปลง {xls_file.name}: {e}")


    # ✅ แก้ชื่อ sheet และบันทึกเป็น .xlsx
    if path.exists() and path.is_dir():
        for excel_file in path.glob("*.xlsx"):
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

                print(f"💾 Saved & replaced: {new_file.name} ({list(path.glob('*.xlsx')).index(excel_file)+1}/{len(list(path.glob('*.xlsx')))})")

            except Exception as e:
                print(f"❌ Error {excel_file.name}: {e}")

    app.quit()

    # ✅ Process each Excel file

    for i, file in enumerate(excel_files, start=1):
        xls = None
        try:
            xls = pd.ExcelFile(file, engine="openpyxl")

            df = pd.read_excel(
                file,
                sheet_name='sheet1',
                usecols=column_var,
                dtype=str,
                engine="openpyxl"
            )
            df.columns = df.columns.str.lower()

            file_parts = file.stem.split('_')
            # rpname,bu,store,cntnum,cntdate = file_parts
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

            df['rpname'] = cols1
            df['skcode'] = df['skcode'].fillna('')

            # check df with filename parts
            df = df[df['cntnum'].str[:5] == cols3]
            cols2 == bu.upper()

            df['store'] = df['cntnum'].str[:5]

            # Extract date part (positions 6–11 → '151225')
            df['cntdate'] = df['cntnum'].str[6:12]

            # Convert ddmmyy → yyyymmdd
            df['cntdate'] = pd.to_datetime(df['cntdate'], format='%d%m%y').dt.strftime('%Y%m%d')

            # Clean and trim all string columns in the dataframe, replace empty strings with NaN, and remove multiple or trailing newlines
            df = df.applymap(
                lambda x: x.strip().replace('\n', ' ').replace('\r', ' ') if isinstance(x, str) else x
            ).replace('', pd.NA)

            
            # check df with database
            c_db = create_engine(db_connect.db_url_pstdb)
            c_db3 = create_engine(db_connect.db_url_pstdb3)

            q_plan_db = f"""
                SELECT 
                    stcode as store, cntdate,branch
                FROM planall2
                where atype = '3F'
                    and cntdate >= '{date}'
                    and bu = '{bu.upper()}'
                """
            
            q_db3 = f"""
            SELECT distinct
                substring(cntnum, 1, 5) as store,
                concat('20', "substring"(cntnum, 11, 2), substring(cntnum, 9, 2), substring(cntnum, 7, 2)) AS cntdate,
                prtype , rpname,'recheck' as recheck
            FROM {bu}_{table}_this_year
            where rpname = 'VAR1'
            """
            
            df_plan_db = pd.read_sql(q_plan_db, c_db)
            df_db3_var = pd.read_sql(q_db3, c_db3)


            df = df.merge(df_plan_db[['store','cntdate','branch']], on=['store','cntdate'], how='left')
            df = df[df['branch'].notna()]

            df = df.merge(
                df_db3_var[['store','cntdate','prtype','rpname','recheck']],
                on=['store','cntdate','prtype','rpname'],
                how='left'
            )

            df = df[df['recheck'].isna()].drop(columns=['branch','recheck','store','cntdate'])
            df = df.drop_duplicates().reset_index(drop=True)


            if df.empty:
                # close xls
                xls.close()
                print(f"⚠️ No new VAR1 data to insert in {file.name} ({i}/{len(excel_files)}).")
            else:
                df.to_sql(f'{bu}_{table}_this_year', c_db3, if_exists='append', index=False)

                print(f"✅ VAR1 inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")

                # close xls
                xls.close()


        except Exception as e:
            print(f"❌ Error VAR {file.name}: {e} ({i}/{len(excel_files)})")

        finally:
            print("All done for this file.")

def jda_import_stk(bu, date, table, column):

    user_path = pathlib.Path.home()

    path = user_path / 'Documents' / 'soh' / 'rawdata' / bu.upper()

    for file in path.glob("*"):
        print(f"Found file: {file.name}")

    # Start xlwings app for conversions
    app = xw.App(visible=False)
    # Convert .xls to .xlsx
    if path.exists() and path.is_dir():
        for xls_file in path.glob("*.xls"):
            xlsx_file = path / (xls_file.stem + ".xlsx")
            try:
                wb = app.books.open(str(xls_file))
                wb.save(str(xlsx_file))
                wb.close()
                xls_file.unlink()
                print(f"✅ Converted & deleted: {xls_file.name} → {xlsx_file.name}")
            except Exception as e:
                print(f"❌ Error converting {xls_file.name}: {e}")

    # Rename first sheet to "sheet1" for all .xlsx files
    if path.exists() and path.is_dir():
        for excel_file in path.glob("*.xlsx"):
            try:
                wb = app.books.open(str(excel_file))
                first_sheet = wb.sheets[0]
                if first_sheet.name.lower() != "sheet1":
                    first_sheet.name = "sheet1"
                    print(f"✅ {excel_file.name}: First sheet renamed to 'sheet1'")
                wb.save(str(excel_file))
                wb.close()
            except Exception as e:
                print(f"❌ Error renaming sheet {excel_file.name}: {e}")

    app.quit()

    # Gather only .xlsx files with size > 0 after conversion
    excel_files = [f for f in path.glob("*.xlsx") if f.stat().st_size > 0]

    print(f"Total Excel files to process: {len(excel_files)}")

    column_mapping = {
        'dept': 'dpt', 'sdept': 'sdpt', 'class': 'cls', 'sclass': 'scls',
        'sku_desc': 'sku_des', 'brndname': 'brnnam', 'brnddesc': 'brandid',
        'vend_name': 'vend_nam', 'color': 'coldsc', 'size': 'sizdsc',
        'stock_oh': 'soh',
    }

    # Database connections
    c_db = create_engine(db_connect.db_url_pstdb)
    c_db3 = create_engine(db_connect.db_url_pstdb3)

    for i, file in enumerate(excel_files, 1):
        try:
            # Read Excel
            print(f'Processing file {i}/{len(excel_files)}: {file.name}')
            df = pd.read_excel(
                file,
                sheet_name='sheet1',
                usecols=column,
                dtype=str)
            
            df.columns = df.columns.str.lower()


            file_parts = file.stem.split('_')
            cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]
            df['rpname'] = cols1
            df['sku'] = df['sku'].fillna('')

            # Filter by store
            if 'store' in df.columns and cols3:
                df = df[df['store'] == cols3]

            # columns validations
            df['cntdate'] = cols4
            if 'countname' in df.columns:
                df['skutype'] = df['countname'].apply(lambda x: 'Credit' if isinstance(x, str) and len(x) > 1 and x[1] == 'B' else 'Consign')
            
            #print(f"Initial rows in {file.name}: {len(df)}")

            # DB queries
            q_plan_db = f"""
                SELECT stcode as store, cntdate, branch
                FROM planall2
                WHERE atype = '3F' AND cntdate >= '{date}' AND bu = '{bu.upper()}'
            """
            q_db3 = f"""
                SELECT DISTINCT store, cntdate, skutype, rpname, 'check' AS recheck
                FROM {bu}_{table}_this_year
            """

            df_plan_db = pd.read_sql(q_plan_db, c_db)
            df_db3_var = pd.read_sql(q_db3, c_db3)

            df = df.merge(df_plan_db[['store','cntdate','branch']], on=['store','cntdate'], how='left')
            df = df[df['branch'].notna()]
            #print(f"After merging plan DB: {len(df)} rows")
            df = df.merge(
                df_db3_var[['store','cntdate','skutype','rpname','recheck']],
                on=['store','cntdate','skutype','rpname'],
                how='left'
            )
            print(f"After merging DB3: {len(df)} rows")
            print(df.head())
            df = df[df['recheck'].isna()]#.drop(columns=['branch','recheck'])
            print(f"After filtering recheck: {len(df)} rows")
            print(df.head())
            #df.drop_duplicates()
            #print(f"After dropping recheck and duplicates: {len(df)} rows")

            # Uncomment to enable SQL append
            '''
            if df.empty:
                print(f"⚠️ No new {table} data to insert in {file.name} ({i}/{len(excel_files)}).")
            else:
                df.to_sql(f'{bu}_{table}_this_year', c_db3, if_exists='append', index=False)
                print(f"✅ {table} inserted {file.name} ({len(df)} rows) ({i}/{len(excel_files)})")
            '''
        except Exception as e:
            print(f"❌ Error {table} {file.name}: {e} ({i}/{len(excel_files)})")
        finally:
            # xls (pd.ExcelFile) doesn't need explicit close unless you use openpyxl directly.
            print("All done for this file.")

# Example usage
jda_import_stk('ssp','20250101','stk','A:X')