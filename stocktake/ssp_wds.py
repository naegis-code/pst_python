import pandas as pd
from sqlalchemy import create_engine
import pathlib
import db_connect
from datetime import datetime, timedelta
import os
from openpyxl import load_workbook
import shutil
import pathlib

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

f_path = filepath / 'Shared' / 'Performance' / 'report' / 'report3' / 'SSP' / 'Y2026 WDS from mail'

# Set parameters
bu = 'ssp'
date = '20260101'

table_stk = 'stk'
table_var = 'var'

rpname_stk = 'STK2'
rpname_var = 'VAR2'

sheet_name_stk = 'ข้อมูลตรวจนับ'
sheet_name_var = 'Type F'

column_stk = 'B:Q'
column_var = 'A:Q'

#connect to database
connect_db3 = create_engine(db_connect.db_url_pstdb3)
connect_db = create_engine(db_connect.db_url_pstdb)


# ✅ สร้างเป็น Path object เลย
excel_files = [
    f_path / f for f in os.listdir(f_path)
    if f.lower().endswith(('.xls', '.xlsx', '.xlsm')) and os.path.getsize(f_path / f) > 0
]

print(f"{len(excel_files)} Files")


# get data from stk db3
stk2_db3 = f"""
SELECT distinct
    store, cntdate,skutype, rpname,'recheck' as recheck
FROM {bu}_{table_stk}_this_year
where rpname = '{rpname_stk}'
"""
stk2_db3 = pd.read_sql(stk2_db3, connect_db3)

# get data from plan db
plan_db = f"""
SELECT 
    stcode as store, cntdate,branch
FROM planall2
where atype = '3F'
    and cntdate >= '{date}'
    and bu = '{bu.upper()}'
"""
plan_db = pd.read_sql(plan_db, connect_db)



for file in excel_files:
    file_path = os.path.join(f_path, file)
    try:
        xls = pd.ExcelFile(file_path)
        if sheet_name_stk in xls.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name_stk, usecols=column_stk, dtype=str,skiprows=5)

            file_parts = os.path.splitext(file)[0].split('_')
            if len(file_parts) == 3:
                cols1, cols2, cols3 = file_parts
            else:
                cols1, cols2, cols3 = None, None, None

            column_mapping = {
                'CUS_CODE': 'countname',
                'DP_CODE': 'dpt',
                'PR_CODE': 'sku',
                'PR_NAME': 'sku_des',
                'PR_BRAND': 'brnnam',
                'PR_MODEL': 'catalogue',
                'COLOR': 'coldsc',
                'PR_SIZE': 'sizdsc',
                'Unit Cost': 'cost',
                'Unit Retail': 'retail',
                'Quantity': 'soh',
                'Physical Count': 'qty_count',
                'Sum Unit Retail': 'phycnt_rtl',
                'Sum Unit Cost': 'physcnt_cst'
            }

            df = df.rename(columns=column_mapping)

            df.columns = df.columns.str.lower()
            
            
            for col in ['cost', 'retail', 'soh', 'qty_count']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(3)
            

            df['qty_var'] = df['qty_count'] - df['soh']

            df['extrtl_var'] = df['qty_var'] * df['retail']
            df['extcst_var'] = df['qty_var'] * df['cost']

            # เพิ่มคอลัมน์ใหม่
            df['store'] = 'W' + cols1[-6:]
            df['cntdate'] = cols2.replace('-', '')
            df['rpname'] = rpname_stk

            # ถ้า COUNTNAME ไม่มีค่า (NaN) ให้ fill ด้วย '' เพื่อป้องกัน error
            df['sku'] = df['sku'].fillna('')

            # เพิ่มคอลัมน์ ibc และ sbc โดยคัดลอกค่าจาก sku
            df['ibc'] = df['sku']
            df['sbc'] = df['sku']

            # สร้างคอลัมน์ skutype
            df['skutype'] = 'Credit'

            # join plan db
            df = df.merge(plan_db[['store', 'cntdate','branch']],
                          on=['store', 'cntdate'],
                          how='left')
            # keep only rows with branch **not null**
            df = df[df['branch'].notna()]

            # join stk db3
            df = df.merge(stk2_db3[['store', 'cntdate', 'skutype', 'rpname','recheck']],
                          on=['store', 'cntdate', 'skutype', 'rpname'],
                          how='left')
            # keep only rows with recheck **is null**
            df = df[df['recheck'].isna()]

            df = df.drop(columns=['branch', 'recheck','gp%','amount','sum cost','amount - gp%'])

            df.to_sql(f'{bu}_{table_stk}_this_year', connect_db3, if_exists='append', index=False)

            xls.close()
            print(f"✅Processed & inserted {file} with {len(df)} rows to {bu}_{table_stk}_this_year ({excel_files.index(file)+1}/{len(excel_files)})")
            print(df.head(5))
        else:
            print(f"❌sheet not found in {file}")
    except Exception as e:
        print(f"❌Error processing {file}: {e}")