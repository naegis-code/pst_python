import os
import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib
import shutil
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from tqdm import tqdm
import xlwings as xw

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

bu = 'CFR'
sheet_name = 'STK'
table = 'cfr_stk_this_year'
f_path = filepath / 'Shared' / 'Performance' / 'report' / 'report3' / bu
t_path = filepath / 'Shared' / 'Performance' / 'report' / 'report_completed'
error_log_path = filepath / 'Shared' / 'Performance' / 'report' / 'error_log.csv'
timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')


excel_files = [f for f in os.listdir(f_path) if f.endswith(('.xlsx', '.xls')) and os.path.getsize(os.path.join(f_path, f)) > 0]


# PostgreSQL connection details
engine_plan = create_engine(db_connect.db_url_pstdb)
plan_query = f"SELECT stcode, cntdate, branch FROM planall2 WHERE bu = '{bu}'"
df_plan = pd.read_sql(plan_query, con=engine_plan)
#df_plan = df_plan.rename(columns={'stcode': 'store', 'cntdate': 'cntdate'})

engine_result = create_engine(db_connect.db_url_pstdb3)
result_query = f"select distinct stcode ,cntdate, 'recheck' as recheck from {table} where rpname = 'STK2'"
df_result = pd.read_sql(result_query, con=engine_result)

files = [f for f in os.listdir(f_path) if f.endswith(('.xlsx', '.xls')) and os.path.getsize(f_path / f) > 0]
for file in tqdm(excel_files,total=len(files), leave=False):
    file_path = f_path / file
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, usecols='A:Z', dtype={'DEPT':str, 'SUB_DEPT': str, 'SKU': str, 'BARCODE': str, 'PRODUCT_NAME': str, 'BRAND': str, 
                                                                                   'MODEL': str, 'QNT_COST': float, 'QNT_RETAIL': float,
                                                                                    'VARIANCE': float, 'VAR_COST': float, 'VAR_RETAIL': float, 'VAR_PERCENT' : float})


        file_parts = os.path.splitext(file)[0].split('_')
        if len(file_parts) == 5:
            cols1, cols2, cols3, cols4 ,cols5 = file_parts
        else:
            cols1, cols2, cols3, cols4 ,cols5 = None, None, None, None, None

        df['cols1'] = cols1
        df['cols2'] = cols2
        df['stcode'] = cols3
        df['cntdate'] = cols4
        df['rpname'] = 'STK2'
        df['cols5'] = cols5
        df['skutype'] = 'Credit'

        df['STOCK'] = pd.to_numeric(df['STOCK'], errors='coerce')
        df['QNT'] = pd.to_numeric(df['QNT'], errors='coerce')
        df = df.drop(df[(df['STOCK'] == 0) & (df['QNT'].isna())].index)
        df['QNT'] = df['QNT'].fillna(0)

        df = df.drop(columns=['cols1', 'cols2', 'cols5','SKU_TYPE','BARCODE1','BARCODE2','BARCODE3','BARCODE4','BARCODE5','BARCODE6','BARCODE7','BARCODE8','BARCODE9','BARCODE10'])
        df.columns = df.columns.str.lower()

        join_plan = df.merge(df_plan[['stcode', 'cntdate','branch']], on=['stcode', 'cntdate'], how='left')
        df = join_plan
        df = df[df['branch'].notnull()]
        df = df.drop(columns=['branch'])

        join_result = df.merge(df_result[['stcode', 'cntdate','recheck']], on=['stcode', 'cntdate'], how='left')
        df = join_result
        df = df[df['recheck'].isnull()]
        df = df.drop(columns=['recheck'])
        df = df[df['sku'].notnull()]

        if df.empty:
            tqdm.write(f"No valid data in {file}")
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_df = pd.DataFrame([[file, 'No valid data after filtering', timestamp]], columns=['filename', 'error', 'timestamp'])
            log_df.to_csv(error_log_path, mode='a', index=False, header=not error_log_path.exists())
        else:
            df.to_sql(table, engine_result, if_exists='append', index=False)
            tqdm.write(f"✅ Data inserted into '{table}' at {timestamp}")


    except Exception as e:
        tqdm.write(f"Error processing {file}: {e}")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_df = pd.DataFrame([[file, str(e), timestamp]], columns=['filename', 'error', 'timestamp'])
        log_df.to_csv(error_log_path, mode='a', index=False, header=not error_log_path.exists())