import os
import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib
import shutil
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'


bu = 'CFR'
sheet_name = 'Summary'
table = 'missrate'
f_path = filepath / 'Shared' / 'Performance' / 'Missrate' / 'Missrate_approved'
t_path = filepath / 'Shared' / 'Performance' / 'Missrate' / 'Missrate_imported'
error_log_path = filepath / 'Shared' / 'Performance' / 'report' / 'error_log.csv'
timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

excel_files = [f for f in os.listdir(f_path) if f.endswith(('.xlsx', '.xls')) and os.path.getsize(os.path.join(f_path, f)) > 0]


# PostgreSQL connection details
engine = create_engine(db_connect.db_url_pstdb)
plan_query = f"SELECT stcode, cntdate, branch FROM planall2 WHERE bu = '{bu}'"
df_plan = pd.read_sql(plan_query, con=engine)

for file in excel_files:
    file_path = f_path / file
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, usecols='A:J', skiprows=6,
                           dtype={'Group': str, 'Department': str,'Barcode': str,
                                  'Name': str,'Count': float,'Add/Edit': float,'Scan Error': float,
                                  'PST/Store': str,'Remark (Root Cause)': str,'Note': str})

        file_parts = os.path.splitext(file)[0].split('_')
        if len(file_parts) == 4:
            cols1, cols2, cols3, cols4 = file_parts
        else:
            cols1, cols2, cols3, cols4 = None, None, None, None

        df['cols1'] = cols1
        df['bu'] = cols2
        df['stcode'] = cols3
        df['cntdate'] = cols4

        df['PST/Store'] = df['PST/Store'].str.upper()
        df['PST/Store'] = df['PST/Store'].replace(['AJIS', 'INNOVATION', 'SSD','PCS'], 'OUTSOURCE')
        
        #df['skutype'] = 'Credit'
        #df = df[df['STORE'] == cols3]
        df = df.drop(columns=['cols1'])
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'remark (root cause)': 'remark root cause'})
        
        join_plan = df.merge(df_plan[['stcode', 'cntdate','branch']], on=['stcode', 'cntdate'], how='left')
        df = join_plan
        df = df[df['branch'].notnull()]
        df = df.drop(columns=['branch'])
        
        del_stcode = df['stcode'].iloc[0]
        del_cntdate = df['cntdate'].iloc[0]
        
        del_query = f"DELETE FROM {table} WHERE stcode = '{del_stcode}' AND cntdate = '{del_cntdate}'"
        with engine.connect() as connection:
            connection.execute(text(del_query))
            print(f"✅ Deleted existing data for stcode: {del_stcode}, cntdate: {del_cntdate}")
        
        
        df_piece = pd.read_excel(file_path, sheet_name=sheet_name, usecols='L:M', skiprows=2,nrows=3,header=None)
        
        # Add constant columns
        df_piece['bu'] = cols2
        df_piece['stcode'] = cols3
        df_piece['cntdate'] = cols4
        
        # Extract values row-wise
        df_piece['all scan-pst'] = df_piece.iloc[0, 0]
        df_piece['qty weight-pst'] = df_piece.iloc[1, 0]
        df_piece['sku weright-pst'] = df_piece.iloc[2, 0]
        df_piece['all scan-outsource'] = df_piece.iloc[0, 1]
        df_piece['qty weight-outsource'] = df_piece.iloc[1, 1]
        df_piece['sku weright-outsource'] = df_piece.iloc[2, 1]

        # Drop original columns (optional)
        df_piece = df_piece.drop(columns=[11, 12])

        # Reset index to a single summary row
        df_piece = df_piece.iloc[[0]].reset_index(drop=True)
        
        # Add the new columns to the main dataframe and fill down their values
        df['all scan-pst'] = df_piece['all scan-pst'].iloc[0]
        df['qty weight-pst'] = df_piece['qty weight-pst'].iloc[0]
        df['sku weright-pst'] = df_piece['sku weright-pst'].iloc[0]
        df['all scan-outsource'] = df_piece['all scan-outsource'].iloc[0]
        df['qty weight-outsource'] = df_piece['qty weight-outsource'].iloc[0]
        df['sku weright-outsource'] = df_piece['sku weright-outsource'].iloc[0]
        
        # Remove duplicate rows based on all columns except index
        df = df.drop_duplicates()
        # Check if 'pst/store' or 'remark root cause' have NA values; if so, skip this file
        if df['pst/store'].isna().any() or df['remark root cause'].isna().any():
            print(f"❌ Skipping file {file} due to NA in 'pst/store' or 'remark root cause'")
            continue

        df.to_sql(table, engine, if_exists='append', index=False)
        print(f"✅ Data inserted into '{table}' at {timestamp}")
        shutil.move(file_path, os.path.join(t_path, file))
        print(f"✅ moved file: {file} → {t_path}")

    except Exception as e:
        print(f"Error processing {file}: {e}")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_df = pd.DataFrame([[file, str(e), timestamp]], columns=['filename', 'error', 'timestamp'])
        log_df.to_csv(error_log_path, mode='a', index=False, header=not error_log_path.exists())