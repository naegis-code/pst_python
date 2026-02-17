import pandas as pd
import pathlib
from datetime import datetime
from sqlalchemy import create_engine,text
import db_connect

db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

bu = 'b2s'

sheet_reconcile = 'Reconcile'
usecols_reconcile = 'A:W'
skiprows_reconcile = 0
table_reconcile = 'b2s_reconcile_this_year'

sheet_block = 'Block'
usecols_block = 'A:W'
skiprows_block = 1
table_block = 'b2s_block_this_year'

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path = filepath / 'Shared' / 'Performance' / 'report' / 'report3' / bu.upper()

excel_files = [
    f for f in path.glob("*.xlsx")
    if f.stat().st_size > 0
] + [
    f for f in path.glob("*.xls")
    if f.stat().st_size > 0
]

def process(file, sheet_name, usecols, skiprows):
    try:
        file_parts = file.stem.split('_')
        cols1, cols2, cols3, cols4, cols5 = (file_parts + [None]*5)[:5]

        df = pd.read_excel(file, sheet_name=sheet_name, usecols=usecols, skiprows=skiprows, dtype=str)

        df.columns = df.columns.str.lower()

        df['bu'] = cols2
        df['stcode'] = cols3
        df['cntdate'] = cols4
        df['skutype'] = df['countname'].apply(lambda x: 'Credit' if x[1:2] == 'B' else 'Consign')
        df['rpname'] = sheet_name
        df = df[df['sku'].notna() & (df['sku'].str.strip() != '')]
        if 'เหตุผล' in df.columns:
            df = df.rename(columns={'เหตุผล': 'remark'})
        else:
            df['remark'] = None

        query_plan = text(f"""SELECT 1
            FROM planall2
            WHERE bu = '{cols2}' 
                AND stcode = '{cols3}' 
                AND cntdate = '{cols4}' 
                AND atype = '3F'""")
        df_plan = pd.read_sql(query_plan, db)
        if df_plan.empty:
            print(f"⚠️ No plan record for {cols2} {cols3} {cols4} - skipping {file.name} - {sheet_name}")
            return pd.DataFrame()  # Return empty DataFrame if no plan record
        
        query_old = text(f"""SELECT 1
            FROM {table_reconcile if sheet_name == sheet_reconcile else table_block}
            WHERE bu = '{cols2}' 
                AND stcode = '{cols3}' 
                AND cntdate = '{cols4}'""")
        df_old = pd.read_sql(query_old, db3)
        if not df_old.empty:
            print(f"⚠️ Old record exists for {cols2} {cols3} {cols4} - skipping {file.name} - {sheet_name}")
            return pd.DataFrame()  # Return empty DataFrame if old record exists

        return df
    except Exception as e:
        print(f"Error processing {file.name} - {sheet_name}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    
# Process Reconcile and Block sheets
for i, file in enumerate(excel_files, start=1):
    df_reconcile = process(file, sheet_reconcile, usecols_reconcile, skiprows_reconcile)
    if not df_reconcile.empty:
        print(f"✅ Reconcile processed {file.name} ({len(df_reconcile)} rows) ({i}/{len(excel_files)})")
        df_reconcile.to_sql(table_reconcile, db3, if_exists='append', index=False)

    df_block = process(file, sheet_block, usecols_block, skiprows_block)
    if not df_block.empty:
        print(f"✅ Block processed {file.name} ({len(df_block)} rows) ({i}/{len(excel_files)})")
        df_block.to_sql(table_block, db3, if_exists='append', index=False)



    

    
