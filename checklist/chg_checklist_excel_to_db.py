import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'chg'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = create_engine(db_connect.db_url_pstdb)

# =================================
df = pd.read_excel(src_path, sheet_name='chg_v1',dtype=str)

# Select only the necessary columns
keep_columns = [
    'ID', 'Start time','รหัสสาขา','วันที่ตรวจนับ',
    'CHGV01A01','CHGV01A02','CHGV01A03','CHGV01A04','CHGV01A05','CHGV01A06','CHGV01A07','CHGV01A08','CHGV01A09','CHGV01A10',
    'CHGV01A11','CHGV01A12','CHGV01A13','CHGV01A14','CHGV01A15','CHGV01A16','CHGV01A17','CHGV01A18','CHGV01A19','CHGV01A20',
    'CHGV01A21','CHGV01A22','CHGV01A23','CHGV01A24','CHGV01A25','CHGV01A26','CHGV01A27','CHGV01A28','CHGV01A29','CHGV01A30',
    'CHGV01A31','CHGV01A32','CHGV01A33','CHGV01A34','CHGV01A35','CHGV01A36','CHGV01A37','CHGV01A38','CHGV01A39','CHGV01A40',
    'CHGV01A41'

]

df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'ID': 'Int64',
    'Start time': 'datetime64[ns]',
    'รหัสสาขา': 'string',
    'วันที่ตรวจนับ': 'datetime64[ns]',
}

# auto ใส่ int ให้ CHGV01A01–41
dtype_map.update({f'CHGV01A{i:02d}': 'Int64' for i in range(1, 42)})

# select + cast
df = df.astype(dtype_map)

df.rename(columns={'ID':'id','Start time':'checkdate','รหัสสาขา':'stcode','วันที่ตรวจนับ':'cntdate'}, inplace=True)

df = df.sort_values(by='id', ascending=False).drop_duplicates(subset=['stcode', 'cntdate'])

# Convert checkdate and cntdate to yyyymmdd format
df['checkdate'] = pd.to_datetime(df['checkdate']).dt.strftime('%Y%m%d')
df['cntdate'] = pd.to_datetime(df['cntdate'], format='%d/%m/%Y').dt.strftime('%Y%m%d')

# Unpivot the DataFrame to have a long format
df = df.melt(
    id_vars=['id', 'checkdate', 'stcode', 'cntdate'],
    var_name='question_code',
    value_name='point'
)


df_mapping = pd.read_excel(src_path, sheet_name='Mapping')

df_mapping.columns = df_mapping.columns.str.lower()

df_mapping.rename(columns={'code':'question_code'}, inplace=True)

df = df.merge(
    df_mapping[['question_code', 'bu', 'zone', 'weight', 'no','subject','desceiption',
                'subdescription']],
    on='question_code',
    how='left'
)

rename_columns = {
    'subject':'section',
    'desceiption':'subject'
}   

df.rename(columns=rename_columns, inplace=True)

# Calculate 'full' and 'act' columns
df['full'] = 5 * df['weight']
df['act'] = df['point'] * df['weight']

# Replace 'zone' values: 'B' with 'Back' and 'F' with 'Sale'
df['zone'] = df['zone'].replace({'B': 'Back', 'F': 'Sale'})

# check branch from planall2
df_plan = pd.read_sql(
    text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}'"),
    connect_db
)
df = df.merge(
    df_plan,
    on=['bu', 'stcode', 'cntdate'],
    how='left'
)
df = df[df['branch'].notna()].drop(columns=['branch','question_code','id'], errors='ignore')


# check recheck from checklist table
df_checklist = pd.read_sql(
    text(f"select distinct bu,stcode ,cntdate ,'check' as recheck from checklist WHERE bu = '{bu.upper()}'"),
    connect_db
)
df = df.merge(
    df_checklist,
    on=['bu', 'stcode', 'cntdate'],
    how='left'
)
df = df[df['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

# Display the first few rows of the DataFrame
print(df.shape)
print(df.head())
# Insert data into the checklist table
df.to_sql('checklist', connect_db, if_exists='append', index=False)
print(f"✅ Data inserted into the checklist table successfully. Total rows inserted: {len(df)}")

