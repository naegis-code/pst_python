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

bu = 'cfr'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = create_engine(db_connect.db_url_pstdb)

# =================================
df_cfr_v2 = pd.read_excel(src_path, sheet_name='cfr_v2')

# Select only the necessary columns
keep_columns = [
    'ID', 'Start time','รหัสสาขา','วันที่ตรวจนับ','CFRTMRV2B01','CFRTMRV2B02','CFRTMRV2B03','CFRTMRV2B04','CFRTMRV2B05',
    'CFRTMRV2B06','CFRTMRV2B07','CFRTMRV2B08','CFRTMRV2B09','CFRTMRV2B10','CFRTMRV2B11','CFRTMRV2B12','CFRTMRV2B13',
    'CFRTMRV2B14','CFRTMRV2B15','CFRTMRV2B16','CFRTMRV2B17','CFRTMRV2B18','CFRTMRV2B19','CFRTMRV2B20','CFRTMRV2B21',
    'CFRTMRV2B22','CFRTMRV2B23','CFRTMRV2B24','CFRTMRV2B25','CFRTMRV2B26','CFRTMRV2B27','CFRTMRV2B28','CFRTMRV2B29',
    'CFRTMRV2B30','CFRTMRV2B31','CFRTMRV2B32','CFRTMRV2B33','CFRTMRV2B34','CFRTMRV2B35','CFRTMRV2B36','CFRTMRV2B37',
    'CFRTMRV2F01','CFRTMRV2F02','CFRTMRV2F03','CFRTMRV2F04','CFRTMRV2F05','CFRTMRV2F06','CFRTMRV2F07','CFRTMRV2F08',
    'CFRTMRV2F09','CFRTMRV2F10','CFRTMRV2F11','CFRTMRV2F12','CFRTMRV2F13','CFRTMRV2F14','CFRTMRV2F15','CFRTMRV2F16',
    'CFRTMRV2F17','CFRTMRV2F18','CFRTMRV2F19','CFRTMRV2F20','CFRTMRV2F21','CFRTMRV2F22','CFRTMRV2F23','CFRTMRV2F24',
    'CFRTMRV2F25','CFRTMRV2F26','CFRTMRV2F27','CFRTMRV2F28','CFRTMRV2F29','CFRTMRV2F30','CFRTMRV2F31'
]
df_cfr_v2 = df_cfr_v2[keep_columns]

df_cfr_v2.rename(columns={'ID':'id','Start time':'checkdate','รหัสสาขา':'stcode','วันที่ตรวจนับ':'cntdate'}, inplace=True)

df_cfr_v2 = df_cfr_v2.sort_values(by='id', ascending=False).drop_duplicates(subset=['stcode', 'cntdate'])

# Convert checkdate and cntdate to yyyymmdd format
df_cfr_v2['checkdate'] = pd.to_datetime(df_cfr_v2['checkdate']).dt.strftime('%Y%m%d')
df_cfr_v2['cntdate'] = pd.to_datetime(df_cfr_v2['cntdate'], format='%d/%m/%Y').dt.strftime('%Y%m%d')

# Unpivot the DataFrame to have a long format
df_cfr_v2 = df_cfr_v2.melt(
    id_vars=['id', 'checkdate', 'stcode', 'cntdate'],
    var_name='question_code',
    value_name='point'
)


df_mapping = pd.read_excel(src_path, sheet_name='Mapping')

df_mapping.columns = df_mapping.columns.str.lower()

df_mapping.rename(columns={'code':'question_code'}, inplace=True)

df_cfr_v2 = df_cfr_v2.merge(
    df_mapping[['question_code', 'bu', 'zone', 'weight', 'no','subject','desceiption',
                'subdescription']],
    on='question_code',
    how='left'
)

rename_columns = {
    'subject':'section',
    'desceiption':'subject'
}   

df_cfr_v2.rename(columns=rename_columns, inplace=True)

# Calculate 'full' and 'act' columns
df_cfr_v2['full'] = 5 * df_cfr_v2['weight']
df_cfr_v2['act'] = df_cfr_v2['point'] * df_cfr_v2['weight']

# Replace 'zone' values: 'B' with 'Back' and 'F' with 'Sale'
df_cfr_v2['zone'] = df_cfr_v2['zone'].replace({'B': 'Back', 'F': 'Sale'})

# check branch from planall2
df_plan = pd.read_sql(
    text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}'"),
    connect_db
)
df_cfr_v2 = df_cfr_v2.merge(
    df_plan,
    on=['bu', 'stcode', 'cntdate'],
    how='left'
)
df_cfr_v2 = df_cfr_v2[df_cfr_v2['branch'].notna()].drop(columns=['branch','question_code','id'], errors='ignore')


# check recheck from checklist table
df_checklist = pd.read_sql(
    text(f"select distinct bu,stcode ,cntdate ,'check' as recheck from checklist WHERE bu = '{bu.upper()}'"),
    connect_db
)
df_cfr_v2 = df_cfr_v2.merge(
    df_checklist,
    on=['bu', 'stcode', 'cntdate'],
    how='left'
)
df_cfr_v2 = df_cfr_v2[df_cfr_v2['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

# Display the first few rows of the DataFrame
print(df_cfr_v2.shape)

# Insert data into the checklist table
df_cfr_v2.to_sql('checklist', connect_db, if_exists='append', index=False)
print(f"✅ Data inserted into the checklist table successfully. Total rows inserted: {len(df_cfr_v2)}")

