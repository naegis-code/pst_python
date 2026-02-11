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

bu = 'b2s'
sheet  = 'b2s_v1'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = create_engine(db_connect.db_url_pstdb)

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)

# Select only the necessary columns
keep_columns = [
    'ID', 'created','stcode','check_date',
    'B2SV1F01','B2SV1F02','B2SV1F03','B2SV1F04','B2SV1F05','B2SV1F06','B2SV1F07','B2SV1F08','B2SV1F09','B2SV1F10',
    'B2SV1F11','B2SV1F12','B2SV1F13','B2SV1F14','B2SV1F15','B2SV1F16','B2SV1F17','B2SV1F18','B2SV1F19','B2SV1F20',
    'B2SV1F21','B2SV1F22','B2SV1F23','B2SV1F24','B2SV1F25','B2SV1F26','B2SV1F27','B2SV1F28','B2SV1F29','B2SV1F30',
    'B2SV1F31','B2SV1F32','B2SV1F33','B2SV1F34','B2SV1F35','B2SV1F36','B2SV1F37','B2SV1F38','B2SV1F39','B2SV1F40',
    'B2SV1F41','B2SV1F42','B2SV1F43',

    'B2SV1B01','B2SV1B02','B2SV1B03','B2SV1B04','B2SV1B05','B2SV1B06','B2SV1B07','B2SV1B08'
]

df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'ID': 'Int64',
    'created': 'datetime64[ns]',
    'stcode': 'string',
    'check_date': 'datetime64[ns]',
}

# auto ใส่ int ให้ OFMV1A01–OFMV1F09
dtype_map.update({f'B2SV1F{i:02d}': 'Int64' for i in range(1, 44)})
dtype_map.update({f'B2SV1B{i:02d}': 'Int64' for i in range(1, 9)})

# select + cast
df = df.astype(dtype_map)

df.rename(columns={'ID':'id','created':'checkdate','check_date':'cntdate'}, inplace=True)

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
#df.to_sql('checklist', connect_db, if_exists='append', index=False)
print(f"✅ Data inserted into the checklist table successfully. Total rows inserted: {len(df)}")

