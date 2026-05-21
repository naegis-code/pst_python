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
sheet  = 'cfr_topscare'

filter_count_date = pd.Timestamp.now() - pd.Timedelta(days=1)
filter_count_date = filter_count_date.strftime('%Y%m%d')
print(f"Start time for filtering data: {filter_count_date}")

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = create_engine(db_connect.db_url_pstdb)

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)

# Select only the necessary columns
keep_columns = [
    'ID', 'Created','stcode','check_date',
    'CFRTCA01','CFRTCA02','CFRTCA03','CFRTCA04','CFRTCA05','CFRTCA06','CFRTCA07','CFRTCA08','CFRTCA09','CFRTCA10',
    'CFRTCA11','CFRTCA12','CFRTCA13','CFRTCA14','CFRTCA15','CFRTCA16','CFRTCA17','CFRTCA18','CFRTCA19','CFRTCA20',
    'CFRTCA21','CFRTCA22','CFRTCA23','CFRTCA24','CFRTCA25','CFRTCA26','CFRTCA27','CFRTCA28','CFRTCA29','CFRTCA30'
]
df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'ID': 'Int64',
    'Created': 'datetime64[ns]',
    'stcode': 'string',
    'check_date': 'datetime64[ns]'
}

# auto ใส่ int ให้ OFMV1A01–OFMV1F09
dtype_map.update({f'CFRTCA{i:02d}': 'Int64' for i in range(1, 31)})

# select + cast
df = df.astype(dtype_map)

df.rename(columns={'ID':'id','Created':'checkdate','check_date':'cntdate','stcode':'stcode'}, inplace=True)

df = df.sort_values(by='id', ascending=False).drop_duplicates(subset=['stcode', 'cntdate'])

# Filter the DataFrame to include only rows where 'cntdate' is less than or equal to the specified date
df = df[df['cntdate'] <= filter_count_date]

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
df = df[df['branch'].notna()].drop(columns=['branch','id'], errors='ignore')

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
print(df)
# Insert data into the checklist table
df.to_sql('checklist', connect_db, if_exists='append', index=False)
print(f"✅ Data inserted into the checklist table successfully. Total rows inserted: {len(df)}")

