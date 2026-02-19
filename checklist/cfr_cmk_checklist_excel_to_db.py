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
sheet  = 'CFR_CMK'

filter_count_date = pd.Timestamp.now() - pd.Timedelta(days=3)
filter_count_date = filter_count_date.strftime('%Y%m%d')
print(f"Start time for filtering data: {filter_count_date}")

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = create_engine(db_connect.db_url_pstdb)

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)

# Select only the necessary columns
keep_columns = [
    'Id', 'Start time','รหัสสาขา (Store Code)','วันที่นับ\xa0Full stock count',
    'CMK01','CMK02','CMK03','CMK04','CMK05','CMK06','CMK07','CMK08','CMK09','CMK10',
    'CMK11','CMK12','CMK13','CMK14','CMK15','CMK16','CMK17','CMK18','CMK19','CMK20',
    'CMK21','CMK22','CMK23','CMK24','CMK25','CMK26','CMK27','CMK28','CMK29','CMK30',
    'CMK31','CMK32','CMK33','CMK34','CMK35'
]
df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'Id': 'Int64',
    'Start time': 'datetime64[ns]',
    'รหัสสาขา (Store Code)': 'string',
    'วันที่นับ\xa0Full stock count': 'datetime64[ns]'
}

# auto ใส่ int ให้ OFMV1A01–OFMV1F09
dtype_map.update({f'CMK{i:02d}': 'Int64' for i in range(1, 36)})

# select + cast
df = df.astype(dtype_map)

df.rename(columns={'Id':'id','Start time':'checkdate','วันที่นับ\xa0Full stock count':'cntdate','รหัสสาขา (Store Code)':'stcode'}, inplace=True)

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

