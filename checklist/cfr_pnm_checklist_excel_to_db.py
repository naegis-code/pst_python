import pandas as pd
from sqlalchemy import create_engine,text
import pathlib
from dotenv import load_dotenv,find_dotenv
import os
load_dotenv(find_dotenv())

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()

if userpath.name == "prthanap":
    path = f"{userpath}/Downloads"
    engine0 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
    engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")
elif userpath.name == "shthanapat":
    path = f"{userpath}/Downloads"
    engine0 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@103.22.182.82:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
    engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@103.22.182.82:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")
else:
    path = f"{userpath}/Downloads"
    engine0 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")
    engine3 = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}")

filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'cfr'
sheet  = 'pnm_v1'

filter_count_date = pd.Timestamp.now() - pd.Timedelta(days=1)
filter_count_date = filter_count_date.strftime('%Y%m%d')
print(f"Start time for filtering data: {filter_count_date}")

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine0

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)

# Select only the necessary columns
keep_columns = [
    'Id', 'Start time','รหัสสาขา (Store Code)','วันที่นับ\xa0Full stock count',
    'PNM01','PNM02','PNM03','PNM04','PNM05','PNM06','PNM07','PNM08','PNM09','PNM10',
    'PNM11','PNM12','PNM13','PNM14','PNM15','PNM16','PNM17','PNM18','PNM19','PNM20',
    'PNM21','PNM22','PNM23','PNM24','PNM25','PNM26','PNM27','PNM28','PNM29','PNM30',
    'PNM31','PNM32','PNM33','PNM34','PNM35','PNM36','PNM37','PNM38','PNM39','PNM40',
    'PNM41','PNM42','PNM43','PNM44','PNM45','PNM46','PNM47'
]
df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'Id': 'Int64',
    'Start time': 'datetime64[ns]',
    'รหัสสาขา (Store Code)': 'string',
    'วันที่นับ\xa0Full stock count': 'datetime64[ns]'
}

# auto ใส่ int ให้
dtype_map.update({f'PNM{i:02d}': 'Int64' for i in range(1, 48)})

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
    text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}' and type1 = 'PNM' and atype = '3F'"),
    connect_db
)
df = df.merge(
    df_plan,
    on=['bu', 'stcode', 'cntdate'],
    how='inner'
)
df = df.drop(columns=['branch','id','type1'], errors='ignore')

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
print(df)
# Insert data into the checklist table
#df.to_sql('checklist', connect_db, if_exists='append', index=False)
print(f"✅ Data inserted into the checklist table successfully. Total rows inserted: {len(df)}")

