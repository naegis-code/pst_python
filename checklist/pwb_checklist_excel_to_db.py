import pandas as pd
from sqlalchemy import text
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from tunnel import engine1  # engine1=pstdb
from checklist_common import replace_sets

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'pwb'
sheet  = 'pwb_v1'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine1

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)
'''

'''
# Select only the necessary columns
keep_columns = [
    'ID', 'Start time','stcode','check_date',
    'PWBV1F01','PWBV1F02','PWBV1F03','PWBV1F04','PWBV1F05','PWBV1F06','PWBV1F07','PWBV1F08','PWBV1F09','PWBV1F10',
    'PWBV1F11','PWBV1F12','PWBV1F13','PWBV1F14','PWBV1F15','PWBV1F16','PWBV1F17','PWBV1F18','PWBV1F19','PWBV1F20',
    'PWBV1F21','PWBV1F22','PWBV1F23','PWBV1F24','PWBV1F25','PWBV1F26','PWBV1F27','PWBV1F28','PWBV1F29','PWBV1F30',
    'PWBV1F31','PWBV1F32','PWBV1F33','PWBV1F34','PWBV1F35','PWBV1F36','PWBV1F37','PWBV1F38','PWBV1F39',

    'PWBV1B01','PWBV1B02','PWBV1B03','PWBV1B04','PWBV1B05','PWBV1B06','PWBV1B07','PWBV1B08','PWBV1B09',
    'PWBV1B10','PWBV1B11','PWBV1B12','PWBV1B13','PWBV1B14'
]

df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'ID': 'Int64',
    'Start time': 'datetime64[ns]',
    'stcode': 'string',
    'check_date': 'datetime64[ns]',
}

# auto ใส่ int ให้ OFMV1A01–OFMV1F09
dtype_map.update({f'PWBV1F{i:02d}': 'Int64' for i in range(1, 40)})
dtype_map.update({f'PWBV1B{i:02d}': 'Int64' for i in range(1, 15)})

# select + cast
df = df.astype(dtype_map)

df.rename(columns={'ID':'id','Start time':'checkdate','check_date':'cntdate'}, inplace=True)

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

# แจ้งสาขา+วันที่ใน Excel ที่ไม่ตรง planall2 (เช่น หน้าร้านใส่วันที่/รหัสสาขาผิด) — ไม่ถูกโหลด
df_unmatched = (
    df.loc[df['branch'].isna(), ['stcode', 'cntdate']]
    .drop_duplicates()
    .sort_values(['cntdate', 'stcode'])
)
if not df_unmatched.empty:
    print(f"⚠️ ไม่ตรง planall2 {len(df_unmatched)} ชุด (ไม่ถูกโหลด):")
    print(df_unmatched.to_string(index=False))

df = df[df['branch'].notna()].drop(columns=['branch','id'], errors='ignore')

print(df.shape)
# แทนที่ทีละชุด (bu + stcode + cntdate) — รันด้วย --dry-run เพื่อดูจำนวนโดยไม่เขียนข้อมูล
replace_sets(connect_db, df, bu)

