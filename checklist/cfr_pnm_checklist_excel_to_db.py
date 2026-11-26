import pandas as pd
from sqlalchemy import text
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from tunnel import engine1  # engine1=pstdb
from checklist_common import upsert_df

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()

filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'cfr'
sheet  = 'pnm_v1'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine1

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
    how='left'
)

# แจ้งสาขา+วันที่ใน Excel ที่ไม่ตรง planall2 (PNM 3F) — ไม่ถูกโหลด
df_unmatched = (
    df.loc[df['branch'].isna(), ['stcode', 'cntdate']]
    .drop_duplicates()
    .sort_values(['cntdate', 'stcode'])
)
if not df_unmatched.empty:
    print(f"⚠️ ไม่ตรง planall2 {len(df_unmatched)} ชุด (ไม่ถูกโหลด):")
    print(df_unmatched.to_string(index=False))

df = df[df['branch'].notna()].drop(columns=['branch','id','type1'], errors='ignore')

print(df.shape)
# UPSERT ตามคีย์ (stcode, cntdate, question_code) — รันด้วย --dry-run เพื่อดูจำนวนโดยไม่เขียนข้อมูล
upsert_df(connect_db, df, bu)

