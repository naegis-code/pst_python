import pandas as pd
from sqlalchemy import text
import io
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

bu = 'ssp'
sheet  = 'ssp_v1'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine1

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)

# Select only the necessary columns

keep_columns = [
    'ID', 'created','stcode','check_date',
    'SSPV1A01','SSPV1A02','SSPV1A03','SSPV1A04','SSPV1A05','SSPV1A06','SSPV1A07','SSPV1A08','SSPV1A09','SSPV1A10',
    'SSPV1A11','SSPV1A12','SSPV1A13','SSPV1A14','SSPV1A15','SSPV1A16','SSPV1A17','SSPV1A18','SSPV1A19','SSPV1A20',
    'SSPV1A21','SSPV1A22','SSPV1A23','SSPV1A24','SSPV1A25','SSPV1A26','SSPV1A27','SSPV1A28','SSPV1A29','SSPV1A30',
    'SSPV1A31','SSPV1A32','SSPV1A33','SSPV1A34','SSPV1A35'
]

df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'ID': 'Int64',
    'created': 'datetime64[ns]',
    'stcode': 'string',
    'check_date': 'datetime64[ns]',
}

# auto ใส่ int ให้ SSPV1A01–SSPV1A35
dtype_map.update({f'SSPV1A{i:02d}': 'Int64' for i in range(1, 36)})

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
# UPSERT ตามคีย์ (stcode, cntdate, question_code) — รันด้วย --dry-run เพื่อดูจำนวนโดยไม่เขียนข้อมูล
upsert_df(connect_db, df, bu)
