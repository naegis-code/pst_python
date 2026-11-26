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

bu = 'chg'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine1

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

